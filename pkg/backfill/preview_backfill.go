package backfill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

const (
	// DefaultPreviewBatch is the number of spans one batch reads, previews
	// and writes back in a single transaction.
	DefaultPreviewBatch = 500
	// DefaultPreviewPause is the idle time between batches, so a backfill
	// over a large corpus shares the database with live derive and reads.
	DefaultPreviewPause = 200 * time.Millisecond
)

// PreviewOptions configures one run of the span preview backfill.
type PreviewOptions struct {
	// Store serves the rows without previews and takes the previews back.
	Store storage.PreviewBackfiller

	// SessionID restricts the scan to one session; empty means every
	// session.
	SessionID string

	// BatchSize bounds every read and every write transaction. Zero
	// means DefaultPreviewBatch.
	BatchSize int

	// Pause is the sleep between batches; zero or negative means none. The
	// CLI defaults it to DefaultPreviewPause.
	Pause time.Duration

	// DryRun scans and counts but writes nothing.
	DryRun bool

	Logf func(format string, args ...any)

	// AfterBatch, when set, runs after each batch has been applied (or
	// counted, in a dry run) and before the pause. It exists so a caller
	// can observe or interrupt progress at a batch boundary.
	AfterBatch func(progress PreviewProgress)
}

// PreviewProgress is the state of a run after one batch.
type PreviewProgress struct {
	// Batch is the rows the batch read.
	Batch []storage.SpanBackfillRow
	// Backfilled is the running total of rows written (or, in a dry
	// run, rows that would have been).
	Backfilled int
	// Last is the keyset cursor the next batch starts after.
	Last storage.SpanBackfillCursor
}

// PreviewResult summarizes a run.
type PreviewResult struct {
	// Batches is the number of non-empty pages read.
	Batches int `json:"batches"`
	// Backfilled is the number of spans whose previews were written
	// (counted but not written in a dry run).
	Backfilled int `json:"backfilled"`
	// Last is the key of the last span the run reached; zero when the
	// scan found nothing.
	Last storage.SpanBackfillCursor `json:"last"`
	// DryRun echoes the option so a report is self-describing.
	DryRun bool `json:"dry_run"`
}

// Previews fills the stored input_preview / output_preview of spans
// derived before the preview columns existed, from the payload already
// on the row, as derive.PreviewBlocks would have at write time. It never
// re-derives and never touches the payload, content_hash or derive_seq.
//
// The scan is keyset-paged on (session_id, trace_id, span_id) with one
// bounded transaction per batch and a pause between batches, so it can
// run against a live deployment. It is idempotent and resumable: a row
// that has previews no longer matches the selection, so an interrupted
// run picks up where it stopped when re-run, and a completed run is a
// no-op. The run ends when a page comes back empty.
func Previews(ctx context.Context, opts PreviewOptions) (*PreviewResult, error) {
	if opts.Store == nil {
		return nil, errors.New("preview backfill requires a store")
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultPreviewBatch
	}

	result := &PreviewResult{DryRun: opts.DryRun}
	var after storage.SpanBackfillCursor
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rows, err := opts.Store.ListSpansMissingPreviews(ctx, after, opts.SessionID, opts.BatchSize)
		if err != nil {
			return result, fmt.Errorf("list spans missing previews after %s: %w", after, err)
		}
		if len(rows) == 0 {
			return result, nil
		}

		updates := make([]storage.SpanPreviewUpdate, 0, len(rows))
		for _, row := range rows {
			updates = append(updates, storage.SpanPreviewUpdate{
				OrgID:         row.OrgID,
				TraceID:       row.TraceID,
				SpanID:        row.SpanID,
				InputPreview:  derive.PreviewBlocks(row.Input),
				OutputPreview: derive.PreviewBlocks(row.Output),
			})
		}
		if !opts.DryRun {
			if err := opts.Store.SetSpanPreviews(ctx, updates); err != nil {
				return result, fmt.Errorf("set span previews (batch after %s): %w", after, err)
			}
		}

		after = rows[len(rows)-1].Cursor()
		result.Batches++
		result.Backfilled += len(rows)
		result.Last = after
		if opts.DryRun {
			opts.Logf("dry-run would backfill=%d last=%s", result.Backfilled, after)
		} else {
			opts.Logf("backfilled=%d last=%s", result.Backfilled, after)
		}
		if opts.AfterBatch != nil {
			opts.AfterBatch(PreviewProgress{Batch: rows, Backfilled: result.Backfilled, Last: after})
		}

		// A short page is the last one; do not pay the pause to learn that.
		if len(rows) < opts.BatchSize {
			return result, nil
		}
		if opts.Pause > 0 {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(opts.Pause):
			}
		}
	}
}

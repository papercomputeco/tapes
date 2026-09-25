package postgres

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Row-iterating span readers. These are hand-written pgx queries rather
// than sqlc `:many` methods on purpose: sqlc collects every row into a
// slice before returning, which is the materialization these readers
// exist to avoid. Each row is scanned into the sqlc row shape so the
// mapping stays in one place (spanRecordFromRow), and the record is
// handed to the consumer and dropped before the next row is read.

// The spans_20260615 select list, in exactly the order sqlc scans
// gensqlc.Spans20260615 (see ListSpansBySession in gensqlc/spans.sql.go);
// scanSpanRow depends on this order. It is built from three pieces so that
// the preview list is the same list with the payload columns cut out,
// rather than a second list that could drift.
const (
	spanHeadColumns = `org_id, trace_id, span_id, parent_span_id, session_id, kind, name, status, ` +
		`call_kind, thread_id, model, stop_reason, started_at, duration_ns`
	spanPayloadColumns = `input, output`
	spanTailColumns    = `usage, raw_turn_id, node_hash, seq, verdict, content_hash, derive_seq, fidelity, ` +
		`input_preview, output_preview`

	spanSelectColumns = spanHeadColumns + `, ` + spanPayloadColumns + `, ` + spanTailColumns

	// previewSpanSelect is the preview-mode select list: the header
	// columns and the stored previews, and never input or output. The
	// point of a preview read is that the payload is not detoasted, which
	// only holds if it is not named here. usage and verdict stay: they are
	// small, and the wire carries them in every mode.
	previewSpanSelect = spanHeadColumns + `, ` + spanTailColumns
)

// spanColumns returns the select list for a payload mode.
func spanColumns(mode storage.PayloadMode) string {
	if mode == storage.PayloadPreview {
		return previewSpanSelect
	}
	return spanSelectColumns
}

// iterateSessionSpansFrom streams a session's spans in the same composite
// order ListSpansBySession uses. $2 is true when starting from the first
// span; otherwise the four-column row-value comparison resumes strictly
// after the cursor, so a restart neither repeats nor skips a row.
const iterateSessionSpansFrom = ` FROM spans_20260615
WHERE session_id = $1
  AND ($2::boolean OR (trace_id, seq, started_at, span_id) > ($3::text, $4::bigint, $5::timestamptz, $6::text))
ORDER BY trace_id ASC, seq ASC, started_at ASC, span_id ASC`

// iterateTraceSpansFrom streams one trace's spans in the order
// ListSpansByTrace uses. $3 is true when starting from the first span;
// otherwise the row-value comparison resumes strictly after the cursor
// on the same three columns that order the trace, so spans sharing a seq
// are neither repeated nor skipped.
const iterateTraceSpansFrom = ` FROM spans_20260615
WHERE org_id = $1 AND trace_id = $2
  AND ($3::boolean OR (seq, started_at, span_id) > ($4::bigint, $5::timestamptz, $6::text))
ORDER BY seq ASC, started_at ASC, span_id ASC`

// IterateSessionSpans streams one session's spans in composite order,
// starting strictly after the cursor (zero value: from the first span).
// Implements storage.SpanModelReader.
func (d *Driver) IterateSessionSpans(ctx context.Context, sessionID string, after storage.SpanCursor, mode storage.PayloadMode) iter.Seq2[storage.SpanRecord, error] {
	return func(yield func(storage.SpanRecord, error) bool) {
		if d == nil || d.conn == nil {
			yield(storage.SpanRecord{}, errors.New("postgres driver not open"))
			return
		}
		parsed, err := uuid.Parse(sessionID)
		if err != nil {
			yield(storage.SpanRecord{}, fmt.Errorf("parse session id: %w", err))
			return
		}
		rows, err := d.conn.Query(ctx, `SELECT `+spanColumns(mode)+iterateSessionSpansFrom,
			pgtype.UUID{Bytes: parsed, Valid: true},
			after.IsZero(),
			after.TraceID,
			after.Seq,
			pgtype.Timestamptz{Time: after.StartedAt, Valid: true},
			after.SpanID,
		)
		if err != nil {
			yield(storage.SpanRecord{}, fmt.Errorf("iterate session spans: %w", err))
			return
		}
		streamSpanRows(rows, mode, yield)
	}
}

// IterateTraceSpans streams one trace's spans in presentation order,
// starting strictly after the cursor (zero value: from the first span).
// Implements storage.SpanModelReader.
func (d *Driver) IterateTraceSpans(ctx context.Context, orgID, traceID string, after storage.SpanCursor, mode storage.PayloadMode) iter.Seq2[storage.SpanRecord, error] {
	return func(yield func(storage.SpanRecord, error) bool) {
		if d == nil || d.conn == nil {
			yield(storage.SpanRecord{}, errors.New("postgres driver not open"))
			return
		}
		org, err := orgIDFromString(orgKeyForLookup(orgID))
		if err != nil {
			yield(storage.SpanRecord{}, fmt.Errorf("decode org_id: %w", err))
			return
		}
		rows, err := d.conn.Query(ctx, `SELECT `+spanColumns(mode)+iterateTraceSpansFrom,
			org,
			traceID,
			after.IsZero(),
			after.Seq,
			pgtype.Timestamptz{Time: after.StartedAt, Valid: true},
			after.SpanID,
		)
		if err != nil {
			yield(storage.SpanRecord{}, fmt.Errorf("iterate trace spans: %w", err))
			return
		}
		streamSpanRows(rows, mode, yield)
	}
}

// streamSpanRows hands rows to yield one at a time. The row struct is
// declared per iteration so the previous record is unreachable — and
// collectable — before the next scan. rows is closed on every exit path
// (consumer break, scan failure, end of stream), which also returns the
// pooled connection; rows.Err(), including a context cancellation, is
// surfaced as the final yielded error.
func streamSpanRows(rows pgx.Rows, mode storage.PayloadMode, yield func(storage.SpanRecord, error) bool) {
	defer rows.Close()
	for rows.Next() {
		var row gensqlc.Spans20260615
		if err := scanSpanRow(rows, &row, mode); err != nil {
			yield(storage.SpanRecord{}, fmt.Errorf("scan span: %w", err))
			return
		}
		if !yield(spanRecordFromRow(row), nil) {
			return
		}
	}
	if err := rows.Err(); err != nil {
		yield(storage.SpanRecord{}, fmt.Errorf("iterate spans: %w", err))
	}
}

// scanSpanRow scans the current row in spanColumns(mode) order. In
// preview mode the payload columns were not selected, so Input and
// Output are left nil rather than scanned.
func scanSpanRow(rows pgx.Rows, row *gensqlc.Spans20260615, mode storage.PayloadMode) error {
	dest := make([]any, 0, 26)
	dest = append(dest,
		&row.OrgID,
		&row.TraceID,
		&row.SpanID,
		&row.ParentSpanID,
		&row.SessionID,
		&row.Kind,
		&row.Name,
		&row.Status,
		&row.CallKind,
		&row.ThreadID,
		&row.Model,
		&row.StopReason,
		&row.StartedAt,
		&row.DurationNs,
	)
	if mode != storage.PayloadPreview {
		dest = append(dest, &row.Input, &row.Output)
	}
	dest = append(dest,
		&row.Usage,
		&row.RawTurnID,
		&row.NodeHash,
		&row.Seq,
		&row.Verdict,
		&row.ContentHash,
		&row.DeriveSeq,
		&row.Fidelity,
		&row.InputPreview,
		&row.OutputPreview,
	)
	return rows.Scan(dest...)
}

// Package rawequiv proves that re-reducing a captured turn's stored verbatim
// bytes reproduces the reduction the capture adapter computed live.
//
// # Why this exists
//
// The capture path is moving through a deliberate ratchet: off (the adapter
// reduces and ships only the reduction) → dual (the adapter ships both its
// reduction and the verbatim upstream bytes) → raw (the adapter ships only
// bytes and tapes reduces server-side). The end state is worth reaching
// because one reducer for every capture path means two paths cannot reduce
// differently — which is the failure the shared pkg/capture library exists to
// prevent and cannot prevent on its own while a second reducer runs in the
// adapter.
//
// The middle rung is where the proof happens. Under dual, every row carries
// both halves of the same turn, so the question "would raw have produced this
// row?" is answerable offline, over real traffic, without changing anything an
// operator sees. This package answers it: decode the stored bytes, re-reduce
// them through the exact server-side path mode=raw would run
// (ingest.ReduceStoredRawTurn), and compare against the stored reduction.
//
// A divergence found here is a reason not to flip. A clean window over
// representative traffic is the evidence that flipping is safe.
//
// # What "equivalent" means
//
// Equivalence is structural, not byte-level. The stored reduction comes back
// from Postgres jsonb, which normalizes key order, whitespace, duplicate keys
// and number formatting; the recomputed one is a fresh Go marshal. Comparing
// bytes would report the storage round-trip as a divergence. Both sides are
// therefore decoded to a generic JSON tree and compared structurally.
//
// Exactly two fields are permitted to differ, and both are time rather than
// content. They are removed from both sides before the comparison, so
// everything else — model, message role, every content block, stop reason,
// every other usage counter, extra diagnostics, and the echoed raw body on the
// providers that carry one — is compared strictly. If a third difference ever
// appears it is reported as a divergence rather than absorbed by a loose
// comparison. See TolerableFields for the per-field justification.
package rawequiv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// TolerableFields documents, and defines, the fields allowed to differ between
// a stored adapter reduction and a server-side re-reduction of the same bytes.
//
// This list is the whole tolerance of the proof. Every entry needs a reason
// grounded in the code, not in observed noise: a field that differs for a
// reason nobody can name is a divergence, and the point of the tool is to find
// those.
var TolerableFields = []TolerableField{
	{
		Path: []string{"created_at"},
		Why: "Reducers stamp CreatedAt with time.Now() at reduction time " +
			"(pkg/capture/anthropic.go, anthropic_state.go), so two reductions of " +
			"identical bytes taken at different instants differ by construction. " +
			"Under mode=raw ingest overrides it from meta.captured_at or " +
			"meta.ts_request when the producer sends one; no released producer " +
			"sends either today, so for Anthropic the field carries reduction " +
			"time. OpenAI Responses is the exception — it reads the upstream's own " +
			"created_at — but the tolerance is uniform because the tool cannot " +
			"assume a provider mix, and StampCoverage reports per row which " +
			"source would actually have supplied it.",
	},
	{
		Path: []string{"usage", "total_duration_ns"},
		Why: "The proxy-measured wall clock from request to fully-assembled " +
			"response (pkg/llm/response.go). Neither Anthropic nor OpenAI put a " +
			"call duration in the response body, so only the party that watched " +
			"the stream can know it; a reduction of stored bytes cannot. Under " +
			"mode=raw ingest re-stamps it from meta.elapsed_seconds, a float64 " +
			"seconds value, so even when it is restored it is a different " +
			"rounding of the same measurement than the adapter's own nanoseconds.",
	},
}

// TolerableField is one permitted difference and the reason it is permitted.
type TolerableField struct {
	// Path is the JSON path in a reduced llm.ChatResponse.
	Path []string
	// Why justifies the tolerance from the behaviour of the code.
	Why string
}

// String renders the path in dotted form.
func (f TolerableField) String() string { return strings.Join(f.Path, ".") }

// Class is the verdict for one examined row.
type Class string

const (
	// ClassEquivalent means the re-reduction reproduced the stored reduction.
	ClassEquivalent Class = "equivalent"

	// ClassDivergent means both reductions exist and differ outside the
	// tolerated fields. This is the finding that blocks a ratchet step.
	ClassDivergent Class = "divergent"

	// ClassUndecodable means the stored bytes could not be decoded under
	// their recorded content-encoding. Under mode=raw this row would carry no
	// reduction at all, so it blocks just as a divergence does.
	ClassUndecodable Class = "undecodable"

	// ClassUnreducible means the bytes decoded but the reducer rejected them.
	// Same consequence as undecodable: mode=raw would store an empty
	// reduction.
	ClassUnreducible Class = "unreducible"

	// ClassNoReducer means no server-side reducer is registered for the
	// row's provider (ollama today). mode=raw cannot serve this traffic.
	ClassNoReducer Class = "no_reducer"

	// ClassSkippedNoRaw means the row carries no verbatim bytes and none were
	// reported lost — a producer that was not sending raw when the turn was
	// captured. Nothing to prove; not a failure.
	ClassSkippedNoRaw Class = "skipped_no_raw"

	// ClassSkippedDropped means verbatim bytes existed and were not kept:
	// either the producer withheld them at the transport limit or ingest
	// dropped them over its storage cap. The row is marked
	// raw_response_dropped and lands fidelity:degraded. Nothing to compare,
	// but the count matters — these are rows a raw-only deployment would have
	// no way to reconstruct.
	ClassSkippedDropped Class = "skipped_dropped"

	// ClassSkippedNoReduction means the row has bytes but no adapter
	// reduction — it was captured raw-only already, so there is no second
	// opinion to compare against.
	ClassSkippedNoReduction Class = "skipped_no_reduction"
)

// Blocking reports whether a class should fail the run.
//
// The three reduction failures block alongside an outright divergence because
// their consequence under mode=raw is strictly worse: a divergent row still
// produces a reduction, while an undecodable, unreducible or unrouted row
// produces none, and ingest's own comment records that recovering such a row
// is possible in principle and not in practice. The skip classes never block —
// they describe rows the flip does not affect.
func (c Class) Blocking() bool {
	switch c {
	case ClassDivergent, ClassUndecodable, ClassUnreducible, ClassNoReducer:
		return true
	case ClassEquivalent, ClassSkippedNoRaw, ClassSkippedDropped, ClassSkippedNoReduction:
		return false
	default:
		return false
	}
}

// Row is one stored wire turn, as read from raw_turns.
type Row struct {
	ID                  int64
	RequestID           string
	Provider            string
	HarnessID           string
	HarnessSessionID    string
	Model               string
	RawResponse         []byte
	RawResponseEncoding string
	RawResponseDropped  bool

	// StoredReduction is the raw_turns.response jsonb — the adapter's
	// reduction, exactly as stored.
	StoredReduction json.RawMessage

	// Meta is the capture adapter's meta block from raw_turns.meta.
	Meta ingest.TurnMeta
}

// StampCoverage records which capture-side stamps mode=raw would have been
// able to restore for a row.
//
// This is not part of the equivalence verdict, and deliberately so: both
// stamped fields are excluded from the comparison, so a row can be perfectly
// equivalent while still losing its duration on the flip. Reporting coverage
// separately is what keeps "equivalent" from being read as "loses nothing".
type StampCoverage struct {
	Duration  ingest.StampSource `json:"duration"`
	CreatedAt ingest.StampSource `json:"created_at"`
}

// Outcome is the verdict for one row.
type Outcome struct {
	ID               int64  `json:"id"`
	RequestID        string `json:"request_id,omitempty"`
	Provider         string `json:"provider,omitempty"`
	HarnessID        string `json:"harness_id,omitempty"`
	HarnessSessionID string `json:"harness_session_id,omitempty"`
	Model            string `json:"model,omitempty"`

	Class Class `json:"class"`

	// Reason explains a non-equivalent, non-skip class. It is an error
	// string from the decode or reduce step and never contains body content.
	Reason string `json:"reason,omitempty"`

	// Diffs is the bounded structural difference for a divergent row.
	Diffs []FieldDiff `json:"diffs,omitempty"`

	// DiffsTruncated reports that more differences existed than were kept.
	DiffsTruncated bool `json:"diffs_truncated,omitempty"`

	// Truncated reports that the stored body ended early and the reduction
	// was salvaged from a partial stream. A divergence on such a row is
	// expected rather than alarming — the adapter saw the whole stream live.
	Truncated bool `json:"truncated,omitempty"`

	// Stamps reports which capture-side sources were available. Populated
	// whenever a re-reduction ran.
	Stamps *StampCoverage `json:"stamps,omitempty"`
}

// Options tunes a check.
type Options struct {
	// MaxDiffs bounds the structural differences recorded per row.
	MaxDiffs int
}

// DefaultMaxDiffs is the per-row difference bound when none is given.
const DefaultMaxDiffs = 10

// Check decides whether one stored row's reduction is reproducible from its
// stored bytes.
//
// The row is classified before any work is done, so the skip classes are
// cheap and the counts are exhaustive: every row examined lands in exactly one
// class and the classes sum to the total.
func Check(ctx context.Context, row Row, opts Options) Outcome {
	out := Outcome{
		ID:               row.ID,
		RequestID:        row.RequestID,
		Provider:         row.Provider,
		HarnessID:        row.HarnessID,
		HarnessSessionID: row.HarnessSessionID,
		Model:            row.Model,
	}
	if out.Model == "" {
		out.Model = row.Meta.Model
	}

	maxDiffs := opts.MaxDiffs
	if maxDiffs <= 0 {
		maxDiffs = DefaultMaxDiffs
	}

	// Dropped is checked before absent bytes because it is the more specific
	// fact: a dropped row also has no bytes, and reporting it as "producer
	// wasn't sending raw" would erase the difference between a limit that bit
	// and a deployment that never captured.
	if row.RawResponseDropped {
		out.Class = ClassSkippedDropped
		return out
	}
	if len(row.RawResponse) == 0 {
		out.Class = ClassSkippedNoRaw
		return out
	}

	var stored llm.ChatResponse
	if len(row.StoredReduction) > 0 {
		if err := json.Unmarshal(row.StoredReduction, &stored); err != nil {
			out.Class = ClassSkippedNoReduction
			out.Reason = "stored reduction is not a ChatResponse: " + err.Error()
			return out
		}
	}
	if ingest.ReducedResponseAbsent(stored) {
		out.Class = ClassSkippedNoReduction
		return out
	}

	reduction, err := ingest.ReduceStoredRawTurn(ctx, ingest.StoredRawTurn{
		Provider:            row.Provider,
		RawResponse:         row.RawResponse,
		RawResponseEncoding: row.RawResponseEncoding,
		Meta:                row.Meta,
	})
	if err != nil {
		switch {
		case errors.Is(err, ingest.ErrNoReducer):
			out.Class = ClassNoReducer
		case errors.Is(err, ingest.ErrDecode):
			out.Class = ClassUndecodable
		default:
			out.Class = ClassUnreducible
		}
		out.Reason = err.Error()
		return out
	}

	out.Truncated = reduction.Truncated
	out.Stamps = &StampCoverage{
		Duration:  reduction.DurationSource,
		CreatedAt: reduction.CreatedAtSource,
	}

	diffs, truncated, err := compare(row.StoredReduction, reduction.Response, maxDiffs)
	if err != nil {
		out.Class = ClassUnreducible
		out.Reason = err.Error()
		return out
	}
	out.Diffs = diffs
	out.DiffsTruncated = truncated
	if len(diffs) == 0 {
		out.Class = ClassEquivalent
	} else {
		out.Class = ClassDivergent
	}
	return out
}

// compare canonicalizes both reductions, removes the tolerated fields, and
// returns the bounded structural difference between what remains.
func compare(storedJSON json.RawMessage, recomputed *llm.ChatResponse, maxDiffs int) ([]FieldDiff, bool, error) {
	recomputedJSON, err := json.Marshal(recomputed)
	if err != nil {
		return nil, false, err
	}

	storedTree, err := decodeCanonical(storedJSON)
	if err != nil {
		return nil, false, err
	}
	recomputedTree, err := decodeCanonical(recomputedJSON)
	if err != nil {
		return nil, false, err
	}

	for _, f := range TolerableFields {
		prune(storedTree, f.Path)
		prune(recomputedTree, f.Path)
	}
	// A usage object that held nothing but the pruned duration is an artifact
	// of the pruning, not a difference: the adapter allocates Usage to carry
	// the wall clock, and ingest allocates it for the same reason when it
	// stamps. Dropping an emptied usage on both sides keeps that from
	// surfacing as a spurious object-vs-absent divergence.
	dropEmptyObject(storedTree, "usage")
	dropEmptyObject(recomputedTree, "usage")

	var diffs []FieldDiff
	diffValues("", storedTree, recomputedTree, &diffs, maxDiffs)
	return diffs, len(diffs) >= maxDiffs, nil
}

// dropEmptyObject removes key from a decoded object when its value is an
// object with no remaining members.
func dropEmptyObject(v any, key string) {
	obj, ok := v.(map[string]any)
	if !ok {
		return
	}
	child, ok := obj[key].(map[string]any)
	if ok && len(child) == 0 {
		delete(obj, key)
	}
}

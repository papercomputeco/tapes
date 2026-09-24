package storage

import (
	"context"
	"encoding/json"
	"iter"
	"time"
)

// Span-model read records: the derived trace/span/link projection
// (pkg/derive EmitSpans) as stored rows. Flat values — API callers
// never unwrap pgtype.

// SpanTurnRecord is one user-visible turn (trace).
type SpanTurnRecord struct {
	TraceID    string
	SessionID  string
	UserPrompt string
	// ResponsePreview is the derive-time fold of the closing spine llm
	// call's text output — the turn card's answer line.
	ResponsePreview string
	Synthetic       string
	Status          string
	// Source is the capture origin of the turn's raw rows ("wire" |
	// "transcript"), promoted from raw_turns.source at derive time.
	Source            string
	StartedAt         time.Time
	EndedAt           *time.Time
	DurationNS        int64
	TotalInputTokens  int64
	TotalOutputTokens int64
	// Main* counts only conversation-spine llm calls; the difference
	// from Total* is shadow spend.
	MainInputTokens     int64
	MainOutputTokens    int64
	CacheReadTokens     int64
	CacheCreationTokens int64
	TotalCostUSD        float64
}

// SpanRecord is one observed unit of work within a trace. Input and
// Output hold delta-only content-block arrays; Usage is the llm.Usage
// JSON for llm spans.
type SpanRecord struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Kind         string
	Name         string
	Status       string
	CallKind     string
	ThreadID     string
	Model        string
	StopReason   string
	StartedAt    time.Time
	DurationNS   int64
	// Seq is the deriver's emit ordinal within the trace —
	// presentation order, since started_at ties inside one llm call.
	Seq       int64
	Input     json.RawMessage
	Output    json.RawMessage
	Usage     json.RawMessage
	RawTurnID int64
	NodeHash  string
	// Verdict is the deriver-written security-monitor disposition JSON
	// (null on non-permission-check spans). Served verbatim on the wire.
	Verdict json.RawMessage
	// InputPreview / OutputPreview are the deriver-written bounded
	// previews of Input / Output (derive.PreviewBlocks), stored beside
	// the payload so a preview read never detoasts it. Nil when the row
	// was derived before previews were stored.
	InputPreview  json.RawMessage
	OutputPreview json.RawMessage
	// HasPreview reports whether the row carries stored previews. False
	// on rows derived before the preview columns existed and not yet
	// backfilled; readers serve those as pending rather than computing
	// a preview from the payload.
	HasPreview bool
}

// SpanLinkRecord is a dataflow edge between spans, possibly across
// traces (compaction seams).
type SpanLinkRecord struct {
	FromTraceID string
	FromSpanID  string
	FromIO      string
	ToTraceID   string
	ToSpanID    string
	ToIO        string
	Kind        string
}

// TraceSummaryRecord is a turn header with its span count — the lazy
// session-detail row (no payloads).
type TraceSummaryRecord struct {
	SpanTurnRecord
	SpanCount int
}

// RawTurnHeader is one wire-log row: capture identity and sizes, no
// payloads. The operator surface onto the raw layer.
//
// The sizes are read from the row's capture metadata, never measured
// from the stored payloads: a header listing that detoasted every blob
// to count its bytes would cost what the payload read costs, which is
// the read this surface exists to avoid.
type RawTurnHeader struct {
	ID         int64
	Source     string
	Provider   string
	AgentName  string
	RequestID  string
	ReceivedAt time.Time
	Meta       json.RawMessage

	// RequestBytes / ResponseBytes are the sizes the capture adapter
	// recorded in meta (request_bytes / response_bytes). 0 when the
	// producer did not report them or the value is not a number.
	RequestBytes  int64
	ResponseBytes int64

	// RawResponseBytes is the stored length of the verbatim upstream
	// response bytes; 0 when none were kept. It is read from the bytea
	// column's length, not the bytes.
	RawResponseBytes int64

	// RawResponseDropped marks a turn whose verbatim response existed
	// but was not kept — it exceeded the ingest cap or the producer
	// withheld it — so 0 raw bytes with the flag set is a fidelity gap,
	// not an absence. See RawTurnRecord.RawResponseDropped.
	RawResponseDropped bool
}

// PayloadMode selects which payload columns a span read carries. Full
// reads Input and Output; Preview reads only the deriver-written
// InputPreview / OutputPreview and leaves Input and Output nil, so a
// preview read never detoasts the payload it summarizes.
type PayloadMode string

const (
	PayloadFull    PayloadMode = "full"
	PayloadPreview PayloadMode = "preview"
)

// SpanCursor is a resumption point in a span stream: the composite key
// (trace_id, seq, started_at, span_id) of the last span a consumer has
// already seen, in the order IterateSessionSpans and ListSessionSpanModel
// serve rows. The zero value starts from the first span. Resumption is a
// strict row-value comparison on the key columns — all four for a
// session stream, the last three within one trace — so restarting from a
// cursor neither repeats nor skips a row, including across spans that
// share a seq (pre-re-derive rows all carry seq 0).
type SpanCursor struct {
	TraceID   string
	Seq       int64
	StartedAt time.Time
	SpanID    string
}

// IsZero reports whether the cursor is the start-of-stream marker.
func (c SpanCursor) IsZero() bool {
	return c.TraceID == "" && c.Seq == 0 && c.StartedAt.IsZero() && c.SpanID == ""
}

// SpanModelReader serves the span projection for session UIs.
type SpanModelReader interface {
	ListSessionSpanModel(ctx context.Context, sessionID string) ([]SpanTurnRecord, []SpanRecord, []SpanLinkRecord, error)
	// IterateSessionSpans streams a session's spans one row at a time in
	// the same composite order ListSessionSpanModel returns them
	// (trace_id, seq, started_at, span_id ASC), starting strictly after
	// `after` (zero value: from the first span). Server memory is bounded
	// by one span regardless of session size: each record is yielded and
	// released before the next row is read. Breaking out of the range
	// closes the underlying rows. A read failure — including context
	// cancellation — is surfaced as the final yielded error. mode selects
	// the payload columns: PayloadPreview reads the stored previews and
	// never the payload, so each record has Input and Output nil.
	IterateSessionSpans(ctx context.Context, sessionID string, after SpanCursor, mode PayloadMode) iter.Seq2[SpanRecord, error]
	// IterateTraceSpans streams one trace's spans in presentation order
	// (seq, started_at, span_id ASC), starting strictly after `after`
	// (zero value: from the first span). The cursor's TraceID is not part
	// of the comparison — the trace is fixed by the argument. Same memory,
	// error, and payload-mode contract as IterateSessionSpans.
	IterateTraceSpans(ctx context.Context, orgID, traceID string, after SpanCursor, mode PayloadMode) iter.Seq2[SpanRecord, error]
	ListTraceSummaries(ctx context.Context, sessionID string) ([]TraceSummaryRecord, error)
	// GetTraceSummary returns one turn header with its span count — the
	// payload-free half of a trace, which the standalone trace page
	// writes ahead of its first span. Returns nil when no such trace
	// exists in the org.
	GetTraceSummary(ctx context.Context, orgID, traceID string) (*TraceSummaryRecord, error)
	// ListTraceLinks returns the dataflow links touching one trace on
	// either end. Never nil: an edge-less trace lists an empty set.
	ListTraceLinks(ctx context.Context, orgID, traceID string) ([]SpanLinkRecord, error)
	// ListSessionLinks returns a session's dataflow links alone — the
	// payload-free half of ListSessionSpanModel. It backs the per-trace
	// streaming export, which loads the light turn headers and links whole
	// but reads the heavy spans one trace at a time.
	ListSessionLinks(ctx context.Context, sessionID string) ([]SpanLinkRecord, error)
	// ListTraceSpans returns one trace's spans whole, in presentation
	// order (the order IterateTraceSpans streams them), with full payloads.
	// It backs the per-trace streaming export.
	ListTraceSpans(ctx context.Context, orgID, traceID string) ([]SpanRecord, error)
	GetSpanRecord(ctx context.Context, orgID, traceID, spanID string) (*SpanRecord, error)
	ListRawTurnHeaders(ctx context.Context, orgID, harnessID, harnessSessionID string) ([]RawTurnHeader, error)
}

// SpanStats is the span-layer aggregate behind /v1/stats: trace-grain
// rollups summed over a time window, so the dashboard numbers agree
// with the session detail and trace views. TotalDurationNS is the sum
// of trace durations (agent time), not a wall-clock window.
type SpanStats struct {
	TurnCount           int
	SessionCount        int
	CompletedCount      int
	InputTokens         int64
	OutputTokens        int64
	CacheCreationTokens int64
	CacheReadTokens     int64
	TotalDurationNS     int64
	TotalCostUSD        float64
	ToolCalls           int
}

// SpanStatsReader is the capability interface for span-layer stats.
//
// authSubject narrows every total in the returned SpanStats to sessions
// captured for one gateway-stamped JWT subject; empty means the whole org, the
// only behavior there used to be. It is a filter and not an authorization
// boundary — the caller has already been scoped to a tenant, and this only
// chooses which of that tenant's rows to add up, the same way the auth_subject
// filter on ListSessionRecords does.
type SpanStatsReader interface {
	AggregateSpanStats(ctx context.Context, orgID string, since, until *time.Time, authSubject string) (SpanStats, error)
}

// SpanBackfillCursor is a resumption point in the preview backfill's
// scan: the (session_id, trace_id, span_id) key of the last span a
// batch returned, in the order ListSpansMissingPreviews serves rows. The
// zero value starts from the first span. Unlike started_at the key is
// unique, so resuming from it neither repeats nor skips a row.
type SpanBackfillCursor struct {
	SessionID string
	TraceID   string
	SpanID    string
}

// IsZero reports whether the cursor is the start-of-scan marker.
func (c SpanBackfillCursor) IsZero() bool {
	return c.SessionID == "" && c.TraceID == "" && c.SpanID == ""
}

// String renders the cursor as session/trace/span for progress logs.
func (c SpanBackfillCursor) String() string {
	return c.SessionID + "/" + c.TraceID + "/" + c.SpanID
}

// SpanBackfillRow is one span the preview backfill has to fill: its key
// and the payloads the previews are a pure function of. Nothing else is
// read — the job never re-derives.
type SpanBackfillRow struct {
	OrgID     string
	SessionID string
	TraceID   string
	SpanID    string
	Input     json.RawMessage
	Output    json.RawMessage
}

// Cursor returns the row's position in the backfill scan.
func (r SpanBackfillRow) Cursor() SpanBackfillCursor {
	return SpanBackfillCursor{SessionID: r.SessionID, TraceID: r.TraceID, SpanID: r.SpanID}
}

// SpanPreviewUpdate carries the computed previews for one span back to
// the store.
type SpanPreviewUpdate struct {
	OrgID         string
	TraceID       string
	SpanID        string
	InputPreview  json.RawMessage
	OutputPreview json.RawMessage
}

// PreviewBackfiller is the storage capability behind `tapes backfill
// previews`: fill input_preview / output_preview on rows derived before
// the columns existed, from the stored payload, without running the
// deriver.
type PreviewBackfiller interface {
	// ListSpansMissingPreviews returns up to limit spans that carry no
	// stored preview but do carry a payload, in (session_id, trace_id,
	// span_id) order, starting strictly after `after` (zero value: from
	// the first span). sessionID restricts the scan to one session;
	// empty means every session. An empty page means the scan is done.
	ListSpansMissingPreviews(ctx context.Context, after SpanBackfillCursor, sessionID string, limit int) ([]SpanBackfillRow, error)
	// SetSpanPreviews writes the two preview columns for each update in
	// one transaction and touches nothing else: not the payload, not
	// content_hash, not derive_seq. Writing a preview never advances a
	// change-feed cursor.
	SetSpanPreviews(ctx context.Context, updates []SpanPreviewUpdate) error
}

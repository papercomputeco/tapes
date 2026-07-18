package storage

import (
	"context"
	"encoding/json"
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
type RawTurnHeader struct {
	ID            int64
	Source        string
	Provider      string
	AgentName     string
	RequestID     string
	ReceivedAt    time.Time
	Meta          json.RawMessage
	RequestBytes  int64
	ResponseBytes int64
}

// SpanModelReader serves the span projection for session UIs.
type SpanModelReader interface {
	ListSessionSpanModel(ctx context.Context, sessionID string) ([]SpanTurnRecord, []SpanRecord, []SpanLinkRecord, error)
	ListTraceSummaries(ctx context.Context, sessionID string) ([]TraceSummaryRecord, error)
	// ListSessionLinks returns a session's dataflow links alone — the
	// payload-free half of ListSessionSpanModel. It backs the per-trace
	// streaming export, which loads the light turn headers and links whole
	// but reads the heavy spans one trace at a time.
	ListSessionLinks(ctx context.Context, sessionID string) ([]SpanLinkRecord, error)
	// ListTraceSpans returns one trace's spans in presentation order — the
	// same per-trace read GetTraceDetail performs, without the turn/link
	// round-trips. It backs the per-trace streaming export.
	ListTraceSpans(ctx context.Context, orgID, traceID string) ([]SpanRecord, error)
	GetTraceDetail(ctx context.Context, orgID, traceID string) (*SpanTurnRecord, []SpanRecord, []SpanLinkRecord, error)
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
type SpanStatsReader interface {
	AggregateSpanStats(ctx context.Context, orgID string, since, until *time.Time) (SpanStats, error)
}

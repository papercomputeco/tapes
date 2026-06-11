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
	TraceID           string
	SessionID         string
	UserPrompt        string
	Synthetic         string
	Status            string
	StartedAt         time.Time
	EndedAt           *time.Time
	DurationNS        int64
	TotalInputTokens  int64
	TotalOutputTokens int64
	TotalCostUSD      float64
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
	Input        json.RawMessage
	Output       json.RawMessage
	Usage        json.RawMessage
	RawTurnID    int64
	NodeHash     string
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

// SpanModelReader serves the span projection for session UIs.
type SpanModelReader interface {
	ListSessionSpanModel(ctx context.Context, sessionID string) ([]SpanTurnRecord, []SpanRecord, []SpanLinkRecord, error)
}

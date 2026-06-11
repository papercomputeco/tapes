package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

const (
	SpanKindAgent = "agent"
	SpanKindStep  = "step"
	SpanKindLLM   = "llm"
	SpanKindTool  = "tool"
)

// SpanIngester is the experimental Lapdog-style write model. It persists one
// captured provider turn as an explicit session -> turn/trace -> span graph
// instead of deriving continuity from Merkle node hashes.
type SpanIngester interface {
	IngestSpanTurn(ctx context.Context, req IngestSpanTurnRequest) (IngestSpanTurnResult, error)
}

// SpanReader is the experimental read model for trace/session UIs.
type SpanReader interface {
	ListTraceRecords(ctx context.Context, orgID string, limit int, cursorTs *time.Time, cursorTraceID *string) ([]TraceRecord, error)
	GetTrace(ctx context.Context, orgID, traceID string) (*TraceRecord, []SpanRecord, []SpanLinkRecord, error)
}

// SpanContext carries trace/span identity supplied by a harness extension at
// provider-request time. For Pi this is injected as X-Tapes-* headers by the
// POC extension so the proxy can persist the same IDs the harness UI saw.
type SpanContext struct {
	TraceID      string `json:"trace_id,omitempty"`
	TurnID       string `json:"turn_id,omitempty"`
	RootSpanID   string `json:"root_span_id,omitempty"`
	LLMSpanID    string `json:"llm_span_id,omitempty"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
}

// IngestSpanTurnRequest contains the provider-normalized request/response for
// a completed turn plus optional harness session/span attribution.
type IngestSpanTurnRequest struct {
	Session     *sessions.IngestEnvelope
	SpanContext *SpanContext
	Provider    string
	AgentName   string
	Project     string
	Request     *llm.ChatRequest
	Response    *llm.ChatResponse
	CostUSD     float64
}

// IngestSpanTurnResult reports the durable identity assigned to the turn.
type IngestSpanTurnResult struct {
	SessionID string
	TurnID    string
	TraceID   string
	SpanCount int
}

// TraceRecord is the list/detail representation of a user turn trace.
type TraceRecord struct {
	ID                string
	OrgID             string
	SessionID         string
	TraceID           string
	HarnessTurnID     string
	HarnessID         string
	HarnessSessionID  string
	Name              string
	Cwd               string
	UserPrompt        string
	Status            string
	StartedAt         time.Time
	EndedAt           *time.Time
	DurationNS        int64
	TotalInputTokens  int64
	TotalOutputTokens int64
	TotalCostUSD      float64
	SpanCount         int
	Metadata          map[string]any
}

// SpanRecord is a single observed agent/step/llm/tool span.
type SpanRecord struct {
	ID           string
	SessionID    string
	TurnID       string
	TraceID      string
	SpanID       string
	ParentSpanID string
	Kind         string
	Name         string
	Status       string
	StartNS      int64
	DurationNS   int64
	Input        json.RawMessage
	Output       json.RawMessage
	Metadata     json.RawMessage
	Metrics      json.RawMessage
	Raw          json.RawMessage
}

// SpanLinkRecord captures causal edges that are not tree containment, e.g.
// LLM.output -> Tool.input or Tool.output -> LLM.input.
type SpanLinkRecord struct {
	TraceID    string
	FromSpanID string
	ToSpanID   string
	FromIO     string
	ToIO       string
	Metadata   json.RawMessage
}

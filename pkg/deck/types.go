package deck

import (
	"context"
	"time"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// Querier is the interface the deck TUI uses to fetch session data. The
// HTTPQuery implementation in this package talks to a tapes API server
// (in-process or remote) over HTTP, reading the product session/trace
// surface (/v1/sessions, /v1/traces).
type Querier interface {
	Overview(ctx context.Context, filters Filters) (*Overview, error)
	SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error)
}

// OverviewPager is an optional extension implemented by query backends that
// can return bounded overview pages. TUI callers use it to lazily load more
// sessions without forcing every Querier implementation to support pagination.
type OverviewPager interface {
	OverviewPage(ctx context.Context, filters Filters, cursor string, limit int) (*OverviewPage, error)
}

// TurnQuerier is an optional extension implemented by query backends that can
// drill into a single turn's conversation (GET /v1/traces/{trace_id}). The
// TUI uses it for the per-turn drill-in; mock Queriers that don't implement
// it simply don't offer the drill-in.
type TurnQuerier interface {
	TurnConversation(ctx context.Context, traceID string) (*TurnConversation, error)
}

// OverviewPage is one page of the deck overview plus the API cursor needed to
// fetch the next page. HasMore is true when NextCursor is non-empty.
type OverviewPage struct {
	Overview   *Overview
	NextCursor string
	HasMore    bool
}

// Pricing aliases sessions.Pricing so the deck and the API both speak the
// same model-cost type. The standalone definition was removed when pricing
// logic moved to pkg/sessions.
type Pricing = sessions.Pricing

// SessionSummary aliases sessions.SessionSummary. The deck used to define
// its own copy with identical fields; the alias removes the duplication
// while keeping deck.SessionSummary working for the dozens of TUI sites
// that reference it.
type SessionSummary = sessions.SessionSummary

// ModelCost aliases sessions.ModelCost for the same reason.
type ModelCost = sessions.ModelCost

// TurnSummary is one user-visible turn header within a session, mirroring
// the API's trace summary rows (GET /v1/traces?session_id=). It carries the
// turn's folded rollups only; span payloads arrive via TurnConversation.
type TurnSummary struct {
	TraceID         string        `json:"trace_id"`
	UserPrompt      string        `json:"user_prompt,omitempty"`
	ResponsePreview string        `json:"response_preview,omitempty"`
	Status          string        `json:"status"`
	StartedAt       time.Time     `json:"started_at"`
	EndedAt         *time.Time    `json:"ended_at,omitempty"`
	Duration        time.Duration `json:"duration_ns"`
	InputTokens     int64         `json:"input_tokens"`
	OutputTokens    int64         `json:"output_tokens"`
	// Main* counts conversation-spine llm calls only; Total − Main is the
	// harness's shadow spend on the turn.
	MainInputTokens     int64   `json:"main_input_tokens"`
	MainOutputTokens    int64   `json:"main_output_tokens"`
	CacheReadTokens     int64   `json:"cache_read_tokens"`
	CacheCreationTokens int64   `json:"cache_creation_tokens"`
	TotalCost           float64 `json:"total_cost_usd"`
	SpanCount           int     `json:"span_count"`
}

// TurnConversation is the drill-in view of one turn: the conversation
// reconstructed from the trace's spans (GET /v1/traces/{trace_id}). Messages
// carries the conversation-spine llm calls' input/output blocks rendered as
// user/assistant messages in seq order; offshoot and injected spans are
// counted rather than interleaved.
type TurnConversation struct {
	Turn            TurnSummary           `json:"turn"`
	Messages        []SessionMessage      `json:"messages"`
	GroupedMessages []SessionMessageGroup `json:"grouped_messages,omitempty"`
	ToolFrequency   map[string]int        `json:"tool_frequency,omitempty"`
	// OffshootCalls counts llm spans whose call_kind marks them as harness
	// side-traffic (offshoot:*) rather than conversation spine.
	OffshootCalls int `json:"offshoot_calls"`
	// InjectedContexts counts injected:* spans (context the harness spliced
	// into the conversation without the user typing it).
	InjectedContexts int `json:"injected_contexts"`
}

// SessionMessage is the per-row render shape used by the deck's transcript
// views. At session grain each turn contributes a user/assistant pair built
// from the turn header (prompt + response preview); at turn grain rows come
// from the conversation-spine spans. It is built client-side and is not part
// of any HTTP API surface.
type SessionMessage struct {
	// TraceID is the turn this message belongs to — the drill-in key.
	TraceID      string        `json:"trace_id,omitempty"`
	Hash         string        `json:"hash"`
	Role         string        `json:"role"`
	Model        string        `json:"model"`
	Timestamp    time.Time     `json:"timestamp"`
	Delta        time.Duration `json:"delta_ns"`
	InputTokens  int64         `json:"input_tokens"`
	OutputTokens int64         `json:"output_tokens"`
	TotalTokens  int64         `json:"total_tokens"`
	InputCost    float64       `json:"input_cost"`
	OutputCost   float64       `json:"output_cost"`
	TotalCost    float64       `json:"total_cost"`
	ToolCalls    []string      `json:"tool_calls"`
	Text         string        `json:"text"`
}

// SessionMessageGroup is a batched run of adjacent same-role messages,
// used by the deck's transcript view to collapse rapid back-and-forth
// turns into a single visual entry.
type SessionMessageGroup struct {
	Role         string        `json:"role"`
	StartTime    time.Time     `json:"start_time"`
	EndTime      time.Time     `json:"end_time"`
	Delta        time.Duration `json:"delta_ns"`
	InputTokens  int64         `json:"input_tokens"`
	OutputTokens int64         `json:"output_tokens"`
	TotalTokens  int64         `json:"total_tokens"`
	InputCost    float64       `json:"input_cost"`
	OutputCost   float64       `json:"output_cost"`
	TotalCost    float64       `json:"total_cost"`
	ToolCalls    []string      `json:"tool_calls"`
	Text         string        `json:"text"`
	Count        int           `json:"count"`
	StartIndex   int           `json:"start_index"`
	EndIndex     int           `json:"end_index"`
}

// SessionDetail is the response a Querier returns from SessionDetail. It
// holds the session's SessionSummary plus its turn summaries. Messages is
// the turn list rendered as a user/assistant transcript (prompt + response
// preview per turn) so transcript consumers keep working at the new grain.
type SessionDetail struct {
	Summary         SessionSummary        `json:"summary"`
	Turns           []TurnSummary         `json:"turns,omitempty"`
	Messages        []SessionMessage      `json:"messages"`
	GroupedMessages []SessionMessageGroup `json:"grouped_messages,omitempty"`
}

// Overview is the response a Querier returns from Overview. It holds the
// filtered list of session summaries plus dashboard rollups.
type Overview struct {
	Sessions       []SessionSummary     `json:"sessions"`
	TotalCost      float64              `json:"total_cost"`
	TotalTokens    int64                `json:"total_tokens"`
	InputTokens    int64                `json:"input_tokens"`
	OutputTokens   int64                `json:"output_tokens"`
	TotalDuration  time.Duration        `json:"total_duration_ns"`
	TotalTurns     int                  `json:"total_turns"`
	SuccessRate    float64              `json:"success_rate"`
	Completed      int                  `json:"completed"`
	Failed         int                  `json:"failed"`
	Abandoned      int                  `json:"abandoned"`
	CostByModel    map[string]ModelCost `json:"cost_by_model"`
	PreviousPeriod *PeriodComparison    `json:"previous_period,omitempty"`
}

// PeriodComparison holds the previous-period metrics shown alongside the
// current period in the deck overview.
type PeriodComparison struct {
	TotalCost     float64       `json:"total_cost"`
	TotalTokens   int64         `json:"total_tokens"`
	TotalDuration time.Duration `json:"total_duration_ns"`
	TotalTurns    int           `json:"total_turns"`
	SuccessRate   float64       `json:"success_rate"`
	Completed     int           `json:"completed"`
}

// Filters describes the user-facing filter set the deck applies on top of
// the data returned by the API. Time filters (Since/From/To) are pushed
// down to /v1/sessions as since/until query params; Model, Status, and
// Project are evaluated client-side against the returned page because the
// sessions list endpoint does not filter on them.
type Filters struct {
	Since   time.Duration
	From    *time.Time
	To      *time.Time
	Model   string
	Status  string
	Project string
	Sort    string
	SortDir string
	Session string
}

// Status constants re-exported from pkg/sessions so existing TUI callers
// (`deck.StatusCompleted` etc.) keep working without an import change.
// /v1/sessions rows carry the same vocabulary in derived_status.
const (
	StatusCompleted = sessions.StatusCompleted
	StatusFailed    = sessions.StatusFailed
	StatusAbandoned = sessions.StatusAbandoned
	StatusUnknown   = sessions.StatusUnknown
)

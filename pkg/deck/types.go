package deck

import (
	"context"
	"time"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// Querier is the interface the deck TUI and web dashboard use to fetch
// session data. The HTTPQuery implementation in this package talks to a
// tapes API server (in-process or remote) over HTTP.
type Querier interface {
	Overview(ctx context.Context, filters Filters) (*Overview, error)
	SessionDetail(ctx context.Context, sessionID string) (*SessionDetail, error)
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

// SessionMessage is the per-turn render shape used by the deck's transcript
// view. It is built client-side from the API's Turn objects in HTTPQuery
// and is not part of any HTTP API surface.
type SessionMessage struct {
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
// holds the per-session SessionSummary plus its rendered transcript.
type SessionDetail struct {
	Summary         SessionSummary        `json:"summary"`
	Messages        []SessionMessage      `json:"messages"`
	GroupedMessages []SessionMessageGroup `json:"grouped_messages,omitempty"`
	ToolFrequency   map[string]int        `json:"tool_frequency"`
	SubSessions     []SessionSummary      `json:"sub_sessions,omitempty"`
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
	TotalToolCalls int                  `json:"total_tool_calls"`
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
	TotalCost      float64       `json:"total_cost"`
	TotalTokens    int64         `json:"total_tokens"`
	TotalDuration  time.Duration `json:"total_duration_ns"`
	TotalToolCalls int           `json:"total_tool_calls"`
	SuccessRate    float64       `json:"success_rate"`
	Completed      int           `json:"completed"`
}

// Filters describes the user-facing filter set the deck applies on top
// of the data returned by the API. Time filters are evaluated client-side
// against SessionSummary.StartTime / EndTime; the per-field string filters
// are also applied client-side after the rich /v1/sessions/summary fetch.
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
const (
	StatusCompleted = sessions.StatusCompleted
	StatusFailed    = sessions.StatusFailed
	StatusAbandoned = sessions.StatusAbandoned
	StatusUnknown   = sessions.StatusUnknown
)

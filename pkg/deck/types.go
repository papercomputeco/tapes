package deck

import "time"

type Pricing struct {
	Input  float64 `json:"input"`
	Output float64 `json:"output"`
}

type SessionSummary struct {
	ID           string        `json:"id"`
	Label        string        `json:"label"`
	Model        string        `json:"model"`
	Status       string        `json:"status"`
	StartTime    time.Time     `json:"start_time"`
	EndTime      time.Time     `json:"end_time"`
	Duration     time.Duration `json:"duration_ns"`
	InputTokens  int64         `json:"input_tokens"`
	OutputTokens int64         `json:"output_tokens"`
	InputCost    float64       `json:"input_cost"`
	OutputCost   float64       `json:"output_cost"`
	TotalCost    float64       `json:"total_cost"`
	ToolCalls    int           `json:"tool_calls"`
	MessageCount int           `json:"message_count"`
}

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

type SessionDetail struct {
	Summary       SessionSummary   `json:"summary"`
	Messages      []SessionMessage `json:"messages"`
	ToolFrequency map[string]int   `json:"tool_frequency"`
}

type ModelCost struct {
	Model        string  `json:"model"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	InputCost    float64 `json:"input_cost"`
	OutputCost   float64 `json:"output_cost"`
	TotalCost    float64 `json:"total_cost"`
	SessionCount int     `json:"session_count"`
}

type DeckOverview struct {
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
}

type Filters struct {
	Since   time.Duration
	From    *time.Time
	To      *time.Time
	Model   string
	Status  string
	Sort    string
	Session string
}

const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusAbandoned = "abandoned"
	StatusUnknown   = "unknown"
)

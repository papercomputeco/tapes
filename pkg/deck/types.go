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

type PeriodComparison struct {
	TotalCost      float64       `json:"total_cost"`
	TotalTokens    int64         `json:"total_tokens"`
	TotalDuration  time.Duration `json:"total_duration_ns"`
	TotalToolCalls int           `json:"total_tool_calls"`
	SuccessRate    float64       `json:"success_rate"`
	Completed      int           `json:"completed"`
}

type Filters struct {
	Since   time.Duration
	From    *time.Time
	To      *time.Time
	Model   string
	Status  string
	Sort    string
	SortDir string
	Session string
}

// SessionAnalytics holds per-session computed analytics.
type SessionAnalytics struct {
	SessionID         string  `json:"session_id"`
	UserMessageCount  int     `json:"user_message_count"`
	AssistantMsgCount int     `json:"assistant_message_count"`
	AvgResponseTimeNs int64   `json:"avg_response_time_ns"`
	LongestPauseNs    int64   `json:"longest_pause_ns"`
	UniqueTools       int     `json:"unique_tools"`
	ToolErrorCount    int     `json:"tool_error_count"`
	TokensPerMinute   float64 `json:"tokens_per_minute"`
	AvgPromptLength   int     `json:"avg_prompt_length"`
	AvgResponseLength int     `json:"avg_response_length"`
	FirstPrompt       string  `json:"first_prompt"`
}

// AnalyticsOverview holds cross-session analytics.
type AnalyticsOverview struct {
	TotalSessions     int                `json:"total_sessions"`
	AvgSessionCost    float64            `json:"avg_session_cost"`
	AvgDurationNs     int64              `json:"avg_duration_ns"`
	TopTools          []ToolMetric       `json:"top_tools"`
	ActivityByDay     []DayActivity      `json:"activity_by_day"`
	DurationBuckets   []Bucket           `json:"duration_buckets"`
	CostBuckets       []Bucket           `json:"cost_buckets"`
	ModelPerformance  []ModelPerformance `json:"model_performance"`
	ProviderBreakdown map[string]int     `json:"provider_breakdown"`
}

type ToolMetric struct {
	Name       string `json:"name"`
	Count      int    `json:"count"`
	ErrorCount int    `json:"error_count"`
	Sessions   int    `json:"sessions"`
}

type DayActivity struct {
	Date     string  `json:"date"`
	Sessions int     `json:"sessions"`
	Cost     float64 `json:"cost"`
	Tokens   int64   `json:"tokens"`
}

type Bucket struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

type ModelPerformance struct {
	Model          string  `json:"model"`
	Provider       string  `json:"provider"`
	Sessions       int     `json:"sessions"`
	AvgCost        float64 `json:"avg_cost"`
	AvgDurationNs  int64   `json:"avg_duration_ns"`
	AvgTokens      int64   `json:"avg_tokens"`
	TotalCost      float64 `json:"total_cost"`
	SuccessRate    float64 `json:"success_rate"`
	CompletedCount int     `json:"completed_count"`
}

const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusAbandoned = "abandoned"
	StatusUnknown   = "unknown"
)

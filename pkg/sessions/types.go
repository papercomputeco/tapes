// Package sessions provides the shared types and logic for building a
// per-session summary from a chain of merkle nodes. It is consumed by both
// the API server (for /v1/sessions/summary) and the deck TUI (for rendering).
//
// All functions in this package operate on *merkle.Node, not on any specific
// storage driver type. Callers are responsible for fetching and walking the
// ancestry chain; this package computes derived metadata over that chain.
package sessions

import "time"

// Pricing is the per-million-tokens price for a single model.
type Pricing struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cache_read"`
	CacheWrite float64 `json:"cache_write"`
}

// PricingTable maps model names (after normalization) to their Pricing.
type PricingTable map[string]Pricing

// SessionSummary is the aggregate view of a single session (a root-to-leaf
// chain). It is produced by BuildSummary and consumed by both the API's
// /v1/sessions/summary response and the deck TUI overview.
type SessionSummary struct {
	ID           string        `json:"id"`
	Label        string        `json:"label"`
	Model        string        `json:"model"`
	Project      string        `json:"project"`
	AgentName    string        `json:"agent_name,omitempty"`
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
	SessionCount int           `json:"session_count,omitempty"`
}

// ModelCost is a per-model aggregate of cost and token usage within a session
// or a collection of sessions.
type ModelCost struct {
	Model        string  `json:"model"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	InputCost    float64 `json:"input_cost"`
	OutputCost   float64 `json:"output_cost"`
	TotalCost    float64 `json:"total_cost"`
	SessionCount int     `json:"session_count"`
}

// Status values returned by DetermineStatus.
const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusAbandoned = "abandoned"
	StatusUnknown   = "unknown"
)

// Internal content-block constants used by the derivation helpers.
const (
	blockTypeToolUse = "tool_use"
	roleAssistant    = "assistant"
	roleUser         = "user"
)

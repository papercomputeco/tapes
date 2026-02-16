package llm

import (
	"encoding/json"
	"time"
)

// ChatResponse represents a provider-agnostic chat completion response.
// This is the internal representation used by the proxy after parsing
// provider-specific response formats.
type ChatResponse struct {
	// Model that generated the response
	Model string `json:"model"`

	// Response timestamp
	CreatedAt time.Time `json:"created_at,omitzero"`

	// The assistant's response message
	Message Message `json:"message"`

	// Whether generation is complete (for streaming)
	Done bool `json:"done"`

	// Stop reason (e.g., "stop", "length", "tool_use", "end_turn")
	StopReason string `json:"stop_reason,omitempty"`

	// Token usage and timing metrics
	Usage *Usage `json:"usage,omitempty"`

	// Provider-specific fields that don't map to common parameters
	Extra map[string]any `json:"extra,omitempty"`

	// RawResponse preserves the original response payload for cases where
	// parsing is incomplete or for debugging.
	RawResponse json.RawMessage `json:"raw_response,omitempty"`
}

// Usage contains token counts and timing information.
type Usage struct {
	// Token counts
	PromptTokens     int `json:"prompt_tokens,omitempty"`
	CompletionTokens int `json:"completion_tokens,omitempty"`
	TotalTokens      int `json:"total_tokens,omitempty"`

	// Cache token counts (Anthropic prompt caching)
	CacheCreationInputTokens int `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens,omitempty"`

	// Timing (provider-specific, but normalized to nanoseconds where possible)
	TotalDurationNs  int64 `json:"total_duration_ns,omitempty"`
	PromptDurationNs int64 `json:"prompt_duration_ns,omitempty"`
}

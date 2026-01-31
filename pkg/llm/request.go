package llm

import "encoding/json"

// ChatRequest represents a provider-agnostic chat completion request.
// This is the internal representation used by the proxy after parsing
// provider-specific request formats.
type ChatRequest struct {
	// Model name (e.g., "gpt-4", "claude-3-sonnet", "llama2")
	Model string `json:"model"`

	// Conversation messages
	Messages []Message `json:"messages"`

	// Whether to stream the response
	Stream *bool `json:"stream,omitempty"`

	// System prompt (some providers handle this separately from messages)
	System string `json:"system,omitempty"`

	// Generation parameters (unified across providers)
	MaxTokens   *int     `json:"max_tokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	TopP        *float64 `json:"top_p,omitempty"`
	TopK        *int     `json:"top_k,omitempty"`
	Stop        []string `json:"stop,omitempty"`
	Seed        *int     `json:"seed,omitempty"`

	// Provider-specific fields that don't map to common parameters
	Extra map[string]any `json:"extra,omitempty"`

	// RawRequest preserves the original request payload for cases where
	// parsing is incomplete or for debugging.
	RawRequest json.RawMessage `json:"raw_request,omitempty"`
}

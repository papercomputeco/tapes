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

	// Tools are the tool definitions offered to the model, preserved as
	// raw provider JSON. The definitions are large and provider-shaped;
	// callers that only need the count (e.g. shadow-call classification:
	// the security monitor sends zero tools, the main conversation sends
	// the full set) should use len(Tools).
	Tools []json.RawMessage `json:"tools,omitempty"`

	// Provider-specific fields that don't map to common parameters
	Extra map[string]any `json:"extra,omitempty"`

	// RawRequest preserves the original request payload for cases where
	// parsing is incomplete or for debugging.
	RawRequest json.RawMessage `json:"raw_request,omitempty"`
}

// RequestParams is the subset of request-envelope parameters promoted
// onto each node a captured call newly inserts. They identify the KIND
// of call that produced the node — main conversation vs harness shadow
// call (security monitor, title-gen, suggestion, …) — and are stored as
// queryable columns alongside the node without participating in the
// content-addressed hash.
//
// Pointer fields distinguish "absent from the request" (nil) from a
// zero value, mirroring the provider wire format.
type RequestParams struct {
	System      string   `json:"system,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
	Stream      *bool    `json:"stream,omitempty"`
	ToolCount   *int     `json:"tool_count,omitempty"`
}

// Params extracts the promotable request parameters from the parsed
// request. ToolCount is always concrete (a request with no tools field
// offered zero tools); Stream/MaxTokens/Temperature stay nil when the
// request omitted them.
func (r *ChatRequest) Params() *RequestParams {
	if r == nil {
		return nil
	}
	toolCount := len(r.Tools)
	return &RequestParams{
		System:      r.System,
		MaxTokens:   r.MaxTokens,
		Temperature: r.Temperature,
		Stream:      r.Stream,
		ToolCount:   &toolCount,
	}
}

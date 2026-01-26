package merkle

import "github.com/papercomputeco/tapes/pkg/llm"

// Bucket represents the hashable content stored in a Merkle DAG node.
// This is the canonical storage format for LLM conversation turns.
type Bucket struct {
	// Type identifies the kind of content (e.g., "message")
	Type string `json:"type"`

	// Role indicates who produced this message ("system", "user", "assistant", "tool")
	Role string `json:"role"`

	// Content holds the message content blocks
	Content []llm.ContentBlock `json:"content"`

	// Model identifies the LLM model (e.g., "gpt-4", "claude-3-sonnet")
	Model string `json:"model"`

	// Provider identifies the API provider (e.g., "openai", "anthropic", "ollama")
	Provider string `json:"provider"`

	// StopReason indicates why generation stopped (only for responses)
	// Values: "stop", "length", "tool_use", "end_turn", etc.
	StopReason string `json:"stop_reason,omitempty"`

	// Usage contains token counts and timing (only for responses)
	Usage *llm.Usage `json:"usage,omitempty"`
}

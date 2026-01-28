package merkle

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

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

// ExtractText returns the concatenated text content from the bucket's content blocks.
// This is useful for generating embeddings for semantic search.
// It extracts text from text blocks and tool outputs, joining them with newlines.
func (b *Bucket) ExtractText() string {
	var texts []string

	for _, block := range b.Content {
		switch {
		case block.Text != "":
			texts = append(texts, block.Text)
		case block.ToolOutput != "":
			texts = append(texts, block.ToolOutput)
		}
	}

	return strings.Join(texts, "\n")
}

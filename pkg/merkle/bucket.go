package merkle

import (
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Bucket represents the hashable content stored in a Merkle DAG node.
// This is the tapes canonical content-addressable hashing structure
// for all LLM conversation turns.
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

	// AgentName identifies the agent harness (e.g., "claude", "opencode", "codex")
	AgentName string `json:"agent_name,omitempty"`
}

// Clone returns a deep copy of the bucket with every string field and
// content block reallocated, so the copy shares no backing array with the
// zero-copy raw request buffer the bucket was parsed from. The bytes are
// identical to the original, so a node hashed from a cloned bucket
// produces the same Hash — cloning is a pure memory optimization, never a
// content change. See llm.ContentBlock.Clone for why the alias matters.
func (b *Bucket) Clone() Bucket {
	c := *b
	c.Type = strings.Clone(b.Type)
	c.Role = strings.Clone(b.Role)
	c.Model = strings.Clone(b.Model)
	c.Provider = strings.Clone(b.Provider)
	c.AgentName = strings.Clone(b.AgentName)
	if b.Content != nil {
		content := make([]llm.ContentBlock, len(b.Content))
		for i := range b.Content {
			content[i] = b.Content[i].Clone()
		}
		c.Content = content
	}
	return c
}

// ExtractText returns the concatenated text content from the bucket's content blocks.
// This is useful for generating embeddings for semantic search.
// It extracts text from text blocks, tool outputs, and tool use requests,
// joining them with newlines.
func (b *Bucket) ExtractText() string {
	var texts []string

	for _, block := range b.Content {
		switch {
		case block.Text != "":
			texts = append(texts, block.Text)
		case block.ToolOutput != "":
			texts = append(texts, block.ToolOutput)
		case block.ToolName != "":
			texts = append(texts, formatToolUse(block))
		}
	}

	return strings.Join(texts, "\n")
}

// formatToolUse creates a human-readable string representation of a tool use block.
// This enables semantic search to find assistant messages that invoke tools.
func formatToolUse(block llm.ContentBlock) string {
	var sb strings.Builder
	sb.WriteString("Tool call: ")
	sb.WriteString(block.ToolName)

	if len(block.ToolInput) > 0 {
		sb.WriteString("(")
		first := true
		for key, value := range block.ToolInput {
			if !first {
				sb.WriteString(", ")
			}
			first = false
			sb.WriteString(key)
			sb.WriteString(": ")
			sb.WriteString(fmt.Sprintf("%v", value))
		}
		sb.WriteString(")")
	}

	return sb.String()
}

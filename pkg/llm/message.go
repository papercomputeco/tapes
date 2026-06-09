package llm

import (
	"encoding/json"
	"strings"
)

// Message represents a single message in a conversation.
// Content is stored as an array of ContentBlocks to support multimodal content
// (text, images, tool use, etc.) in a provider-agnostic way.
type Message struct {
	Role    string         `json:"role"`    // "system", "user", "assistant", "tool"
	Content []ContentBlock `json:"content"` // Array of content blocks
}

// ContentBlock represents a single piece of content within a message.
// The Type field determines which other fields are populated.
type ContentBlock struct {
	Type string `json:"type"` // "text", "image", "tool_use", "tool_result", "thinking"

	// Text content (type="text")
	Text string `json:"text,omitempty"`

	// Image content (type="image")
	ImageURL    string `json:"image_url,omitempty"`    // URL to image
	ImageBase64 string `json:"image_base64,omitempty"` // Base64-encoded image data
	MediaType   string `json:"media_type,omitempty"`   // MIME type (e.g., "image/png")

	// Tool use (type="tool_use") - assistant requesting tool execution
	ToolUseID string         `json:"tool_use_id,omitempty"`
	ToolName  string         `json:"tool_name,omitempty"`
	ToolInput map[string]any `json:"tool_input,omitempty"`

	// Tool result (type="tool_result") - result from tool execution
	ToolResultID string `json:"tool_result_id,omitempty"` // References the tool_use_id
	ToolOutput   string `json:"tool_output,omitempty"`
	IsError      bool   `json:"is_error,omitempty"`

	// Thinking (type="thinking") - Anthropic extended-thinking blocks.
	// Anthropic emits thinking as content_block_delta frames with type
	// "thinking_delta" followed by a "signature_delta" that authenticates the
	// block. Consumers treat Thinking as opaque text; the signature is persisted
	// so downstream tooling can verify integrity.
	Thinking          string `json:"thinking,omitempty"`
	ThinkingSignature string `json:"thinking_signature,omitempty"`

	// Server tool use (type="server_tool_use") - an Anthropic-hosted tool the
	// model invokes server-side (e.g. web_search). Shaped exactly like
	// tool_use, so it reuses ToolUseID / ToolName / ToolInput.

	// Content (type="web_search_tool_result" and other server-tool results) -
	// the raw result payload Anthropic returns inline on the block, captured
	// verbatim as JSON so the variable result-object shapes survive without
	// imposing a schema. ToolResultID links it to the paired server_tool_use.
	Content json.RawMessage `json:"content,omitempty"`
}

// NewTextMessage creates a simple text message with the given role and content.
func NewTextMessage(role, text string) Message {
	return Message{
		Role: role,
		Content: []ContentBlock{
			{Type: "text", Text: text},
		},
	}
}

// GetText returns the concatenated text content from all text blocks in the message.
// This is a convenience method for simple text-only messages.
func (m *Message) GetText() string {
	var b strings.Builder
	for _, block := range m.Content {
		if block.Type == "text" {
			b.WriteString(block.Text)
		}
	}
	return b.String()
}

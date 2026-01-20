package llm

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
	Type string `json:"type"` // "text", "image", "tool_use", "tool_result"

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
	var result string
	for _, block := range m.Content {
		if block.Type == "text" {
			result += block.Text
		}
	}
	return result
}

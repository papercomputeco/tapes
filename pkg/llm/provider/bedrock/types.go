package bedrock

// bedrockRequest represents the AWS Bedrock InvokeModel request format for Claude.
// This is similar to the Anthropic Messages API but without a model field
// (the model is specified in the URL path) and with an anthropic_version field.
type bedrockRequest struct {
	AnthropicVersion string           `json:"anthropic_version,omitempty"`
	Model            string           `json:"model,omitempty"`
	Messages         []bedrockMessage `json:"messages"`
	System           any              `json:"system,omitempty"`
	MaxTokens        int              `json:"max_tokens"`
	Temperature      *float64         `json:"temperature,omitempty"`
	TopP             *float64         `json:"top_p,omitempty"`
	TopK             *int             `json:"top_k,omitempty"`
	Stop             []string         `json:"stop_sequences,omitempty"`
	Stream           *bool            `json:"stream,omitempty"`
}

// bedrockMessage represents a message in the Bedrock request/response format.
type bedrockMessage struct {
	Role string `json:"role"`

	// Union type: can be "string" or "[]bedrockContentBlock"
	Content any `json:"content"`
}

// bedrockContentBlock represents a content block in the Bedrock format.
type bedrockContentBlock struct {
	Type   string         `json:"type"`
	Text   string         `json:"text,omitempty"`
	Source *bedrockSource `json:"source,omitempty"`
	ID     string         `json:"id,omitempty"`
	Name   string         `json:"name,omitempty"`
	Input  map[string]any `json:"input,omitempty"`
}

type bedrockSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

// bedrockResponse represents the AWS Bedrock InvokeModel response format for Claude.
// This is identical to the Anthropic Messages API response format.
type bedrockResponse struct {
	ID           string                `json:"id"`
	Type         string                `json:"type"`
	Role         string                `json:"role"`
	Content      []bedrockContentBlock `json:"content"`
	Model        string                `json:"model"`
	StopReason   string                `json:"stop_reason"`
	StopSequence *string               `json:"stop_sequence,omitempty"`
	Usage        *bedrockUsage         `json:"usage,omitempty"`
}

type bedrockUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

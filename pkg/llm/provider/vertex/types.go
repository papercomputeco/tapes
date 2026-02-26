package vertex

// vertexRequest represents a Vertex AI request for Anthropic Claude models.
// This is the same as the Anthropic Messages API format with two differences:
// - "model" is omitted (it is specified in the Vertex AI endpoint URL)
// - "anthropic_version" is included in the body (not as a header)
type vertexRequest struct {
	AnthropicVersion string           `json:"anthropic_version,omitempty"`
	Model            string           `json:"model,omitempty"`
	Messages         []vertexMessage  `json:"messages"`
	System           any              `json:"system,omitempty"`
	MaxTokens        int              `json:"max_tokens"`
	Temperature      *float64         `json:"temperature,omitempty"`
	TopP             *float64         `json:"top_p,omitempty"`
	TopK             *int             `json:"top_k,omitempty"`
	Stop             []string         `json:"stop_sequences,omitempty"`
	Stream           *bool            `json:"stream,omitempty"`
}

type vertexMessage struct {
	Role string `json:"role"`

	// Union type: can be "string" or "[]vertexContentBlock"
	Content any `json:"content"`
}

type vertexContentBlock struct {
	Type   string        `json:"type"`
	Text   string        `json:"text,omitempty"`
	Source *vertexSource `json:"source,omitempty"`
	ID     string        `json:"id,omitempty"`
	Name   string        `json:"name,omitempty"`
	Input  map[string]any `json:"input,omitempty"`
}

type vertexSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

// vertexResponse represents a Vertex AI response for Anthropic Claude models.
// The response format is identical to the Anthropic Messages API.
type vertexResponse struct {
	ID           string               `json:"id"`
	Type         string               `json:"type"`
	Role         string               `json:"role"`
	Content      []vertexContentBlock `json:"content"`
	Model        string               `json:"model"`
	StopReason   string               `json:"stop_reason"`
	StopSequence *string              `json:"stop_sequence,omitempty"`
	Usage        *vertexUsage         `json:"usage,omitempty"`
}

type vertexUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

package openai

import "encoding/json"

// openaiRequest represents OpenAI's request format.
type openaiRequest struct {
	Model       string          `json:"model"`
	Messages    []openaiMessage `json:"messages"`
	MaxTokens   *int            `json:"max_tokens,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
	TopP        *float64        `json:"top_p,omitempty"`
	Stop        any             `json:"stop,omitempty"` // string or []string
	Seed        *int            `json:"seed,omitempty"`
	Stream      *bool           `json:"stream,omitempty"`
	// Additional OpenAI-specific fields
	FrequencyPenalty *float64       `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64       `json:"presence_penalty,omitempty"`
	ResponseFormat   map[string]any `json:"response_format,omitempty"`
	// Tools stay raw JSON: passed through to llm.ChatRequest.Tools verbatim.
	Tools []json.RawMessage `json:"tools,omitempty"`
}

// openaiMessage represents a message in OpenAI's format.
type openaiMessage struct {
	Role       string `json:"role"`
	Content    any    `json:"content"` // string or []openaiContentPart for vision
	Name       string `json:"name,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
	// ReasoningContent and Reasoning carry the non-standard reasoning trace
	// OpenAI-compatible reasoning models return, and that clients re-send on
	// assistant history during tool loops: reasoning_content (DeepSeek,
	// vLLM/SGLang reasoning parsers) and its successor reasoning (newer vLLM,
	// OpenRouter).
	ReasoningContent string        `json:"reasoning_content,omitempty"`
	Reasoning        lenientString `json:"reasoning,omitempty"`
	ToolCalls        []struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	} `json:"tool_calls,omitempty"`
}

// openaiResponse represents OpenAI's response format.
type openaiResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int           `json:"index"`
		Message      openaiMessage `json:"message"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage *openaiUsage `json:"usage,omitempty"`
}

type openaiUsage struct {
	PromptTokens        int                        `json:"prompt_tokens"`
	CompletionTokens    int                        `json:"completion_tokens"`
	TotalTokens         int                        `json:"total_tokens"`
	PromptTokensDetails *openaiPromptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

type openaiPromptTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

// reasoningText returns the message's reasoning trace. Servers migrating
// between the two field names send both with the same text, so
// reasoning_content wins rather than the trace being counted twice.
func (m openaiMessage) reasoningText() string {
	if m.ReasoningContent != "" {
		return m.ReasoningContent
	}
	return string(m.Reasoning)
}

// lenientString decodes a JSON string and treats any other value as empty.
// "reasoning" is not part of the Chat Completions schema, so a non-string
// there must not fail parsing the whole message.
type lenientString string

func (s *lenientString) UnmarshalJSON(data []byte) error {
	var v string
	if json.Unmarshal(data, &v) == nil {
		*s = lenientString(v)
	}
	return nil
}

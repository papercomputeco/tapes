// Package anthropic
package anthropic

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// provider implements the Provider interface for Anthropic's Claude API.
type provider struct{}

// New
func New() *provider { return &provider{} }

// Name
func (p *provider) Name() string {
	return "anthropic"
}

func (p *provider) CanHandle(payload []byte) bool {
	var probe struct {
		Model     string `json:"model"`
		MaxTokens *int   `json:"max_tokens"`

		// Union type: string or []ContentBlock
		System any `json:"system"`

		// Response-specific fields
		Type       string `json:"type"`
		StopReason string `json:"stop_reason"`
	}

	err := json.Unmarshal(payload, &probe)
	if err != nil {
		return false
	}

	// Check for Claude model names
	if strings.HasPrefix(probe.Model, "claude-") {
		return true
	}

	// Check for Anthropic response structure
	if probe.Type == "message" && probe.StopReason != "" {
		return true
	}

	// max_tokens is required for Anthropic, optional for others
	// combined with a top-level system field is a strong signal
	if probe.MaxTokens != nil && probe.System != nil {
		return true
	}

	return false
}

func (p *provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	var req anthropicRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}

	messages := make([]llm.Message, 0, len(req.Messages))
	for _, msg := range req.Messages {
		converted := llm.Message{Role: msg.Role}

		switch content := msg.Content.(type) {
		case string:
			converted.Content = []llm.ContentBlock{{Type: "text", Text: content}}
		case []any:
			// Parse as array of content blocks
			for _, item := range content {
				if block, ok := item.(map[string]any); ok {
					cb := llm.ContentBlock{}
					if t, ok := block["type"].(string); ok {
						cb.Type = t
					}
					if text, ok := block["text"].(string); ok {
						cb.Text = text
					}
					if source, ok := block["source"].(map[string]any); ok {
						if mt, ok := source["media_type"].(string); ok {
							cb.MediaType = mt
						}
						if data, ok := source["data"].(string); ok {
							cb.ImageBase64 = data
						}
					}

					// Tool use
					if id, ok := block["id"].(string); ok {
						cb.ToolUseID = id
					}
					if name, ok := block["name"].(string); ok {
						cb.ToolName = name
					}
					if input, ok := block["input"].(map[string]any); ok {
						cb.ToolInput = input
					}
					converted.Content = append(converted.Content, cb)
				}
			}
		}

		messages = append(messages, converted)
	}

	result := &llm.ChatRequest{
		Model:       req.Model,
		Messages:    messages,
		System:      req.System,
		MaxTokens:   &req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        req.TopK,
		Stop:        req.Stop,
		Stream:      req.Stream,
		RawRequest:  payload,
	}

	return result, nil
}

func (p *provider) ParseResponse(payload []byte) (*llm.ChatResponse, error) {
	var resp anthropicResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, err
	}

	// Convert content blocks
	content := make([]llm.ContentBlock, 0, len(resp.Content))
	for _, block := range resp.Content {
		cb := llm.ContentBlock{Type: block.Type}
		switch block.Type {
		case "text":
			cb.Text = block.Text
		case "tool_use":
			cb.ToolUseID = block.ID
			cb.ToolName = block.Name
			cb.ToolInput = block.Input
		}
		content = append(content, cb)
	}

	var usage *llm.Usage
	if resp.Usage != nil {
		usage = &llm.Usage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		}
	}

	result := &llm.ChatResponse{
		Model: resp.Model,
		Message: llm.Message{
			Role:    resp.Role,
			Content: content,
		},
		Done:        true,
		StopReason:  resp.StopReason,
		Usage:       usage,
		CreatedAt:   time.Now(),
		RawResponse: payload,
		Extra: map[string]any{
			"id":   resp.ID,
			"type": resp.Type,
		},
	}

	return result, nil
}

func (p *provider) ParseStreamChunk(payload []byte) (*llm.StreamChunk, error) {
	panic("not implemented")
}

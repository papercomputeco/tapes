// Package vertex implements the Provider interface for Anthropic Claude models
// accessed through Google Cloud's Vertex AI platform.
//
// The Vertex AI Claude API is nearly identical to the Anthropic Messages API,
// with two key differences:
//   - "model" is not passed in the request body (it is specified in the Vertex AI endpoint URL)
//   - "anthropic_version" is passed in the request body (rather than as a header)
package vertex

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Provider implements the Provider interface for Vertex AI (Anthropic Claude).
type Provider struct{}

// New
func New() *Provider { return &Provider{} }

// Name
func (p *Provider) Name() string {
	return "vertex"
}

// DefaultStreaming is false - Vertex AI requires explicit "stream": true.
func (p *Provider) DefaultStreaming() bool {
	return false
}

func (p *Provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	var req vertexRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}

	system := parseVertexSystem(req.System)
	messages := make([]llm.Message, 0, len(req.Messages))
	for _, msg := range req.Messages {
		converted := llm.Message{Role: msg.Role}

		switch content := msg.Content.(type) {
		case string:
			converted.Content = []llm.ContentBlock{{Type: "text", Text: content}}
		case []any:
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

	extra := map[string]any{}
	if req.AnthropicVersion != "" {
		extra["anthropic_version"] = req.AnthropicVersion
	}

	result := &llm.ChatRequest{
		Model:       req.Model,
		Messages:    messages,
		System:      system,
		MaxTokens:   &req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        req.TopK,
		Stop:        req.Stop,
		Stream:      req.Stream,
		RawRequest:  payload,
	}

	if len(extra) > 0 {
		result.Extra = extra
	}

	return result, nil
}

func parseVertexSystem(system any) string {
	if system == nil {
		return ""
	}

	switch value := system.(type) {
	case string:
		return value
	case []any:
		var builder strings.Builder
		for _, item := range value {
			block, ok := item.(map[string]any)
			if !ok {
				continue
			}
			blockType, _ := block["type"].(string)
			text, _ := block["text"].(string)
			if blockType == "text" && text != "" {
				if builder.Len() > 0 {
					builder.WriteString("\n")
				}
				builder.WriteString(text)
			}
		}
		return builder.String()
	default:
		return ""
	}
}

func (p *Provider) ParseResponse(payload []byte) (*llm.ChatResponse, error) {
	var resp vertexResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, err
	}

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

func (p *Provider) ParseStreamChunk(_ []byte) (*llm.StreamChunk, error) {
	panic("not implemented")
}

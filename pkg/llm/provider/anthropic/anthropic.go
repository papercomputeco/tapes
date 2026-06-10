// Package anthropic
package anthropic

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Provider implements the Provider interface for Anthropic's Claude API.
type Provider struct{}

// New
func New() *Provider { return &Provider{} }

// Name
func (p *Provider) Name() string {
	return "anthropic"
}

// DefaultStreaming is false - Anthropic requires explicit "stream": true.
func (p *Provider) DefaultStreaming() bool {
	return false
}

func (p *Provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	var req anthropicRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}

	system := parseAnthropicSystem(req.System)
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

					// tool_use and server_tool_use share the id / name / input
					// shape. Guard on type so the extraction is explicit and
					// symmetric with ParseResponse, rather than relying on these
					// keys happening to be absent on other block types.
					if cb.Type == "tool_use" || cb.Type == "server_tool_use" {
						if id, ok := block["id"].(string); ok {
							cb.ToolUseID = id
						}
						if name, ok := block["name"].(string); ok {
							cb.ToolName = name
						}
						if input, ok := block["input"].(map[string]any); ok {
							cb.ToolInput = input
						}
					}

					// Tool result. Anthropic permits the `content` field to be
					// either a string or an array of text/image content blocks;
					// flatten any text into ToolOutput. Guard on cb.Type so a
					// future block type that exposes a top-level `content` key
					// can't silently populate ToolOutput.
					if cb.Type == "tool_result" {
						if toolUseID, ok := block["tool_use_id"].(string); ok {
							cb.ToolResultID = toolUseID
						}
						if isError, ok := block["is_error"].(bool); ok {
							cb.IsError = isError
						}
						switch tc := block["content"].(type) {
						case string:
							cb.ToolOutput = tc
						case []any:
							var parts []string
							for _, item := range tc {
								part, ok := item.(map[string]any)
								if !ok {
									continue
								}
								if text, ok := part["text"].(string); ok {
									parts = append(parts, text)
								}
							}
							cb.ToolOutput = strings.Join(parts, "\n")
						}
					}
					// web_search_tool_result: server-tool result echoed back in
					// request history. tool_use_id links to the server_tool_use;
					// content is the raw result array, preserved verbatim.
					if cb.Type == "web_search_tool_result" {
						if toolUseID, ok := block["tool_use_id"].(string); ok {
							cb.ToolResultID = toolUseID
						}
						if raw, ok := block["content"]; ok {
							if encoded, err := json.Marshal(raw); err == nil {
								cb.Content = encoded
							}
						}
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
		System:      system,
		MaxTokens:   &req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		TopK:        req.TopK,
		Stop:        req.Stop,
		Stream:      req.Stream,
		Tools:       req.Tools,
		RawRequest:  payload,
	}

	return result, nil
}

func parseAnthropicSystem(system any) string {
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
		case "thinking":
			cb.Thinking = block.Thinking
			cb.ThinkingSignature = block.Signature
		case "server_tool_use":
			cb.ToolUseID = block.ID
			cb.ToolName = block.Name
			cb.ToolInput = block.Input
		case "web_search_tool_result":
			cb.ToolResultID = block.ToolUseID
			cb.Content = block.Content
		}
		content = append(content, cb)
	}

	var usage *llm.Usage
	if resp.Usage != nil {
		totalInput := resp.Usage.InputTokens + resp.Usage.CacheCreationInputTokens + resp.Usage.CacheReadInputTokens
		usage = &llm.Usage{
			PromptTokens:             totalInput,
			CompletionTokens:         resp.Usage.OutputTokens,
			TotalTokens:              totalInput + resp.Usage.OutputTokens,
			CacheCreationInputTokens: resp.Usage.CacheCreationInputTokens,
			CacheReadInputTokens:     resp.Usage.CacheReadInputTokens,
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

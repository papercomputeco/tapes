// Package openai
package openai

import (
	"encoding/json"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// provider implements the Provider interface for OpenAI's Chat Completions API.
type provider struct{}

func New() *provider { return &provider{} }

func (o *provider) Name() string {
	return "openai"
}

func (o *provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	var req openaiRequest
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
			// Multimodal content (e.g., vision)
			for _, item := range content {
				if part, ok := item.(map[string]any); ok {
					cb := llm.ContentBlock{}
					if t, ok := part["type"].(string); ok {
						cb.Type = t
					}
					if text, ok := part["text"].(string); ok {
						cb.Text = text
					}
					if imageURL, ok := part["image_url"].(map[string]any); ok {
						cb.Type = "image"
						if url, ok := imageURL["url"].(string); ok {
							cb.ImageURL = url
						}
					}
					converted.Content = append(converted.Content, cb)
				}
			}
		case nil:
			// Empty content (can happen with tool calls)
			converted.Content = []llm.ContentBlock{}
		}

		// Handle tool calls in assistant messages
		for _, tc := range msg.ToolCalls {
			var input map[string]any
			json.Unmarshal([]byte(tc.Function.Arguments), &input)
			converted.Content = append(converted.Content, llm.ContentBlock{
				Type:      "tool_use",
				ToolUseID: tc.ID,
				ToolName:  tc.Function.Name,
				ToolInput: input,
			})
		}

		// Handle tool results
		if msg.Role == "tool" && msg.ToolCallID != "" {
			text := ""
			if s, ok := msg.Content.(string); ok {
				text = s
			}
			converted.Content = []llm.ContentBlock{{
				Type:         "tool_result",
				ToolResultID: msg.ToolCallID,
				ToolOutput:   text,
			}}
		}

		messages = append(messages, converted)
	}

	// Parse stop sequences
	var stop []string
	switch s := req.Stop.(type) {
	case string:
		stop = []string{s}
	case []any:
		for _, item := range s {
			if str, ok := item.(string); ok {
				stop = append(stop, str)
			}
		}
	}

	result := &llm.ChatRequest{
		Model:       req.Model,
		Messages:    messages,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		Stop:        stop,
		Seed:        req.Seed,
		Stream:      req.Stream,
		RawRequest:  payload,
	}

	// Preserve OpenAI-specific fields
	if req.FrequencyPenalty != nil || req.PresencePenalty != nil || req.ResponseFormat != nil {
		result.Extra = make(map[string]any)
		if req.FrequencyPenalty != nil {
			result.Extra["frequency_penalty"] = *req.FrequencyPenalty
		}
		if req.PresencePenalty != nil {
			result.Extra["presence_penalty"] = *req.PresencePenalty
		}
		if req.ResponseFormat != nil {
			result.Extra["response_format"] = req.ResponseFormat
		}
	}

	return result, nil
}

func (o *provider) ParseResponse(payload []byte) (*llm.ChatResponse, error) {
	var resp openaiResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, err
	}

	if len(resp.Choices) == 0 {
		// Return empty response if no choices
		return &llm.ChatResponse{
			Model:       resp.Model,
			Done:        true,
			RawResponse: payload,
		}, nil
	}

	choice := resp.Choices[0]
	msg := choice.Message

	// Convert message content
	var content []llm.ContentBlock
	switch c := msg.Content.(type) {
	case string:
		content = []llm.ContentBlock{{Type: "text", Text: c}}
	case []any:
		for _, item := range c {
			if part, ok := item.(map[string]any); ok {
				cb := llm.ContentBlock{}
				if t, ok := part["type"].(string); ok {
					cb.Type = t
				}
				if text, ok := part["text"].(string); ok {
					cb.Text = text
				}
				content = append(content, cb)
			}
		}
	case nil:
		content = []llm.ContentBlock{}
	}

	// Handle tool calls
	for _, tc := range msg.ToolCalls {
		var input map[string]any
		json.Unmarshal([]byte(tc.Function.Arguments), &input)
		content = append(content, llm.ContentBlock{
			Type:      "tool_use",
			ToolUseID: tc.ID,
			ToolName:  tc.Function.Name,
			ToolInput: input,
		})
	}

	var usage *llm.Usage
	if resp.Usage != nil {
		usage = &llm.Usage{
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
		}
	}

	result := &llm.ChatResponse{
		Model: resp.Model,
		Message: llm.Message{
			Role:    msg.Role,
			Content: content,
		},
		Done:        true,
		StopReason:  choice.FinishReason,
		Usage:       usage,
		CreatedAt:   time.Unix(resp.Created, 0),
		RawResponse: payload,
		Extra: map[string]any{
			"id":     resp.ID,
			"object": resp.Object,
		},
	}

	return result, nil
}

func (o *provider) ParseStreamChunk(payload []byte) (*llm.StreamChunk, error) {
	panic("Not yet implemented")
}

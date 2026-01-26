package ollama

import (
	"encoding/json"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// provider implements the Provider interface for Ollama's API.
type provider struct{}

func New() *provider { return &provider{} }

func (o *provider) Name() string {
	return "ollama"
}

func (o *provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	var req ollamaRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}

	messages := make([]llm.Message, 0, len(req.Messages))
	for _, msg := range req.Messages {
		converted := llm.Message{
			Role:    msg.Role,
			Content: []llm.ContentBlock{},
		}

		// Add text content if present
		if msg.Content != "" {
			converted.Content = append(converted.Content, llm.ContentBlock{Type: "text", Text: msg.Content})
		}

		// Handle images
		for _, img := range msg.Images {
			converted.Content = append(converted.Content, llm.ContentBlock{
				Type:        "image",
				ImageBase64: img,
			})
		}

		// Handle tool calls in assistant messages
		for _, tc := range msg.ToolCalls {
			converted.Content = append(converted.Content, llm.ContentBlock{
				Type:      "tool_use",
				ToolUseID: tc.ID,
				ToolName:  tc.Function.Name,
				ToolInput: tc.Function.Arguments,
			})
		}

		messages = append(messages, converted)
	}

	result := &llm.ChatRequest{
		Model:      req.Model,
		Messages:   messages,
		Stream:     req.Stream,
		RawRequest: payload,
	}

	// Map options to common fields
	if req.Options != nil {
		result.Temperature = req.Options.Temperature
		result.TopP = req.Options.TopP
		result.TopK = req.Options.TopK
		result.Seed = req.Options.Seed
		result.MaxTokens = req.Options.NumPredict
		result.Stop = req.Options.Stop

		// Preserve Ollama-specific options
		result.Extra = make(map[string]any)
		if req.Options.NumCtx != nil {
			result.Extra["num_ctx"] = *req.Options.NumCtx
		}
		if req.Options.RepeatPenalty != nil {
			result.Extra["repeat_penalty"] = *req.Options.RepeatPenalty
		}
		if req.Options.RepeatLastN != nil {
			result.Extra["repeat_last_n"] = *req.Options.RepeatLastN
		}
	}

	// Preserve other Ollama-specific fields
	if req.Format != "" {
		if result.Extra == nil {
			result.Extra = make(map[string]any)
		}
		result.Extra["format"] = req.Format
	}
	if req.KeepAlive != "" {
		if result.Extra == nil {
			result.Extra = make(map[string]any)
		}
		result.Extra["keep_alive"] = req.KeepAlive
	}

	return result, nil
}

func (o *provider) ParseResponse(payload []byte) (*llm.ChatResponse, error) {
	var resp ollamaResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, err
	}

	// Convert message content
	var content []llm.ContentBlock

	// Add text content if present
	if resp.Message.Content != "" {
		content = append(content, llm.ContentBlock{Type: "text", Text: resp.Message.Content})
	}

	// Handle images in response (if any)
	for _, img := range resp.Message.Images {
		content = append(content, llm.ContentBlock{
			Type:        "image",
			ImageBase64: img,
		})
	}

	// Handle tool calls
	for _, tc := range resp.Message.ToolCalls {
		content = append(content, llm.ContentBlock{
			Type:      "tool_use",
			ToolUseID: tc.ID,
			ToolName:  tc.Function.Name,
			ToolInput: tc.Function.Arguments,
		})
	}

	// Map Ollama metrics to common Usage format
	var usage *llm.Usage
	if resp.PromptEvalCount > 0 || resp.EvalCount > 0 || resp.TotalDuration > 0 {
		usage = &llm.Usage{
			PromptTokens:     resp.PromptEvalCount,
			CompletionTokens: resp.EvalCount,
			TotalTokens:      resp.PromptEvalCount + resp.EvalCount,
			TotalDurationNs:  resp.TotalDuration,
			PromptDurationNs: resp.PromptEvalDuration,
		}
	}

	// Determine stop reason - use DoneReason if available, otherwise default to "stop" if done
	stopReason := resp.DoneReason
	if stopReason == "" && resp.Done {
		stopReason = "stop"
	}

	result := &llm.ChatResponse{
		Model: resp.Model,
		Message: llm.Message{
			Role:    resp.Message.Role,
			Content: content,
		},
		Done:        resp.Done,
		StopReason:  stopReason,
		Usage:       usage,
		CreatedAt:   resp.CreatedAt,
		RawResponse: payload,
	}

	// Preserve Ollama-specific fields
	if resp.Context != nil {
		result.Extra = map[string]any{
			"context":       resp.Context,
			"load_duration": resp.LoadDuration,
			"eval_duration": resp.EvalDuration,
		}
	}

	return result, nil
}

func (o *provider) ParseStreamChunk(payload []byte) (*llm.StreamChunk, error) {
	panic("Not yet implemented")
}

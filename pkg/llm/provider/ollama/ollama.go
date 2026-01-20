package ollama

import (
	"encoding/json"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// provider implements the Provider interface for provider's API.
type provider struct{}

func New() *provider { return &provider{} }

func (o *provider) Name() string {
	return "ollama"
}

func (o *provider) CanHandle(payload []byte) bool {
	var probe struct {
		KeepAlive string `json:"keep_alive"`
		Options   any    `json:"options"`
		Context   []int  `json:"context"`

		// Ollama-specific response fields
		TotalDuration int64 `json:"total_duration"`
		EvalCount     int   `json:"eval_count"`
	}

	if err := json.Unmarshal(payload, &probe); err != nil {
		return false
	}

	// Check for Ollama-specific request fields
	if probe.KeepAlive != "" || probe.Options != nil {
		return true
	}

	// Check for Ollama-specific response fields
	if probe.Context != nil || probe.TotalDuration > 0 || probe.EvalCount > 0 {
		return true
	}

	return false
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
			Content: []llm.ContentBlock{{Type: "text", Text: msg.Content}},
		}

		// Handle images
		for _, img := range msg.Images {
			converted.Content = append(converted.Content, llm.ContentBlock{
				Type:        "image",
				ImageBase64: img,
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
	content := []llm.ContentBlock{{Type: "text", Text: resp.Message.Content}}

	// Handle images in response (if any)
	for _, img := range resp.Message.Images {
		content = append(content, llm.ContentBlock{
			Type:        "image",
			ImageBase64: img,
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

	// Determine stop reason
	stopReason := ""
	if resp.Done {
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

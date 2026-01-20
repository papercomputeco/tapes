package besteffort

import (
	"encoding/json"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// provider implements the Provider interface as a fallback for unknown API formats.
// It attempts to extract common fields from any JSON payload and stores the raw
// payload when parsing is incomplete.
type provider struct{}

func New() *provider { return &provider{} }

func (b *provider) Name() string {
	return "besteffort"
}

// CanHandle always returns true - this is the fallback provider.
func (b *provider) CanHandle(payload []byte) bool {
	return true
}

// ParseRequest attempts to extract a ChatRequest from an unknown format.
// It looks for common field names across different LLM APIs.
func (b *provider) ParseRequest(payload []byte) (*llm.ChatRequest, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		// If we can't even parse as JSON, return minimal request with raw payload
		return &llm.ChatRequest{
			RawRequest: payload,
			Extra: map[string]any{
				"parse_error": err.Error(),
			},
		}, nil
	}

	result := &llm.ChatRequest{
		RawRequest: payload,
		Extra:      make(map[string]any),
	}

	// Extract model - common across all providers
	result.Model = extractString(raw, "model")

	// Extract messages - try various common field names
	if messages := extractMessages(raw); len(messages) > 0 {
		result.Messages = messages
	}

	// Extract system prompt (Anthropic-style separate system)
	if system := extractString(raw, "system"); system != "" {
		result.System = system
	}

	// Extract streaming flag
	if stream, ok := raw["stream"].(bool); ok {
		result.Stream = &stream
	}

	// Extract generation parameters - try multiple common names
	result.MaxTokens = extractIntPtr(raw, "max_tokens", "max_new_tokens", "num_predict", "maxTokens")
	result.Temperature = extractFloat64Ptr(raw, "temperature")
	result.TopP = extractFloat64Ptr(raw, "top_p", "topP")
	result.TopK = extractIntPtr(raw, "top_k", "topK")
	result.Seed = extractIntPtr(raw, "seed")
	result.Stop = extractStringSlice(raw, "stop", "stop_sequences", "stopSequences")

	// Store any unrecognized fields in Extra
	knownFields := map[string]bool{
		"model": true, "messages": true, "system": true, "stream": true,
		"max_tokens": true, "max_new_tokens": true, "num_predict": true, "maxTokens": true,
		"temperature": true, "top_p": true, "topP": true, "top_k": true, "topK": true,
		"seed": true, "stop": true, "stop_sequences": true, "stopSequences": true,
		"prompt": true, "input": true,
	}
	for k, v := range raw {
		if !knownFields[k] {
			result.Extra[k] = v
		}
	}

	return result, nil
}

// ParseResponse attempts to extract a ChatResponse from an unknown format.
// It tries multiple extraction strategies for different API structures.
func (b *provider) ParseResponse(payload []byte) (*llm.ChatResponse, error) {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		// If we can't parse as JSON, return minimal response with raw payload
		return &llm.ChatResponse{
			Done:        true,
			RawResponse: payload,
			Extra: map[string]any{
				"parse_error": err.Error(),
			},
		}, nil
	}

	result := &llm.ChatResponse{
		Done:        true,
		CreatedAt:   time.Now(),
		RawResponse: payload,
		Extra:       make(map[string]any),
	}

	// Extract model
	result.Model = extractString(raw, "model")

	// Try to extract content using multiple strategies
	content := b.extractResponseContent(raw)
	role := b.extractResponseRole(raw)
	if role == "" {
		role = "assistant"
	}

	result.Message = llm.Message{
		Role:    role,
		Content: content,
	}

	// Extract stop reason
	result.StopReason = extractString(raw, "stop_reason", "finish_reason", "stop_sequence")

	// Extract usage metrics
	result.Usage = b.extractUsage(raw)

	// Extract done/complete flag
	if done, ok := raw["done"].(bool); ok {
		result.Done = done
	}

	// Store unrecognized fields
	knownFields := map[string]bool{
		"model": true, "message": true, "content": true, "text": true, "output": true, "response": true,
		"role": true, "choices": true, "stop_reason": true, "finish_reason": true,
		"usage": true, "done": true, "created": true, "created_at": true,
	}
	for k, v := range raw {
		if !knownFields[k] {
			result.Extra[k] = v
		}
	}

	return result, nil
}

// ParseStreamChunk is not implemented for BestEffort.
func (b *provider) ParseStreamChunk(payload []byte) (*llm.StreamChunk, error) {
	panic("Not yet implemented")
}

// extractResponseContent tries multiple strategies to find the response content.
func (b *provider) extractResponseContent(raw map[string]any) []llm.ContentBlock {
	// Strategy 1: OpenAI-style choices[0].message.content
	if choices, ok := raw["choices"].([]any); ok && len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			if msg, ok := choice["message"].(map[string]any); ok {
				if content := extractString(msg, "content"); content != "" {
					return []llm.ContentBlock{{Type: "text", Text: content}}
				}
			}
			// Also try delta for streaming
			if delta, ok := choice["delta"].(map[string]any); ok {
				if content := extractString(delta, "content"); content != "" {
					return []llm.ContentBlock{{Type: "text", Text: content}}
				}
			}
		}
	}

	// Strategy 2: Anthropic-style content array
	if contentArr, ok := raw["content"].([]any); ok {
		var blocks []llm.ContentBlock
		for _, item := range contentArr {
			if block, ok := item.(map[string]any); ok {
				cb := llm.ContentBlock{}
				if t, ok := block["type"].(string); ok {
					cb.Type = t
				} else {
					cb.Type = "text"
				}
				if text, ok := block["text"].(string); ok {
					cb.Text = text
				}
				blocks = append(blocks, cb)
			}
		}
		if len(blocks) > 0 {
			return blocks
		}
	}

	// Strategy 3: Ollama-style message.content
	if msg, ok := raw["message"].(map[string]any); ok {
		if content := extractString(msg, "content"); content != "" {
			return []llm.ContentBlock{{Type: "text", Text: content}}
		}
	}

	// Strategy 4: Direct content string
	if content := extractString(raw, "content"); content != "" {
		return []llm.ContentBlock{{Type: "text", Text: content}}
	}

	// Strategy 5: Generic text/output/response fields
	for _, field := range []string{"text", "output", "response", "generated_text", "result"} {
		if text := extractString(raw, field); text != "" {
			return []llm.ContentBlock{{Type: "text", Text: text}}
		}
	}

	// Fallback: empty content, raw payload is preserved in RawResponse
	return []llm.ContentBlock{}
}

// extractResponseRole tries to find the role in the response.
func (b *provider) extractResponseRole(raw map[string]any) string {
	// Direct role field
	if role := extractString(raw, "role"); role != "" {
		return role
	}

	// OpenAI-style choices[0].message.role
	if choices, ok := raw["choices"].([]any); ok && len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			if msg, ok := choice["message"].(map[string]any); ok {
				if role := extractString(msg, "role"); role != "" {
					return role
				}
			}
		}
	}

	// Ollama-style message.role
	if msg, ok := raw["message"].(map[string]any); ok {
		if role := extractString(msg, "role"); role != "" {
			return role
		}
	}

	return ""
}

// extractUsage tries to find usage metrics in various formats.
func (b *provider) extractUsage(raw map[string]any) *llm.Usage {
	usage := &llm.Usage{}
	found := false

	// Check for usage object
	if u, ok := raw["usage"].(map[string]any); ok {
		// OpenAI style
		if v, ok := u["prompt_tokens"].(float64); ok {
			usage.PromptTokens = int(v)
			found = true
		}
		if v, ok := u["completion_tokens"].(float64); ok {
			usage.CompletionTokens = int(v)
			found = true
		}
		if v, ok := u["total_tokens"].(float64); ok {
			usage.TotalTokens = int(v)
			found = true
		}

		// Anthropic style
		if v, ok := u["input_tokens"].(float64); ok {
			usage.PromptTokens = int(v)
			found = true
		}
		if v, ok := u["output_tokens"].(float64); ok {
			usage.CompletionTokens = int(v)
			found = true
		}
	}

	// Ollama style (top-level fields)
	if v, ok := raw["prompt_eval_count"].(float64); ok {
		usage.PromptTokens = int(v)
		found = true
	}
	if v, ok := raw["eval_count"].(float64); ok {
		usage.CompletionTokens = int(v)
		found = true
	}
	if v, ok := raw["total_duration"].(float64); ok {
		usage.TotalDurationNs = int64(v)
		found = true
	}

	if !found {
		return nil
	}

	// Calculate total if not present
	if usage.TotalTokens == 0 && (usage.PromptTokens > 0 || usage.CompletionTokens > 0) {
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	return usage
}

// extractMessages attempts to extract messages from various formats.
func extractMessages(raw map[string]any) []llm.Message {
	// Try "messages" field (most common)
	if msgs, ok := raw["messages"].([]any); ok {
		return parseMessageArray(msgs)
	}

	// Try "prompt" as a single user message
	if prompt, ok := raw["prompt"].(string); ok && prompt != "" {
		return []llm.Message{llm.NewTextMessage("user", prompt)}
	}

	// Try "input" as a single user message
	if input, ok := raw["input"].(string); ok && input != "" {
		return []llm.Message{llm.NewTextMessage("user", input)}
	}

	// Try "inputs" array
	if inputs, ok := raw["inputs"].([]any); ok {
		var messages []llm.Message
		for _, input := range inputs {
			if text, ok := input.(string); ok {
				messages = append(messages, llm.NewTextMessage("user", text))
			}
		}
		if len(messages) > 0 {
			return messages
		}
	}

	return nil
}

// parseMessageArray converts a generic message array to our format.
func parseMessageArray(msgs []any) []llm.Message {
	var messages []llm.Message
	for _, m := range msgs {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}

		role := extractString(msg, "role")
		if role == "" {
			role = "user" // Default to user if no role specified
		}

		converted := llm.Message{Role: role}

		// Try content as string
		if content, ok := msg["content"].(string); ok {
			converted.Content = []llm.ContentBlock{{Type: "text", Text: content}}
		} else if contentArr, ok := msg["content"].([]any); ok {
			// Content is an array (Anthropic/OpenAI vision style)
			for _, item := range contentArr {
				if block, ok := item.(map[string]any); ok {
					cb := llm.ContentBlock{}
					if t, ok := block["type"].(string); ok {
						cb.Type = t
					} else {
						cb.Type = "text"
					}
					if text, ok := block["text"].(string); ok {
						cb.Text = text
					}
					// Handle images
					if source, ok := block["source"].(map[string]any); ok {
						if data, ok := source["data"].(string); ok {
							cb.ImageBase64 = data
						}
						if mt, ok := source["media_type"].(string); ok {
							cb.MediaType = mt
						}
					}
					if imageURL, ok := block["image_url"].(map[string]any); ok {
						if url, ok := imageURL["url"].(string); ok {
							cb.ImageURL = url
						}
					}
					converted.Content = append(converted.Content, cb)
				}
			}
		}

		messages = append(messages, converted)
	}
	return messages
}

// Helper functions for extracting values from maps

func extractString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key].(string); ok {
			return v
		}
	}
	return ""
}

func extractIntPtr(m map[string]any, keys ...string) *int {
	for _, key := range keys {
		switch v := m[key].(type) {
		case float64:
			i := int(v)
			return &i
		case int:
			return &v
		}
	}
	return nil
}

func extractFloat64Ptr(m map[string]any, keys ...string) *float64 {
	for _, key := range keys {
		if v, ok := m[key].(float64); ok {
			return &v
		}
	}
	return nil
}

func extractStringSlice(m map[string]any, keys ...string) []string {
	for _, key := range keys {
		switch v := m[key].(type) {
		case string:
			return []string{v}
		case []any:
			var result []string
			for _, item := range v {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			if len(result) > 0 {
				return result
			}
		case []string:
			return v
		}
	}
	return nil
}

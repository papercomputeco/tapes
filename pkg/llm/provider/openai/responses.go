package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// OpenAI Responses API support. Codex and other Responses-first harnesses
// POST /v1/responses with an `input` item list instead of Chat Completions'
// `messages`, so the provider sniffs the request shape and parses each with
// its own mapping into the canonical llm.ChatRequest.

// responsesRequest is the subset of the Responses API request body tapes
// maps onto llm.ChatRequest. `input` is either a bare string or an array of
// items; items are kept raw and decoded per type.
type responsesRequest struct {
	Model           string          `json:"model"`
	Input           json.RawMessage `json:"input"`
	Instructions    string          `json:"instructions,omitempty"`
	MaxOutputTokens *int            `json:"max_output_tokens,omitempty"`
	Temperature     *float64        `json:"temperature,omitempty"`
	TopP            *float64        `json:"top_p,omitempty"`
	Stream          *bool           `json:"stream,omitempty"`
	Reasoning       map[string]any  `json:"reasoning,omitempty"`
	PreviousID      string          `json:"previous_response_id,omitempty"`
}

// responsesItem is the union of the Responses input/output item shapes tapes
// understands. Codex omits `"type":"message"` on plain role/content items,
// so a missing type with a role present is treated as a message.
type responsesItem struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`

	// function_call / function_call_output
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Arguments string          `json:"arguments"`
	Output    json.RawMessage `json:"output"`

	// reasoning
	Summary          []responsesContentPart `json:"summary"`
	EncryptedContent string                 `json:"encrypted_content"`
}

// responsesContentPart is one entry of a message item's content array
// (input_text / output_text / summary_text / refusal / input_image, ...).
type responsesContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Refusal  string `json:"refusal"`
	ImageURL string `json:"image_url"`
}

// isResponsesRequest reports whether the payload is a Responses API request
// rather than Chat Completions: Responses carries `input`, never `messages`.
func isResponsesRequest(payload []byte) bool {
	var probe struct {
		Messages json.RawMessage `json:"messages"`
		Input    json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return false
	}
	return jsonFieldPresent(probe.Input) && !jsonFieldPresent(probe.Messages)
}

// jsonFieldPresent reports whether a raw field carried a real value: a
// missing key decodes to an empty RawMessage, but an explicit JSON null
// keeps the literal `null` bytes and must count as absent too — routing
// `{"input": null}` to the Responses parser would fabricate an empty
// user message out of nothing.
func jsonFieldPresent(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}

// parseResponsesRequest maps a Responses API request to llm.ChatRequest.
// Instructions become the system prompt; input items become messages with
// the same role conventions as the Chat Completions mapping (tool calls on
// assistant messages, tool outputs on role "tool").
func parseResponsesRequest(payload []byte) (*llm.ChatRequest, error) {
	var req responsesRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}

	var messages []llm.Message
	if len(req.Input) > 0 {
		var inputText string
		if err := json.Unmarshal(req.Input, &inputText); err == nil {
			messages = []llm.Message{llm.NewTextMessage("user", inputText)}
		} else {
			var items []json.RawMessage
			if err := json.Unmarshal(req.Input, &items); err != nil {
				return nil, fmt.Errorf("responses input is neither string nor item array: %w", err)
			}
			messages = make([]llm.Message, 0, len(items))
			for _, raw := range items {
				msg, ok := responsesItemToMessage(raw)
				if ok {
					messages = append(messages, msg)
				}
			}
		}
	}

	result := &llm.ChatRequest{
		Model:       req.Model,
		Messages:    messages,
		System:      req.Instructions,
		MaxTokens:   req.MaxOutputTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		Stream:      req.Stream,
		RawRequest:  payload,
	}

	extra := map[string]any{"endpoint": "responses"}
	if len(req.Reasoning) > 0 {
		extra["reasoning"] = req.Reasoning
	}
	if req.PreviousID != "" {
		extra["previous_response_id"] = req.PreviousID
	}
	result.Extra = extra

	return result, nil
}

// responsesItemToMessage maps one Responses item to a canonical message.
// Unknown item types are preserved as a single raw-content block rather than
// dropped, so novel Codex tool shapes survive capture verbatim.
func responsesItemToMessage(raw json.RawMessage) (llm.Message, bool) {
	var item responsesItem
	if err := json.Unmarshal(raw, &item); err != nil {
		return llm.Message{}, false
	}

	switch {
	case item.Type == "message" || (item.Type == "" && item.Role != ""):
		return llm.Message{
			Role:    item.Role,
			Content: responsesContentBlocks(item.Content),
		}, true

	case item.Type == "function_call":
		return llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{functionCallBlock(item, raw)},
		}, true

	case item.Type == "function_call_output":
		return llm.Message{
			Role: "tool",
			Content: []llm.ContentBlock{{
				Type:         "tool_result",
				ToolResultID: item.CallID,
				ToolOutput:   rawToText(item.Output),
			}},
		}, true

	case item.Type == "reasoning":
		return llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{reasoningBlock(item)},
		}, true

	default:
		return llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: item.Type, Content: raw}},
		}, true
	}
}

// responsesContentBlocks maps a message item's content — bare string or
// part array — to canonical content blocks.
func responsesContentBlocks(raw json.RawMessage) []llm.ContentBlock {
	if len(raw) == 0 {
		return []llm.ContentBlock{}
	}

	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return []llm.ContentBlock{{Type: "text", Text: text}}
	}

	var parts []responsesContentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return []llm.ContentBlock{{Type: "text", Content: raw}}
	}

	blocks := make([]llm.ContentBlock, 0, len(parts))
	for _, part := range parts {
		switch part.Type {
		case "input_text", "output_text", "summary_text":
			blocks = append(blocks, llm.ContentBlock{Type: "text", Text: part.Text})
		case "refusal":
			blocks = append(blocks, llm.ContentBlock{Type: "refusal", Text: part.Refusal})
		case "input_image":
			blocks = append(blocks, llm.ContentBlock{Type: "image", ImageURL: part.ImageURL})
		default:
			blocks = append(blocks, llm.ContentBlock{Type: part.Type, Text: part.Text})
		}
	}
	return blocks
}

// functionCallBlock maps a function_call item to a tool_use block. The
// Responses wire carries arguments as a JSON string; when it does not parse
// as an object the raw item is preserved on Content so nothing is lost.
func functionCallBlock(item responsesItem, raw json.RawMessage) llm.ContentBlock {
	cb := llm.ContentBlock{
		Type:      "tool_use",
		ToolUseID: item.CallID,
		ToolName:  item.Name,
	}
	if item.Arguments != "" {
		var input map[string]any
		if err := json.Unmarshal([]byte(item.Arguments), &input); err == nil {
			cb.ToolInput = input
		} else {
			cb.Content = raw
		}
	}
	return cb
}

// reasoningBlock maps a reasoning item to a thinking block. The summary text
// is the only human-readable part; encrypted_content is the opaque
// continuation blob, preserved on the signature field like Anthropic's
// thinking signatures.
func reasoningBlock(item responsesItem) llm.ContentBlock {
	var b strings.Builder
	for _, part := range item.Summary {
		if b.Len() > 0 && part.Text != "" {
			b.WriteString("\n")
		}
		b.WriteString(part.Text)
	}
	return llm.ContentBlock{
		Type:              "thinking",
		Thinking:          b.String(),
		ThinkingSignature: item.EncryptedContent,
	}
}

// rawToText renders a function_call_output payload as text: bare strings
// unquote, structured outputs stay compact JSON.
func rawToText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

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
	Model           string            `json:"model"`
	Input           json.RawMessage   `json:"input"`
	Instructions    string            `json:"instructions,omitempty"`
	MaxOutputTokens *int              `json:"max_output_tokens,omitempty"`
	Temperature     *float64          `json:"temperature,omitempty"`
	TopP            *float64          `json:"top_p,omitempty"`
	Stream          *bool             `json:"stream,omitempty"`
	Tools           []json.RawMessage `json:"tools,omitempty"`
	ToolChoice      json.RawMessage   `json:"tool_choice,omitempty"`
	Reasoning       map[string]any    `json:"reasoning,omitempty"`
	PreviousID      string            `json:"previous_response_id,omitempty"`
}

// Responses item types for the custom-tool spine (GPT-5.6 Codex's
// freeform exec runtime). Shared by the request parser and the
// derive-time content normalizer.
const (
	itemCustomToolCall       = "custom_tool_call"
	itemCustomToolCallOutput = "custom_tool_call_output"
)

// responsesItem is the union of the Responses input/output item shapes tapes
// understands. Codex omits `"type":"message"` on plain role/content items,
// so a missing type with a role present is treated as a message.
type responsesItem struct {
	Type      string          `json:"type"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	Author    string          `json:"author"`
	Recipient string          `json:"recipient"`

	// function_call / function_call_output; custom_tool_call shares
	// call_id/name and carries its freeform input on `input`.
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Arguments string          `json:"arguments"`
	Input     string          `json:"input"`
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
		Tools:       req.Tools,
		RawRequest:  payload,
	}

	extra := map[string]any{"endpoint": "responses"}
	if threadID := responsesThreadIDFromInput(req.Input); threadID != "" {
		extra["thread_id"] = threadID
	}
	if len(req.Reasoning) > 0 {
		extra["reasoning"] = req.Reasoning
	}
	if req.PreviousID != "" {
		extra["previous_response_id"] = req.PreviousID
	}
	// GPT-5.6 Codex omits client-side tool definitions (the backend
	// injects them server-side) but still declares tool routing via
	// tool_choice — preserve it so classification can recognize the
	// conversation spine without a `tools` array.
	if jsonFieldPresent(req.ToolChoice) {
		extra["tool_choice"] = string(req.ToolChoice)
	}
	result.Extra = extra

	return result, nil
}

// responsesThreadIDFromInput recovers Codex collaboration child identity from
// the request envelope. Codex does not currently send the capture-side
// thread-id header used by Claude, but child calls carry an agent_message
// addressed to /root/<task> whose visible prefix says NEW_TASK. Parent-thread
// FINAL_ANSWER handbacks are deliberately excluded.
func responsesThreadIDFromInput(input json.RawMessage) string {
	var items []json.RawMessage
	if json.Unmarshal(input, &items) != nil {
		return ""
	}
	return responsesThreadID(items)
}

func responsesThreadID(items []json.RawMessage) string {
	for _, raw := range items {
		var item responsesItem
		if json.Unmarshal(raw, &item) != nil || item.Type != "agent_message" {
			continue
		}
		if item.Recipient == "" || item.Recipient == "/root" {
			continue
		}
		if strings.Contains(rawToText(item.Content), "Message Type: NEW_TASK") {
			return item.Recipient
		}
	}
	return ""
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

	case item.Type == itemCustomToolCall:
		return llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{customToolCallBlock(item)},
		}, true

	case item.Type == itemCustomToolCallOutput:
		return llm.Message{
			Role:    "tool",
			Content: []llm.ContentBlock{customToolCallOutputBlock(item)},
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

// ResponsesItemContentBlocks exposes the canonical content projection for one
// Responses item. Harness transcript adapters use the same mapper as wire
// requests so reconciliation signatures cannot drift between capture sources.
func ResponsesItemContentBlocks(raw json.RawMessage) []llm.ContentBlock {
	msg, ok := responsesItemToMessage(raw)
	if !ok {
		return nil
	}
	return msg.Content
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

// customToolCallBlock maps a custom_tool_call item to a tool_use block.
// Custom tools (GPT-5.6 Codex's exec runtime) carry a freeform string
// instead of JSON arguments; it lands under the "input" key so the block
// stays a structured tool call rather than an opaque blob. Only the
// call identity fields participate, so a response item (which also
// carries id/status) and its request-echo map to identical blocks.
func customToolCallBlock(item responsesItem) llm.ContentBlock {
	cb := llm.ContentBlock{
		Type:      "tool_use",
		ToolUseID: item.CallID,
		ToolName:  item.Name,
	}
	if item.Input != "" {
		cb.ToolInput = map[string]any{"input": item.Input}
	}
	return cb
}

// customToolCallOutputBlock maps a custom_tool_call_output item to a
// tool_result block. The wire output is either a bare string or an
// array of content parts ({type:"input_text",text:...}); parts are
// joined so the result reads as the text the model saw.
func customToolCallOutputBlock(item responsesItem) llm.ContentBlock {
	return llm.ContentBlock{
		Type:         "tool_result",
		ToolResultID: item.CallID,
		ToolOutput:   customToolOutputText(item.Output),
	}
}

// customToolOutputText renders a custom_tool_call_output payload as
// text: bare strings unquote, content-part arrays join their text, and
// anything else stays compact JSON so nothing is lost.
func customToolOutputText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var parts []responsesContentPart
	if err := json.Unmarshal(raw, &parts); err == nil {
		var b strings.Builder
		for _, part := range parts {
			b.WriteString(part.Text)
		}
		return b.String()
	}
	return string(raw)
}

// NormalizeResponsesContent rewrites Responses output items that the
// capture reducer preserved verbatim — custom_tool_call and
// custom_tool_call_output — into the canonical tool blocks the derived
// layer understands. The reducer keeps unknown item types raw by
// design, so this runs at derive time (chain construction), which also
// heals sessions whose responses were reduced before these item types
// were cataloged: a re-derive reprojects them from the preserved raw
// item. Blocks of any other type pass through untouched.
func NormalizeResponsesContent(blocks []llm.ContentBlock) []llm.ContentBlock {
	normalized := blocks
	copied := false
	for i, b := range blocks {
		if (b.Type != itemCustomToolCall && b.Type != itemCustomToolCallOutput) || len(b.Content) == 0 {
			continue
		}
		var item responsesItem
		if err := json.Unmarshal(b.Content, &item); err != nil || item.CallID == "" {
			continue
		}
		if !copied {
			normalized = make([]llm.ContentBlock, len(blocks))
			copy(normalized, blocks)
			copied = true
		}
		switch b.Type {
		case itemCustomToolCall:
			normalized[i] = customToolCallBlock(item)
		case itemCustomToolCallOutput:
			normalized[i] = customToolCallOutputBlock(item)
		}
	}
	return normalized
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

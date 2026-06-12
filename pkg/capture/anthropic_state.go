package capture

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// turnState holds the per-turn reducer state while the Anthropic state
// machine walks the event sequence. It is local to a single Reduce call and
// is not shared across goroutines.
type turnState struct {
	id           string
	model        string
	role         string
	stopReason   string
	stopSequence *string

	// blocks keyed by the Anthropic "index" field. Anthropic guarantees
	// indices are small non-negative ints increasing from 0, but we stay
	// defensive by using a map so out-of-order events don't corrupt content.
	blocks map[int]*blockState

	// maxIndex tracks the highest index we've seen so final ordering is
	// deterministic regardless of map iteration order.
	maxIndex int

	// toolInputFragments holds the concatenated input_json_delta partials for
	// each tool_use block until the matching content_block_stop tries to
	// parse them.
	toolInputFragments map[int]*strings.Builder

	usage llm.Usage

	// errorHit is set when an error event arrived mid-stream. The reducer
	// still returns a ChatResponse with whatever content accumulated, flagged
	// via Extra["error"].
	errorType    string
	errorMessage string

	// toolInputErrors accumulates diagnostics for any tool_use block whose
	// input_json_delta fragments didn't parse. Surfaced via Extra so
	// downstream consumers can see what broke without the ToolInput field
	// silently changing shape.
	toolInputErrors []map[string]any

	// seenMessageStop records whether the stream completed cleanly.
	seenMessageStop bool
}

type blockState struct {
	kind      string // "text" | "tool_use" | "thinking" | "server_tool_use" | "web_search_tool_result"
	text      strings.Builder
	thinking  strings.Builder
	signature strings.Builder
	toolID    string
	toolName  string
	toolInput map[string]any
	// web_search_tool_result: tool_use_id links to the paired server_tool_use,
	// content is the inline result payload (a JSON array), captured verbatim.
	toolUseID string
	content   json.RawMessage
}

func newTurnState() *turnState {
	return &turnState{
		blocks:             map[int]*blockState{},
		toolInputFragments: map[int]*strings.Builder{},
	}
}

func (s *turnState) onMessageStart(data []byte) error {
	var ev anthropicMessageStart
	if err := unmarshalStrict(data, &ev, "message_start"); err != nil {
		return err
	}
	s.id = ev.Message.ID
	s.model = ev.Message.Model
	s.role = ev.Message.Role
	if ev.Message.Usage != nil {
		s.usage.PromptTokens = ev.Message.Usage.InputTokens +
			ev.Message.Usage.CacheCreationInputTokens +
			ev.Message.Usage.CacheReadInputTokens
		s.usage.CacheCreationInputTokens = ev.Message.Usage.CacheCreationInputTokens
		s.usage.CacheReadInputTokens = ev.Message.Usage.CacheReadInputTokens
	}
	return nil
}

func (s *turnState) onContentBlockStart(data []byte) error {
	var ev anthropicContentBlockStart
	if err := unmarshalStrict(data, &ev, "content_block_start"); err != nil {
		return err
	}

	b := &blockState{kind: ev.ContentBlock.Type}
	switch ev.ContentBlock.Type {
	case blockTypeText:
		if ev.ContentBlock.Text != "" {
			b.text.WriteString(ev.ContentBlock.Text)
		}
	case blockTypeToolUse:
		b.toolID = ev.ContentBlock.ID
		b.toolName = ev.ContentBlock.Name
		// Initial input is rarely present for streamed tool_use — fragments
		// arrive via input_json_delta. Anthropic sends `"input":{}` as a
		// placeholder in content_block_start, which unmarshals to an empty
		// non-nil map; treat empty as "unset" so ToolInput stays nil when
		// no deltas produce parsed content.
		if len(ev.ContentBlock.Input) > 0 {
			b.toolInput = ev.ContentBlock.Input
		}
	case blockTypeThinking:
		if ev.ContentBlock.Thinking != "" {
			b.thinking.WriteString(ev.ContentBlock.Thinking)
		}
		if ev.ContentBlock.Signature != "" {
			b.signature.WriteString(ev.ContentBlock.Signature)
		}
	case blockTypeServerToolUse:
		// Same wire shape as tool_use — input streams via input_json_delta.
		b.toolID = ev.ContentBlock.ID
		b.toolName = ev.ContentBlock.Name
		if len(ev.ContentBlock.Input) > 0 {
			b.toolInput = ev.ContentBlock.Input
		}
	case blockTypeWebSearchToolResult:
		// Arrives fully formed in content_block_start (no deltas follow).
		b.toolUseID = ev.ContentBlock.ToolUseID
		if len(ev.ContentBlock.Content) > 0 {
			b.content = ev.ContentBlock.Content
		}
	}
	s.blocks[ev.Index] = b
	if ev.Index > s.maxIndex {
		s.maxIndex = ev.Index
	}
	return nil
}

func (s *turnState) onContentBlockDelta(data []byte) error {
	var ev anthropicContentBlockDelta
	if err := unmarshalStrict(data, &ev, "content_block_delta"); err != nil {
		return err
	}
	b, ok := s.blocks[ev.Index]
	if !ok {
		// content_block_delta for an unopened block — synthesize a text block
		// and continue rather than reject. Matches the "preserve what we can"
		// stance for partial captures.
		b = &blockState{kind: blockTypeText}
		s.blocks[ev.Index] = b
		if ev.Index > s.maxIndex {
			s.maxIndex = ev.Index
		}
	}
	switch ev.Delta.Type {
	case "text_delta":
		b.text.WriteString(ev.Delta.Text)
	case "input_json_delta":
		buf, ok := s.toolInputFragments[ev.Index]
		if !ok {
			buf = &strings.Builder{}
			s.toolInputFragments[ev.Index] = buf
		}
		buf.WriteString(ev.Delta.PartialJSON)
	case "thinking_delta":
		b.thinking.WriteString(ev.Delta.Thinking)
	case "signature_delta":
		b.signature.WriteString(ev.Delta.Signature)
	}
	return nil
}

func (s *turnState) onContentBlockStop(data []byte) error {
	var ev anthropicContentBlockStop
	if err := unmarshalStrict(data, &ev, "content_block_stop"); err != nil {
		return err
	}
	// If this block accumulated input_json_delta fragments, parse them now so
	// the final tool_use block carries a structured ToolInput.
	b, ok := s.blocks[ev.Index]
	if !ok {
		return nil
	}
	if buf, ok := s.toolInputFragments[ev.Index]; ok && (b.kind == blockTypeToolUse || b.kind == blockTypeServerToolUse) {
		raw := buf.String()
		if raw != "" {
			var parsed map[string]any
			if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
				b.toolInput = parsed
			} else {
				// Parse failure: leave ToolInput nil so downstream consumers
				// that check `ToolInput != nil` don't mistake a broken payload
				// for a valid tool call. Record the raw fragment in
				// Extra["tool_input_parse_errors"] and the error so the turn
				// is still preserved with the metadata operators need.
				s.recordToolInputError(b.toolID, raw, err.Error())
			}
		}
		delete(s.toolInputFragments, ev.Index)
	}
	return nil
}

// recordToolInputError appends a diagnostic entry for a tool_use block whose
// input_json_delta fragments didn't parse as JSON.
func (s *turnState) recordToolInputError(toolID, raw, errMsg string) {
	s.toolInputErrors = append(s.toolInputErrors, map[string]any{
		"tool_use_id": toolID,
		"raw":         raw,
		"error":       errMsg,
	})
}

func (s *turnState) onMessageDelta(data []byte) error {
	var ev anthropicMessageDelta
	if err := unmarshalStrict(data, &ev, "message_delta"); err != nil {
		return err
	}
	if ev.Delta.StopReason != "" {
		s.stopReason = ev.Delta.StopReason
	}
	if ev.Delta.StopSequence != nil {
		s.stopSequence = ev.Delta.StopSequence
	}
	if ev.Usage != nil {
		s.usage.CompletionTokens = ev.Usage.OutputTokens
	}
	return nil
}

func (s *turnState) onMessageStop(_ []byte) error {
	s.seenMessageStop = true
	return nil
}

func (s *turnState) onError(data []byte) error {
	var ev anthropicErrorEvent
	if err := unmarshalStrict(data, &ev, "error"); err != nil {
		return err
	}
	s.errorType = ev.Error.Type
	s.errorMessage = ev.Error.Message
	if s.stopReason == "" {
		s.stopReason = "error"
	}
	return nil
}

// finalize assembles the accumulated per-block state into a canonical
// *llm.ChatResponse. It is called once the SSE stream has been fully drained
// (successfully or via client disconnect / EOF).
func (s *turnState) finalize() *llm.ChatResponse {
	content := make([]llm.ContentBlock, 0, len(s.blocks))
	for i := 0; i <= s.maxIndex; i++ {
		b, ok := s.blocks[i]
		if !ok {
			continue
		}
		cb := llm.ContentBlock{Type: b.kind}
		switch b.kind {
		case blockTypeText:
			cb.Text = b.text.String()
		case blockTypeToolUse:
			cb.ToolUseID = b.toolID
			cb.ToolName = b.toolName
			cb.ToolInput = b.toolInput
		case blockTypeThinking:
			cb.Thinking = b.thinking.String()
			cb.ThinkingSignature = b.signature.String()
		case blockTypeServerToolUse:
			cb.ToolUseID = b.toolID
			cb.ToolName = b.toolName
			cb.ToolInput = b.toolInput
		case blockTypeWebSearchToolResult:
			cb.ToolResultID = b.toolUseID
			cb.Content = b.content
		}
		content = append(content, cb)
	}

	usage := s.usage
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens

	extra := map[string]any{
		"id":   s.id,
		"type": "message",
	}
	if s.stopSequence != nil {
		extra["stop_sequence"] = *s.stopSequence
	}
	if len(s.toolInputErrors) > 0 {
		extra["tool_input_parse_errors"] = s.toolInputErrors
	}
	if s.errorType != "" {
		extra["error"] = map[string]any{
			"type":    s.errorType,
			"message": s.errorMessage,
		}
	}
	if !s.seenMessageStop && s.errorType == "" {
		extra["incomplete"] = true
		if s.stopReason == "" {
			s.stopReason = stopReasonIncomplete
		}
	}

	return &llm.ChatResponse{
		Model: s.model,
		Message: llm.Message{
			Role:    defaultString(s.role, "assistant"),
			Content: content,
		},
		Done:       s.seenMessageStop,
		StopReason: s.stopReason,
		Usage:      &usage,
		CreatedAt:  time.Now(),
		Extra:      extra,
	}
}

func defaultString(got, fallback string) string {
	if got == "" {
		return fallback
	}
	return got
}

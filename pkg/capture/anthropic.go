package capture

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sse"
)

// ProviderAnthropic is the canonical provider-name string consumers use to
// key the Anthropic reducer in their dispatch maps.
const ProviderAnthropic = "anthropic"

// Anthropic content-block type strings, used for both the one-shot JSON path
// and the streaming state machine.
const (
	blockTypeText                = "text"
	blockTypeToolUse             = "tool_use"
	blockTypeThinking            = "thinking"
	blockTypeServerToolUse       = "server_tool_use"
	blockTypeWebSearchToolResult = "web_search_tool_result"
)

// anthropicReducer turns a single Anthropic Messages turn — streamed SSE or
// one-shot JSON — into a canonical *llm.ChatResponse.
//
// The reducer's output must be byte-equivalent to
// llm/provider/anthropic.ParseResponse's output on the non-streaming form
// of the same turn so the content-addressed DAG dedups streamed and
// non-streamed captures. canonical_equivalence_test verifies the property
// across the fixture pairs in testdata/anthropic/canonical_equivalence.
//
// Memory note: per-block text / thinking / tool_input buffers grow with the
// upstream's output and are NOT bounded here. Anthropic's current hard
// ceiling is the caller's max_tokens (128K for Opus 4.7, model-dependent
// otherwise); at ~4 chars/token the content ceiling is under 1 MB of text
// in practice, and SSE framing overhead puts wire size under ~5 MB for a
// maxed-out plain-text turn. Pathological tool-use turns with large
// input_json_delta payloads can push higher. Production sidecar pods
// should be sized with headroom for that peak. tapes_extproc_body_bytes
// (histogram) surfaces real-world p99.
type anthropicReducer struct{}

// NewAnthropicReducer returns an Anthropic reducer. The value is stateless
// at the package level; per-turn state lives inside Reduce.
func NewAnthropicReducer() Reducer {
	return &anthropicReducer{}
}

// Reduce implements Reducer.
func (r *anthropicReducer) Reduce(ctx context.Context, _, respBody io.Reader, contentType string) (*llm.ChatResponse, error) {
	if respBody == nil {
		return nil, errors.New("anthropic reducer: nil response body")
	}

	// Classify the body shape. In practice upstream sets
	// text/event-stream for streaming and application/json for one-shot,
	// but accept NDJSON as a streaming shape for symmetry with Ollama.
	lower := strings.ToLower(contentType)
	switch {
	case strings.Contains(lower, "event-stream"), strings.Contains(lower, "ndjson"):
		return r.reduceStream(ctx, respBody)
	default:
		return r.reduceOneShot(respBody)
	}
}

func (r *anthropicReducer) reduceOneShot(body io.Reader) (*llm.ChatResponse, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("anthropic reducer: read oneshot body: %w", err)
	}

	var resp oneshotResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("anthropic reducer: parse oneshot body: %w", err)
	}

	content := make([]llm.ContentBlock, 0, len(resp.Content))
	for _, block := range resp.Content {
		cb := llm.ContentBlock{Type: block.Type}
		switch block.Type {
		case blockTypeText:
			cb.Text = block.Text
		case blockTypeToolUse:
			cb.ToolUseID = block.ID
			cb.ToolName = block.Name
			cb.ToolInput = block.Input
		case blockTypeThinking:
			cb.Thinking = block.Thinking
			cb.ThinkingSignature = block.Signature
		case blockTypeServerToolUse:
			cb.ToolUseID = block.ID
			cb.ToolName = block.Name
			cb.ToolInput = block.Input
		case blockTypeWebSearchToolResult:
			cb.ToolResultID = block.ToolUseID
			cb.Content = block.Content
		}
		content = append(content, cb)
	}

	usage := llm.Usage{}
	if resp.Usage != nil {
		totalInput := resp.Usage.InputTokens +
			resp.Usage.CacheCreationInputTokens +
			resp.Usage.CacheReadInputTokens
		usage.PromptTokens = totalInput
		usage.CompletionTokens = resp.Usage.OutputTokens
		usage.TotalTokens = totalInput + resp.Usage.OutputTokens
		usage.CacheCreationInputTokens = resp.Usage.CacheCreationInputTokens
		usage.CacheReadInputTokens = resp.Usage.CacheReadInputTokens
	}

	role := resp.Role
	if role == "" {
		role = "assistant"
	}

	extra := map[string]any{
		"id":   resp.ID,
		"type": defaultString(resp.Type, "message"),
	}

	return &llm.ChatResponse{
		Model: resp.Model,
		Message: llm.Message{
			Role:    role,
			Content: content,
		},
		Done:        true,
		StopReason:  resp.StopReason,
		Usage:       &usage,
		CreatedAt:   time.Now(),
		RawResponse: raw,
		Extra:       extra,
	}, nil
}

func (r *anthropicReducer) reduceStream(ctx context.Context, body io.Reader) (*llm.ChatResponse, error) {
	state := newTurnState()

	// sse.NewTeeReader's second arg is a tee sink; io.Discard here means
	// "just parse events, don't tee anywhere" — upstream callers that need
	// a client-facing tee already wrap body with io.TeeReader themselves.
	tr := sse.NewTeeReader(body, io.Discard)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ev, err := tr.Next()
		if err != nil {
			// Unexpected reader error. Preserve what we have so the operator
			// still sees the partial turn rather than a hole.
			resp := state.finalize()
			if resp.Extra == nil {
				resp.Extra = map[string]any{}
			}
			resp.Extra["reducer_error"] = err.Error()
			return resp, nil
		}
		if ev == nil {
			break
		}
		if err := dispatchStreamEvent(state, ev); err != nil {
			// Malformed event body: treat like a partial capture, flag the
			// error in Extra, and move on. A single bad frame shouldn't erase
			// the whole turn.
			if state.errorMessage == "" {
				state.errorMessage = err.Error()
				state.errorType = "reducer_parse_error"
			}
			continue
		}
	}

	return state.finalize(), nil
}

func dispatchStreamEvent(state *turnState, ev *sse.Event) error {
	data := []byte(ev.Data)

	// Determine event type. Prefer the "event:" field (sse.Event.Type) but
	// fall back to parsing {"type": ...} from the data payload in case the
	// upstream omits the event line.
	evType := ev.Type
	if evType == "" {
		var probe anthropicStreamEvent
		if err := json.Unmarshal(data, &probe); err == nil {
			evType = probe.Type
		}
	}

	switch evType {
	case "message_start":
		return state.onMessageStart(data)
	case "content_block_start":
		return state.onContentBlockStart(data)
	case "content_block_delta":
		return state.onContentBlockDelta(data)
	case "content_block_stop":
		return state.onContentBlockStop(data)
	case "message_delta":
		return state.onMessageDelta(data)
	case "message_stop":
		return state.onMessageStop(data)
	case "error":
		return state.onError(data)
	case "ping", "":
		// ping keep-alive or unlabelled frames are safe to skip.
		return nil
	default:
		// Unknown event types are ignored; log-worthy but not fatal.
		return nil
	}
}

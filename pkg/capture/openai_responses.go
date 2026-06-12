package capture

import (
	"bufio"
	"bytes"
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

// ProviderOpenAI is the canonical provider-name string consumers use to key
// the OpenAI Responses reducer in their dispatch maps.
const ProviderOpenAI = "openai"

// stopReasonIncomplete is the canonical stop reason for turns that ended
// before natural completion, shared across reducers.
const stopReasonIncomplete = "incomplete"

// openaiResponsesReducer turns a single OpenAI Responses API turn — streamed
// SSE or one-shot JSON — into a canonical *llm.ChatResponse.
//
// The streaming shape is friendlier than Anthropic's: the terminal
// `response.completed` / `response.incomplete` / `response.failed` event
// carries the complete Response object (output items, status, usage), so the
// happy path reduces that single payload with the same mapping as the
// one-shot form instead of replaying every delta. Deltas are accumulated
// only as a fallback so a stream torn down before its terminal event still
// captures the partial text rather than a hole.
type openaiResponsesReducer struct{}

// NewOpenAIResponsesReducer returns an OpenAI Responses reducer. The value
// is stateless at the package level; per-turn state lives inside Reduce.
func NewOpenAIResponsesReducer() Reducer {
	return &openaiResponsesReducer{}
}

// Reduce implements Reducer.
func (r *openaiResponsesReducer) Reduce(ctx context.Context, _, respBody io.Reader, contentType string) (*llm.ChatResponse, error) {
	if respBody == nil {
		return nil, errors.New("openai responses reducer: nil response body")
	}

	lower := strings.ToLower(contentType)
	switch {
	case strings.Contains(lower, "event-stream"), strings.Contains(lower, "ndjson"):
		return r.reduceStream(ctx, respBody)
	case strings.Contains(lower, "json"):
		return r.reduceOneShot(respBody)
	default:
		// chatgpt.com/backend-api/codex omits Content-Type entirely on
		// its SSE responses, so an absent or unknown type sniffs the
		// body: SSE frames open with an "event:" or "data:" field name.
		br := bufio.NewReader(respBody)
		prefix, _ := br.Peek(sseSniffLen)
		if looksLikeSSE(prefix) {
			return r.reduceStream(ctx, br)
		}
		return r.reduceOneShot(br)
	}
}

// sseSniffLen covers leading whitespace plus the longest field name the
// sniffer matches ("event:").
const sseSniffLen = 16

func looksLikeSSE(prefix []byte) bool {
	trimmed := bytes.TrimLeft(prefix, " \t\r\n")
	return bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte("data:"))
}

func (r *openaiResponsesReducer) reduceOneShot(body io.Reader) (*llm.ChatResponse, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("openai responses reducer: read oneshot body: %w", err)
	}

	var resp responsesObject
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("openai responses reducer: parse oneshot body: %w", err)
	}
	if resp.Object != "response" {
		return nil, fmt.Errorf("openai responses reducer: unexpected object %q", resp.Object)
	}

	return responsesObjectToChat(&resp), nil
}

// streamEnvelope is the data payload of one Responses SSE event. Terminal
// events nest the full Response object under `response`; delta events carry
// the delta text directly.
type streamEnvelope struct {
	Type     string           `json:"type"`
	Response *responsesObject `json:"response"`
	Delta    string           `json:"delta"`
	Item     json.RawMessage  `json:"item"`
}

func (r *openaiResponsesReducer) reduceStream(ctx context.Context, body io.Reader) (*llm.ChatResponse, error) {
	// Completed output items accumulate from response.output_item.done
	// frames: api.openai.com echoes the full item list again on the
	// terminal event, but chatgpt.com/backend-api/codex sends the
	// terminal event with an EMPTY output array, so the accumulated
	// items are the only complete record there. Text deltas are a
	// second-line fallback for streams torn down mid-item.
	var (
		items     []json.RawMessage
		deltaText strings.Builder
		created   *responsesObject
	)

	tr := sse.NewTeeReader(body, io.Discard)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		ev, err := tr.Next()
		if err != nil {
			return partialResponse(created, items, deltaText.String(), err.Error()), nil
		}
		if ev == nil {
			break
		}

		var env streamEnvelope
		if jsonErr := json.Unmarshal([]byte(ev.Data), &env); jsonErr != nil {
			continue
		}

		switch env.Type {
		case "response.completed", "response.incomplete", "response.failed":
			if env.Response == nil {
				// A terminal envelope without a response object is a
				// malformed frame, not an early-truncated stream — give
				// it a distinct reason so the two failure shapes stay
				// separable in capture diagnostics.
				reason := fmt.Sprintf("terminal event %s carried no response object", env.Type)
				return partialResponse(created, items, deltaText.String(), reason), nil
			}
			if len(env.Response.Output) == 0 {
				env.Response.Output = items
			}
			return responsesObjectToChat(env.Response), nil
		case "response.created":
			created = env.Response
		case "response.output_item.done":
			if len(env.Item) > 0 {
				items = append(items, env.Item)
			}
		case "response.output_text.delta":
			deltaText.WriteString(env.Delta)
		}
	}

	return partialResponse(created, items, deltaText.String(), "stream ended before terminal response event"), nil
}

// partialResponse preserves whatever a truncated stream yielded, flagged via
// Extra so operators see the partial turn instead of silence. Completed
// output items are the best record; loose text deltas cover a stream cut
// mid-item.
func partialResponse(created *responsesObject, items []json.RawMessage, text, reason string) *llm.ChatResponse {
	content := responsesOutputContent(items)
	if len(content) == 0 && text != "" {
		content = append(content, llm.ContentBlock{Type: "text", Text: text})
	}

	resp := &llm.ChatResponse{
		Message: llm.Message{
			Role:    "assistant",
			Content: content,
		},
		Extra: map[string]any{"partial": true, "reducer_error": reason},
	}
	if created != nil {
		resp.Model = created.Model
		if created.CreatedAt > 0 {
			resp.CreatedAt = time.Unix(created.CreatedAt, 0).UTC()
		}
	}
	return resp
}

// responsesObject is the subset of the Responses API Response object tapes
// maps onto llm.ChatResponse.
type responsesObject struct {
	ID                string            `json:"id"`
	Object            string            `json:"object"`
	CreatedAt         int64             `json:"created_at"`
	Status            string            `json:"status"`
	Model             string            `json:"model"`
	Output            []json.RawMessage `json:"output"`
	Usage             *responsesUsage   `json:"usage"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
}

type responsesUsage struct {
	InputTokens        int `json:"input_tokens"`
	OutputTokens       int `json:"output_tokens"`
	TotalTokens        int `json:"total_tokens"`
	InputTokensDetails *struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"input_tokens_details"`
}

// responsesOutputItem is the union of output item shapes (message,
// function_call, reasoning, hosted-tool calls).
type responsesOutputItem struct {
	Type    string `json:"type"`
	Role    string `json:"role"`
	Content []struct {
		Type    string `json:"type"`
		Text    string `json:"text"`
		Refusal string `json:"refusal"`
	} `json:"content"`

	// function_call
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`

	// reasoning
	Summary []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"summary"`
	EncryptedContent string `json:"encrypted_content"`
}

// responsesOutputContent maps Responses output items to canonical content
// blocks. Shared by full-object reduction and partial captures so a stream
// torn down before its terminal event maps items identically.
func responsesOutputContent(output []json.RawMessage) []llm.ContentBlock {
	content := make([]llm.ContentBlock, 0, len(output))
	for _, raw := range output {
		var item responsesOutputItem
		if err := json.Unmarshal(raw, &item); err != nil {
			content = append(content, llm.ContentBlock{Type: "unparsed", Content: raw})
			continue
		}

		switch item.Type {
		case "message":
			for _, part := range item.Content {
				switch part.Type {
				case "output_text":
					content = append(content, llm.ContentBlock{Type: "text", Text: part.Text})
				case "refusal":
					content = append(content, llm.ContentBlock{Type: "refusal", Text: part.Refusal})
				default:
					content = append(content, llm.ContentBlock{Type: part.Type, Text: part.Text})
				}
			}
		case "function_call":
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
			content = append(content, cb)
		case "reasoning":
			var b strings.Builder
			for _, part := range item.Summary {
				if b.Len() > 0 && part.Text != "" {
					b.WriteString("\n")
				}
				b.WriteString(part.Text)
			}
			content = append(content, llm.ContentBlock{
				Type:              "thinking",
				Thinking:          b.String(),
				ThinkingSignature: item.EncryptedContent,
			})
		default:
			// Hosted tool calls (web_search_call, code_interpreter_call,
			// ...) and future item types survive verbatim.
			content = append(content, llm.ContentBlock{Type: item.Type, Content: raw})
		}
	}
	return content
}

// responsesObjectToChat maps a full Response object to the canonical chat
// response shape shared with the Chat Completions path.
func responsesObjectToChat(resp *responsesObject) *llm.ChatResponse {
	content := responsesOutputContent(resp.Output)

	out := &llm.ChatResponse{
		Model: resp.Model,
		Message: llm.Message{
			Role:    "assistant",
			Content: content,
		},
		Done:       true,
		StopReason: responsesStopReason(resp),
		Extra: map[string]any{
			"id":     resp.ID,
			"object": resp.Object,
			"status": resp.Status,
		},
	}
	if resp.CreatedAt > 0 {
		out.CreatedAt = time.Unix(resp.CreatedAt, 0).UTC()
	}
	if resp.Usage != nil {
		usage := &llm.Usage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.TotalTokens,
		}
		if resp.Usage.InputTokensDetails != nil {
			usage.CacheReadInputTokens = resp.Usage.InputTokensDetails.CachedTokens
		}
		out.Usage = usage
	}
	return out
}

// responsesStopReason maps Response status to the canonical stop-reason
// vocabulary: completed turns say "stop" (Chat Completions parity);
// incomplete turns surface the API's own reason (e.g. "max_output_tokens");
// anything else carries the status verbatim.
func responsesStopReason(resp *responsesObject) string {
	switch resp.Status {
	case "completed":
		return "stop"
	case stopReasonIncomplete:
		if resp.IncompleteDetails != nil && resp.IncompleteDetails.Reason != "" {
			return resp.IncompleteDetails.Reason
		}
		return stopReasonIncomplete
	default:
		return resp.Status
	}
}

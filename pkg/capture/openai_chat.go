package capture

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sse"
)

type openAIChatReducer struct{}

// NewOpenAIChatCompletionsReducer reduces Chat Completions JSON and SSE turns.
// Choice zero is the canonical message; other choices remain in Extra rather
// than being concatenated into an answer the provider never generated.
func NewOpenAIChatCompletionsReducer() Reducer { return &openAIChatReducer{} }

type chatFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatTool struct {
	Index    int             `json:"index"`
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Function chatFunction    `json:"function"`
	Custom   json.RawMessage `json:"custom,omitempty"`
}

type chatMessage struct {
	Role         string          `json:"role"`
	Content      string          `json:"content"`
	Refusal      string          `json:"refusal"`
	ToolCalls    []chatTool      `json:"tool_calls"`
	FunctionCall *chatFunction   `json:"function_call,omitempty"`
	Audio        json.RawMessage `json:"audio,omitempty"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	Delta        chatMessage `json:"delta"`
	FinishReason string      `json:"finish_reason"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
	PromptDetails    struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionDetails json.RawMessage `json:"completion_tokens_details,omitempty"`
}

type chatEnvelope struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []chatChoice    `json:"choices"`
	Usage   *chatUsage      `json:"usage"`
	Error   json.RawMessage `json:"error"`
}

func (r *openAIChatReducer) Reduce(ctx context.Context, _ io.Reader, body io.Reader, contentType string) (*llm.ChatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if body == nil {
		return nil, errors.New("openai chat reducer: nil response body")
	}
	br := bufio.NewReader(body)
	prefix, _ := br.Peek(sseSniffLen)
	if strings.Contains(strings.ToLower(contentType), "event-stream") || looksLikeSSE(prefix) {
		return r.stream(ctx, br)
	}
	raw, err := io.ReadAll(br)
	if err != nil {
		return nil, fmt.Errorf("openai chat reducer: read response: %w", err)
	}
	var env chatEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("openai chat reducer: parse response: %w", err)
	}
	if env.Object != "chat.completion" || len(env.Choices) == 0 {
		return nil, errors.New("openai chat reducer: expected chat.completion with choices")
	}
	return chatResponse(env, ""), nil
}

func (r *openAIChatReducer) stream(ctx context.Context, body io.Reader) (*llm.ChatResponse, error) {
	tr := sse.NewTeeReader(body, io.Discard)
	env := chatEnvelope{Object: "chat.completion"}
	choices := map[int]*chatChoice{}
	tools := map[int]map[int]*chatTool{}
	problem := ""
	done := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ev, err := tr.Next()
		if err != nil {
			problem = "stream read failed"
			break
		}
		if ev == nil {
			break
		}
		if strings.TrimSpace(ev.Data) == "[DONE]" {
			done = true
			break
		}
		if ev.Data == "" {
			continue
		}
		var chunk chatEnvelope
		if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
			problem = "malformed stream event"
			continue
		}
		if len(chunk.Error) > 0 && string(chunk.Error) != "null" {
			problem = "upstream stream error"
			break
		}
		if chunk.Object != "chat.completion.chunk" {
			problem = "unexpected stream object"
			continue
		}
		if chunk.ID != "" {
			env.ID = chunk.ID
		}
		if chunk.Model != "" {
			env.Model = chunk.Model
		}
		if chunk.Created != 0 {
			env.Created = chunk.Created
		}
		if chunk.Usage != nil {
			env.Usage = chunk.Usage
		}
		for _, delta := range chunk.Choices {
			if delta.Index < 0 {
				problem = "negative choice index"
				continue
			}
			c := choices[delta.Index]
			if c == nil {
				c = &chatChoice{Index: delta.Index}
				choices[delta.Index] = c
				tools[delta.Index] = map[int]*chatTool{}
			}
			if delta.Delta.Role != "" {
				c.Message.Role = delta.Delta.Role
			}
			c.Message.Content += delta.Delta.Content
			c.Message.Refusal += delta.Delta.Refusal
			if delta.Delta.FunctionCall != nil {
				if c.Message.FunctionCall == nil {
					c.Message.FunctionCall = &chatFunction{}
				}
				c.Message.FunctionCall.Name += delta.Delta.FunctionCall.Name
				c.Message.FunctionCall.Arguments += delta.Delta.FunctionCall.Arguments
			}
			for _, part := range delta.Delta.ToolCalls {
				if part.Index < 0 {
					problem = "negative tool index"
					continue
				}
				t := tools[delta.Index][part.Index]
				if t == nil {
					t = &chatTool{Index: part.Index}
					tools[delta.Index][part.Index] = t
				}
				if part.ID != "" {
					t.ID = part.ID
				}
				if part.Type != "" {
					t.Type = part.Type
				}
				t.Function.Name += part.Function.Name
				t.Function.Arguments += part.Function.Arguments
				if len(part.Custom) > 0 && !bytes.Equal(bytes.TrimSpace(part.Custom), []byte("null")) {
					problem = "custom tool deltas are not supported"
					t.Custom = part.Custom
				}
			}
			if len(delta.Delta.Audio) > 0 && !bytes.Equal(bytes.TrimSpace(delta.Delta.Audio), []byte("null")) {
				problem = "audio deltas are not supported"
				c.Message.Audio = delta.Delta.Audio
			}
			if delta.FinishReason != "" {
				c.FinishReason = delta.FinishReason
			}
		}
	}
	for index, choice := range choices {
		indices := make([]int, 0, len(tools[index]))
		for i := range tools[index] {
			indices = append(indices, i)
		}
		sort.Ints(indices)
		for _, i := range indices {
			choice.Message.ToolCalls = append(choice.Message.ToolCalls, *tools[index][i])
		}
		env.Choices = append(env.Choices, *choice)
	}
	if !done && problem == "" {
		problem = "stream ended before [DONE]"
	}
	if len(env.Choices) == 0 && problem == "" {
		problem = "stream contained no choices"
	}
	return chatResponse(env, problem), nil
}

func chatResponse(env chatEnvelope, problem string) *llm.ChatResponse {
	sort.Slice(env.Choices, func(i, j int) bool { return env.Choices[i].Index < env.Choices[j].Index })
	resp := &llm.ChatResponse{
		Model: env.Model, Message: llm.Message{Role: "assistant"},
		Extra: map[string]any{"id": env.ID, "object": env.Object},
	}
	if env.Created > 0 {
		resp.CreatedAt = time.Unix(env.Created, 0).UTC()
	}
	if env.Usage != nil {
		resp.Usage = &llm.Usage{
			PromptTokens: env.Usage.PromptTokens, CompletionTokens: env.Usage.CompletionTokens,
			TotalTokens: env.Usage.TotalTokens, CacheReadInputTokens: env.Usage.PromptDetails.CachedTokens,
		}
		if len(env.Usage.CompletionDetails) > 0 {
			resp.Extra["completion_tokens_details"] = env.Usage.CompletionDetails
		}
	}
	if len(env.Choices) > 0 && env.Choices[0].Index != 0 {
		resp.Extra["choices"] = env.Choices
		if problem == "" {
			problem = "missing choice zero"
		}
	} else if len(env.Choices) > 0 {
		first := env.Choices[0]
		resp.StopReason = first.FinishReason
		if first.Message.Role != "" {
			resp.Message.Role = first.Message.Role
		}
		if first.Message.Content != "" {
			resp.Message.Content = append(resp.Message.Content, llm.ContentBlock{Type: "text", Text: first.Message.Content})
		}
		if first.Message.Refusal != "" {
			resp.Message.Content = append(resp.Message.Content, llm.ContentBlock{Type: "refusal", Text: first.Message.Refusal})
		}
		for _, tool := range first.Message.ToolCalls {
			if tool.Type != "" && tool.Type != "function" {
				raw, err := json.Marshal(tool)
				if err != nil {
					problem = "unencodable tool call"
					continue
				}
				resp.Message.Content = append(resp.Message.Content, llm.ContentBlock{Type: "unparsed", Content: raw})
				continue
			}
			resp.Message.Content = append(resp.Message.Content, chatToolBlock(tool.ID, tool.Function))
		}
		if first.Message.FunctionCall != nil {
			resp.Message.Content = append(resp.Message.Content, chatToolBlock("", *first.Message.FunctionCall))
		}
		if len(first.Message.Audio) > 0 && string(first.Message.Audio) != "null" {
			resp.Message.Content = append(resp.Message.Content, llm.ContentBlock{Type: "audio", Content: first.Message.Audio})
		}
		if len(env.Choices) > 1 {
			resp.Extra["choices"] = env.Choices
		}
		for _, choice := range env.Choices {
			if choice.FinishReason == "" && problem == "" {
				problem = "choice ended without finish_reason"
			}
		}
	}
	resp.Done = problem == ""
	if problem != "" {
		resp.Extra["partial"] = true
		resp.Extra["reducer_error"] = problem
	}
	return resp
}

func chatToolBlock(id string, fn chatFunction) llm.ContentBlock {
	b := llm.ContentBlock{Type: "tool_use", ToolUseID: id, ToolName: fn.Name}
	decoder := json.NewDecoder(strings.NewReader(fn.Arguments))
	decoder.UseNumber()
	err := decoder.Decode(&b.ToolInput)
	var trailing any
	if err != nil || b.ToolInput == nil || decoder.Decode(&trailing) != io.EOF {
		b.ToolInput = nil
		// Preserve invalid/partial arguments instead of dropping the call or
		// presenting an invented empty argument object.
		raw, err := json.Marshal(map[string]string{"arguments": fn.Arguments})
		if err == nil {
			b.Content = raw
		} else {
			b.Text = fn.Arguments
		}
	}
	return b
}

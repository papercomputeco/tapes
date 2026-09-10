package capture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/papercomputeco/tapes/pkg/llm"
)

type openAIReducer struct{}

// NewOpenAIReducer dispatches OpenAI wire formats using the captured request.
// Chat Completions carries messages; Responses carries input. Missing request
// context retains the historical Responses reduction for old stored captures.
func NewOpenAIReducer() Reducer { return &openAIReducer{} }

func (r *openAIReducer) Reduce(ctx context.Context, request, response io.Reader, contentType string) (*llm.ChatResponse, error) {
	var raw []byte
	if request != nil {
		var err error
		raw, err = io.ReadAll(request)
		if err != nil {
			return nil, fmt.Errorf("openai reducer: read request: %w", err)
		}
	}
	var fields map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("openai reducer: parse request: %w", err)
		}
	}
	messages, chat := fields["messages"]
	chat = chat && !bytes.Equal(bytes.TrimSpace(messages), []byte("null"))
	input, responses := fields["input"]
	responses = responses && !bytes.Equal(bytes.TrimSpace(input), []byte("null"))
	if chat && responses {
		return nil, errors.New("openai reducer: ambiguous request wire format")
	}
	reducer := NewOpenAIResponsesReducer()
	if chat {
		reducer = NewOpenAIChatCompletionsReducer()
	}
	return reducer.Reduce(ctx, bytes.NewReader(raw), response, contentType)
}

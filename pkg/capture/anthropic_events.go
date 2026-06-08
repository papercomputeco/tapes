package capture

import "encoding/json"

// Wire-shape types for Anthropic Messages streaming events. These are kept in
// a separate file from the state machine so the shapes are easy to compare
// against docs.anthropic.com/en/api/messages-streaming.

// anthropicStreamEvent is the discriminator parsed first to decide which
// concrete event shape to unmarshal next.
type anthropicStreamEvent struct {
	Type string `json:"type"`
}

// message_start carries the initial message with preliminary usage and model.
type anthropicMessageStart struct {
	Type    string                `json:"type"`
	Message anthropicStreamingMsg `json:"message"`
}

type anthropicStreamingMsg struct {
	ID           string           `json:"id"`
	Type         string           `json:"type"`
	Role         string           `json:"role"`
	Model        string           `json:"model"`
	StopReason   string           `json:"stop_reason"`
	StopSequence *string          `json:"stop_sequence,omitempty"`
	Usage        *anthropicStrUse `json:"usage,omitempty"`
}

type anthropicStrUse struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}

// content_block_start announces a new block at a given index.
type anthropicContentBlockStart struct {
	Type         string                `json:"type"`
	Index        int                   `json:"index"`
	ContentBlock anthropicContentBlock `json:"content_block"`
}

type anthropicContentBlock struct {
	Type      string         `json:"type"`
	Text      string         `json:"text,omitempty"`
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name,omitempty"`
	Input     map[string]any `json:"input,omitempty"`
	Thinking  string         `json:"thinking,omitempty"`
	Signature string         `json:"signature,omitempty"`
	// ToolUseID links a web_search_tool_result back to its server_tool_use.
	ToolUseID string `json:"tool_use_id,omitempty"`
	// Content is the inline result payload on a web_search_tool_result block
	// (a JSON array of result objects). server_tool_use reuses ID/Name/Input.
	Content json.RawMessage `json:"content,omitempty"`
}

// content_block_delta carries incremental updates to an existing block.
type anthropicContentBlockDelta struct {
	Type  string                `json:"type"`
	Index int                   `json:"index"`
	Delta anthropicStreamingDel `json:"delta"`
}

type anthropicStreamingDel struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	PartialJSON string `json:"partial_json,omitempty"`
	Thinking    string `json:"thinking,omitempty"`
	Signature   string `json:"signature,omitempty"`
}

// content_block_stop marks the end of a block.
type anthropicContentBlockStop struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

// message_delta updates top-level usage / stop_reason / stop_sequence.
type anthropicMessageDelta struct {
	Type  string                  `json:"type"`
	Delta anthropicMessageDeltaIn `json:"delta"`
	Usage *anthropicStrUse        `json:"usage,omitempty"`
}

type anthropicMessageDeltaIn struct {
	StopReason   string  `json:"stop_reason"`
	StopSequence *string `json:"stop_sequence,omitempty"`
}

// error events carry a typed error payload and terminate the stream.
type anthropicErrorEvent struct {
	Type  string          `json:"type"`
	Error anthropicErrDet `json:"error"`
}

type anthropicErrDet struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// oneshotResponse is the shape of a non-streaming POST /v1/messages body.
// It mirrors anthropic/types.go but lives here so pkg/capture can avoid a
// dependency on pkg/llm/provider/anthropic.
type oneshotResponse struct {
	ID           string                  `json:"id"`
	Type         string                  `json:"type"`
	Role         string                  `json:"role"`
	Content      []anthropicContentBlock `json:"content"`
	Model        string                  `json:"model"`
	StopReason   string                  `json:"stop_reason"`
	StopSequence *string                 `json:"stop_sequence,omitempty"`
	Usage        *anthropicStrUse        `json:"usage,omitempty"`
}

// unmarshalStrict wraps json.Unmarshal with a clear error message naming the
// event type so state-machine failures point the operator at the right event.
func unmarshalStrict(data []byte, target any, eventType string) error {
	if err := json.Unmarshal(data, target); err != nil {
		return &anthropicParseError{EventType: eventType, Err: err}
	}
	return nil
}

type anthropicParseError struct {
	EventType string
	Err       error
}

func (e *anthropicParseError) Error() string {
	return "anthropic reducer: parse " + e.EventType + ": " + e.Err.Error()
}

func (e *anthropicParseError) Unwrap() error { return e.Err }

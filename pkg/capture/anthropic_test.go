package capture_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
)

func readFixture(path string) []byte {
	b, err := os.ReadFile(filepath.Join("testdata", "anthropic", path))
	Expect(err).NotTo(HaveOccurred(), "missing fixture: %s", path)
	return b
}

var _ = Describe("Anthropic Reducer", func() {
	ctx := context.Background()
	r := capture.NewAnthropicReducer()

	Describe("one-shot JSON", func() {
		It("reduces messages_oneshot.json to a canonical ChatResponse", func() {
			raw := readFixture("messages_oneshot.json")
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "application/json")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Model).To(Equal("claude-3-5-sonnet-20241022"))
			Expect(resp.Message.Role).To(Equal("assistant"))
			Expect(resp.Message.Content).To(HaveLen(1))
			Expect(resp.Message.Content[0].Type).To(Equal("text"))
			Expect(resp.Message.Content[0].Text).To(Equal("Hello! How can I help you today?"))
			Expect(resp.StopReason).To(Equal("end_turn"))
			Expect(resp.Done).To(BeTrue())
			Expect(resp.Usage).NotTo(BeNil())
			Expect(resp.Usage.PromptTokens).To(Equal(12))
			Expect(resp.Usage.CompletionTokens).To(Equal(10))
			Expect(resp.Usage.TotalTokens).To(Equal(22))
		})
	})

	Describe("streaming SSE", func() {
		It("reduces a multi-chunk text stream", func() {
			raw := readFixture("messages_stream.sse")
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Model).To(Equal("claude-3-5-sonnet-20241022"))
			Expect(resp.Message.Role).To(Equal("assistant"))
			Expect(resp.Message.Content).To(HaveLen(1))
			Expect(resp.Message.Content[0].Type).To(Equal("text"))
			Expect(resp.Message.Content[0].Text).To(Equal("Hello! How can I help you today?"))
			Expect(resp.StopReason).To(Equal("end_turn"))
			Expect(resp.Done).To(BeTrue())
			Expect(resp.Usage.PromptTokens).To(Equal(12))
			Expect(resp.Usage.CompletionTokens).To(Equal(10))
		})

		It("assembles input_json_delta fragments into a parsed tool input", func() {
			raw := readFixture("messages_tool_use.sse")
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.Content).To(HaveLen(2))
			Expect(resp.Message.Content[0].Type).To(Equal("text"))
			Expect(resp.Message.Content[0].Text).To(Equal("I'll check the weather for you."))
			Expect(resp.Message.Content[1].Type).To(Equal("tool_use"))
			Expect(resp.Message.Content[1].ToolName).To(Equal("get_weather"))
			Expect(resp.Message.Content[1].ToolUseID).To(Equal("toolu_FIXTURE0000000000000"))
			Expect(resp.Message.Content[1].ToolInput).To(HaveKeyWithValue("location", "San Francisco, CA"))
			Expect(resp.Message.Content[1].ToolInput).To(HaveKeyWithValue("unit", "celsius"))
			Expect(resp.StopReason).To(Equal("tool_use"))
		})

		It("accumulates thinking_delta and signature_delta into a thinking block", func() {
			raw := readFixture("messages_thinking.sse")
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.Content).To(HaveLen(2))
			Expect(resp.Message.Content[0].Type).To(Equal("thinking"))
			Expect(resp.Message.Content[0].Thinking).To(Equal("Let me add 2 and 2. Two plus two equals four."))
			Expect(resp.Message.Content[0].ThinkingSignature).To(Equal("EqoBCkgIARgCIkBYp3d2"))
			Expect(resp.Message.Content[1].Type).To(Equal("text"))
			Expect(resp.Message.Content[1].Text).To(Equal("2 + 2 = 4."))
		})

		It("preserves partial capture when error event arrives mid-stream", func() {
			raw := readFixture("messages_error_mid_stream.sse")
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.Content).To(HaveLen(1))
			Expect(resp.Message.Content[0].Text).To(Equal("Starting to answer"))
			Expect(resp.StopReason).To(Equal("error"))
			Expect(resp.Done).To(BeFalse())
			Expect(resp.Extra).To(HaveKey("error"))
		})

		It("preserves partial capture when the stream ends before message_stop", func() {
			raw := readFixture("messages_truncated_stream.sse")
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.Content).To(HaveLen(1))
			Expect(resp.Message.Content[0].Text).To(Equal("Partial respon"))
			Expect(resp.Done).To(BeFalse())
			Expect(resp.StopReason).To(Equal("incomplete"))
			Expect(resp.Extra).To(HaveKeyWithValue("incomplete", true))
		})

		It("leaves ToolInput nil when input_json_delta fragments don't parse, surfacing the error via Extra", func() {
			// Synthesize a stream with malformed tool_use input_json_delta.
			malformed := `event: message_start
data: {"type":"message_start","message":{"id":"msg_x","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet-20241022","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_X","name":"get_weather","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{not valid json"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":5}}

event: message_stop
data: {"type":"message_stop"}

`
			resp, err := r.Reduce(ctx, nil, bytes.NewReader([]byte(malformed)), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.Content).To(HaveLen(1))
			Expect(resp.Message.Content[0].Type).To(Equal("tool_use"))
			Expect(resp.Message.Content[0].ToolInput).To(BeNil(),
				"ToolInput must stay nil on parse failure so downstream consumers don't mistake malformed payloads for valid calls")
			Expect(resp.Extra).To(HaveKey("tool_input_parse_errors"))
		})

		It("ignores ping keep-alive events", func() {
			// messages_stream.sse includes a ping; that test already asserts
			// non-empty content without the ping leaking anywhere.
			raw := readFixture("messages_stream.sse")
			Expect(string(raw)).To(ContainSubstring("event: ping"))
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(raw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.Content[0].Text).NotTo(ContainSubstring("ping"))
		})
	})
})

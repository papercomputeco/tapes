package capture_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
)

// The oneshot.json and stream.sse fixtures are real api.openai.com Responses
// wire captures recorded through the papermeetcodex clearing (gpt-4o-mini),
// not hand-written approximations.
var _ = Describe("OpenAI Responses reducer", func() {
	var r capture.Reducer
	ctx := context.Background()

	BeforeEach(func() {
		r = capture.NewOpenAIResponsesReducer()
	})

	readFixture := func(name string) []byte {
		data, err := os.ReadFile(filepath.Join("testdata", "openai_responses", name))
		Expect(err).NotTo(HaveOccurred())
		return data
	}

	Describe("one-shot JSON", func() {
		It("reduces a completed response", func() {
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(readFixture("oneshot.json")), "application/json")
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.Model).To(Equal("gpt-4o-mini-2024-07-18"))
			Expect(resp.Done).To(BeTrue())
			Expect(resp.StopReason).To(Equal("stop"))
			Expect(resp.Message.Role).To(Equal("assistant"))
			Expect(resp.Message.GetText()).To(Equal("paper-codex-ok"))
			Expect(resp.Usage).NotTo(BeNil())
			Expect(resp.Usage.PromptTokens).To(Equal(16))
			Expect(resp.Usage.CompletionTokens).To(Equal(6))
			Expect(resp.Usage.TotalTokens).To(Equal(22))
			Expect(resp.Extra).To(HaveKeyWithValue("status", "completed"))
			Expect(resp.CreatedAt.IsZero()).To(BeFalse())
		})

		It("rejects a non-response object", func() {
			_, err := r.Reduce(ctx, nil, strings.NewReader(`{"object":"chat.completion"}`), "application/json")
			Expect(err).To(HaveOccurred())
		})

		It("maps function_call output items to tool_use blocks", func() {
			body := `{
				"object": "response", "id": "resp_x", "status": "completed",
				"model": "gpt-5.5", "created_at": 1781218506,
				"output": [
					{"type": "reasoning", "summary": [{"type": "summary_text", "text": "think"}], "encrypted_content": "blob"},
					{"type": "function_call", "call_id": "call_1", "name": "shell", "arguments": "{\"command\":[\"ls\"]}"}
				]
			}`
			resp, err := r.Reduce(ctx, nil, strings.NewReader(body), "application/json")
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.Message.Content).To(HaveLen(2))
			Expect(resp.Message.Content[0].Type).To(Equal("thinking"))
			Expect(resp.Message.Content[0].Thinking).To(Equal("think"))
			Expect(resp.Message.Content[0].ThinkingSignature).To(Equal("blob"))
			Expect(resp.Message.Content[1].Type).To(Equal("tool_use"))
			Expect(resp.Message.Content[1].ToolUseID).To(Equal("call_1"))
			Expect(resp.Message.Content[1].ToolName).To(Equal("shell"))
			Expect(resp.Message.Content[1].ToolInput).To(HaveKey("command"))
		})

		It("surfaces incomplete_details as the stop reason", func() {
			body := `{
				"object": "response", "id": "resp_x", "status": "incomplete",
				"model": "gpt-5.5",
				"incomplete_details": {"reason": "max_output_tokens"},
				"output": []
			}`
			resp, err := r.Reduce(ctx, nil, strings.NewReader(body), "application/json")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StopReason).To(Equal("max_output_tokens"))
		})
	})

	Describe("streaming SSE", func() {
		It("reduces the terminal response.completed event", func() {
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(readFixture("stream.sse")), "text/event-stream; charset=utf-8")
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.Model).To(Equal("gpt-4o-mini-2024-07-18"))
			Expect(resp.Done).To(BeTrue())
			Expect(resp.StopReason).To(Equal("stop"))
			Expect(resp.Message.GetText()).To(Equal("paper-codex-sse-ok"))
			Expect(resp.Usage).NotTo(BeNil())
			Expect(resp.Usage.PromptTokens).To(Equal(18))
			Expect(resp.Usage.CompletionTokens).To(Equal(8))
			Expect(resp.Usage.TotalTokens).To(Equal(26))
		})

		It("preserves accumulated deltas when the stream is truncated", func() {
			full := readFixture("stream.sse")
			cut := bytes.Index(full, []byte("event: response.completed"))
			Expect(cut).To(BeNumerically(">", 0))

			resp, err := r.Reduce(ctx, nil, bytes.NewReader(full[:cut]), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.Done).To(BeFalse())
			Expect(resp.Message.GetText()).To(Equal("paper-codex-sse-ok"))
			Expect(resp.Extra).To(HaveKeyWithValue("partial", true))
			// Model comes from the response.created event even without
			// a terminal frame.
			Expect(resp.Model).To(Equal("gpt-4o-mini-2024-07-18"))
		})

		It("accumulates output items when the terminal event has empty output", func() {
			// Real chatgpt.com/backend-api/codex wire capture: that
			// backend sends response.completed with "output": [] and
			// only emits items via response.output_item.done.
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(readFixture("chatgpt_stream.sse")), "")
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.Done).To(BeTrue())
			Expect(resp.StopReason).To(Equal("stop"))
			Expect(resp.Model).To(Equal("gpt-5.5"))
			Expect(resp.Message.GetText()).To(Equal("chatgpt-probe-ok"))
			Expect(resp.Usage).NotTo(BeNil())
			Expect(resp.Usage.PromptTokens).To(Equal(26))
			Expect(resp.Usage.CompletionTokens).To(Equal(21))
		})

		It("sniffs SSE when the upstream omits Content-Type", func() {
			// chatgpt.com/backend-api/codex sends no Content-Type header
			// at all on its event stream.
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(readFixture("stream.sse")), "")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Done).To(BeTrue())
			Expect(resp.Message.GetText()).To(Equal("paper-codex-sse-ok"))
		})

		It("sniffs one-shot JSON when the upstream omits Content-Type", func() {
			resp, err := r.Reduce(ctx, nil, bytes.NewReader(readFixture("oneshot.json")), "")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Message.GetText()).To(Equal("paper-codex-ok"))
		})

		It("flags a terminal event without a response object distinctly", func() {
			body := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":null}\n\n"
			resp, err := r.Reduce(ctx, nil, strings.NewReader(body), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Extra).To(HaveKeyWithValue("partial", true))
			Expect(resp.Extra["reducer_error"]).To(ContainSubstring("carried no response object"))
		})

		It("flags an empty stream as partial instead of erroring", func() {
			resp, err := r.Reduce(ctx, nil, strings.NewReader(""), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Extra).To(HaveKeyWithValue("partial", true))
			Expect(resp.Message.Content).To(BeEmpty())
		})
	})
})

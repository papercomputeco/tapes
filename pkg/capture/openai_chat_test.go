package capture_test

import (
	"context"
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
)

const chatJSON = `{"id":"chatcmpl_test","object":"chat.completion","created":1700000000,"model":"poc-cheap","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":4,"total_tokens":16,"prompt_tokens_details":{"cached_tokens":3}}}`

func chatEvent(choices string) string {
	return `data: {"id":"chatcmpl_test","object":"chat.completion.chunk","created":1700000000,"model":"poc-cheap","choices":` + choices + "}\n\n"
}

var _ = Describe("Chat Completions reducer", func() {
	It("reduces JSON text, cached usage, model and provider timestamp", func() {
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(chatJSON), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Model).To(Equal("poc-cheap"))
		Expect(resp.Message.Content[0].Text).To(Equal("hello"))
		Expect(resp.Done).To(BeTrue())
		Expect(resp.Usage.TotalTokens).To(Equal(16))
		Expect(resp.Usage.CacheReadInputTokens).To(Equal(3))
		Expect(resp.CreatedAt.Unix()).To(Equal(int64(1700000000)))
	})

	It("accumulates UTF-8 text, keeps usage after finish, and accepts CRLF", func() {
		body := chatEvent(`[{"index":0,"delta":{"role":"assistant","content":"hé"}}]`) +
			chatEvent(`[{"index":0,"delta":{"content":"llo"},"finish_reason":"length"}]`) +
			"data: {\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":4,\"total_tokens\":16}}\n\ndata: [DONE]\n\n"
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(strings.ReplaceAll(body, "\n", "\r\n")), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content[0].Text).To(Equal("héllo"))
		Expect(resp.StopReason).To(Equal("length"))
		Expect(resp.Done).To(BeTrue())
		Expect(resp.Usage.TotalTokens).To(Equal(16))
	})

	It("assembles interleaved tool calls by index without mixing arguments", func() {
		body := chatEvent(`[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"b","type":"function","function":{"name":"second","arguments":"{\"y\":"}},{"index":0,"id":"a","type":"function","function":{"name":"first","arguments":"{\"x\":"}}]}}]`) +
			chatEvent(`[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"function":{"arguments":"2}"}}]},"finish_reason":"tool_calls"}]`) + "data: [DONE]\n\n"
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Done).To(BeTrue())
		Expect(resp.Message.Content).To(HaveLen(2))
		Expect(resp.Message.Content[0].ToolUseID).To(Equal("a"))
		Expect(resp.Message.Content[0].ToolInput).To(HaveKeyWithValue("x", json.Number("1")))
		Expect(resp.Message.Content[1].ToolUseID).To(Equal("b"))
		Expect(resp.Message.Content[1].ToolInput).To(HaveKeyWithValue("y", json.Number("2")))
	})

	DescribeTable("retains partial streams without claiming completion", func(tail string) {
		body := chatEvent(`[{"index":0,"delta":{"content":"partial"}}]`) + tail
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content[0].Text).To(Equal("partial"))
		Expect(resp.Done).To(BeFalse())
		Expect(resp.Extra).To(HaveKeyWithValue("partial", true))
	},
		Entry("EOF", ""),
		Entry("premature DONE", "data: [DONE]\n\n"),
		Entry("provider error", "data: {\"error\":{\"message\":\"failed\"}}\n\n"),
		Entry("malformed frame then finish", "data: broken\n\n"+chatEvent(`[{"index":0,"delta":{},"finish_reason":"stop"}]`)+"data: [DONE]\n\n"),
	)

	It("preserves a refusal and invalid tool arguments", func() {
		body := `{"object":"chat.completion","choices":[{"index":0,"message":{"refusal":"no","tool_calls":[{"id":"call","type":"function","function":{"name":"echo","arguments":"{"}}]},"finish_reason":"tool_calls"}]}`
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content[0].Type).To(Equal("refusal"))
		Expect(resp.Message.Content[1].ToolName).To(Equal("echo"))
		Expect(resp.Message.Content[1].Content).To(MatchJSON(`{"arguments":"{"}`))
	})

	It("keeps alternatives separate from choice zero", func() {
		body := `{"object":"chat.completion","choices":[{"index":1,"message":{"content":"other"},"finish_reason":"stop"},{"index":0,"message":{"content":"primary"},"finish_reason":"stop"}]}`
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content[0].Text).To(Equal("primary"))
		Expect(resp.Extra).To(HaveKey("choices"))
	})

	It("retains a nonzero-only choice in metadata, not as the canonical answer", func() {
		body := chatEvent(`[{"index":1,"delta":{"content":"alternative"},"finish_reason":"stop"}]`) + "data: [DONE]\n\n"
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content).To(BeEmpty())
		Expect(resp.Done).To(BeFalse())
		encoded, err := json.Marshal(resp.Extra["choices"])
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).To(ContainSubstring(`"index":1`))
		Expect(string(encoded)).To(ContainSubstring("alternative"))
	})

	It("ignores null optional audio and custom-tool deltas", func() {
		body := chatEvent(`[{"index":0,"delta":{"audio":null,"tool_calls":[{"index":0,"id":"call","type":"function","custom":null,"function":{"name":"echo","arguments":"{}"}}]},"finish_reason":"tool_calls"}]`) + "data: [DONE]\n\n"
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "text/event-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Done).To(BeTrue())
	})

	DescribeTable("preserves tool argument numbers and rejects incomplete objects", func(arguments string, valid bool) {
		quoted, err := json.Marshal(arguments)
		Expect(err).NotTo(HaveOccurred())
		body := `{"object":"chat.completion","choices":[{"index":0,"message":{"tool_calls":[{"type":"function","function":{"name":"echo","arguments":` + string(quoted) + `}}]},"finish_reason":"tool_calls"}]}`
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "application/json")
		Expect(err).NotTo(HaveOccurred())
		block := resp.Message.Content[0]
		if valid {
			encoded, err := json.Marshal(block.ToolInput)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(encoded)).To(Equal(arguments))
		} else {
			Expect(block.ToolInput).To(BeNil())
			Expect(block.Content).To(MatchJSON(`{"arguments":` + string(quoted) + `}`))
		}
	},
		Entry("large integer", `{"n":9007199254740993}`, true),
		Entry("large exponent", `{"n":1e400}`, true),
		Entry("partial object", `{"ok":1,"bad":`, false),
		Entry("trailing JSON", `{"ok":1} {}`, false),
	)

	DescribeTable("rejects non-chat JSON", func(body string) {
		_, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, strings.NewReader(body), "application/json")
		Expect(err).To(HaveOccurred())
	}, Entry("Responses", `{"object":"response","output":[]}`), Entry("empty choices", `{"object":"chat.completion","choices":[]}`), Entry("invalid", `{`))

	It("honors cancellation and rejects nil bodies", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := capture.NewOpenAIChatCompletionsReducer().Reduce(ctx, nil, strings.NewReader(chatJSON), "application/json")
		Expect(err).To(MatchError(context.Canceled))
		_, err = capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, nil, "")
		Expect(err).To(HaveOccurred())
	})

	It("dispatches both protocols without confusing messages and input", func() {
		r := capture.NewOpenAIReducer()
		resp, err := r.Reduce(context.Background(), strings.NewReader(`{"messages":[]}`), strings.NewReader(chatJSON), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content[0].Text).To(Equal("hello"))
		resp, err = r.Reduce(context.Background(), strings.NewReader(`{"messages":[],"input":null}`), strings.NewReader(chatJSON), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Message.Content[0].Text).To(Equal("hello"))
		resp, err = r.Reduce(context.Background(), strings.NewReader(`{"input":"hello"}`), strings.NewReader(`{"object":"response","status":"completed","model":"existing","output":[]}`), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Model).To(Equal("existing"))
		resp, err = r.Reduce(context.Background(), strings.NewReader(`{"messages":null,"input":"hello"}`), strings.NewReader(`{"object":"response","status":"completed","model":"existing","output":[]}`), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Model).To(Equal("existing"))
		_, err = r.Reduce(context.Background(), strings.NewReader(`{"messages":[],"input":[]}`), strings.NewReader(chatJSON), "application/json")
		Expect(err).To(HaveOccurred())
	})
})

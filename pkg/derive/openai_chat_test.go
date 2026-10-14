package derive_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/capture/fixtures"
	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var _ = Describe("Chat Completions tool round trip", func() {
	It("links a captured function result to its tool span", func() {
		requests := []string{
			`{"model":"poc-cheap","messages":[{"role":"user","content":"Use echo to return ok"}]}`,
			`{"model":"poc-cheap","messages":[{"role":"user","content":"Use echo to return ok"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_poc","type":"function","function":{"name":"echo","arguments":"{\"value\":\"ok\"}"}}]},{"role":"tool","tool_call_id":"call_poc","content":"ok"}]}`,
		}
		responses := []string{
			`{"object":"chat.completion","model":"poc-cheap","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_poc","type":"function","function":{"name":"echo","arguments":"{\"value\":\"ok\"}"}}]},"finish_reason":"tool_calls"}]}`,
			`{"object":"chat.completion","model":"poc-cheap","choices":[{"index":0,"message":{"role":"assistant","content":"complete"},"finish_reason":"stop"}]}`,
		}
		dv, err := derive.NewDeriver("chat-test")
		Expect(err).NotTo(HaveOccurred())
		for i := range requests {
			response, err := capture.NewOpenAIReducer().Reduce(context.Background(), strings.NewReader(requests[i]), strings.NewReader(responses[i]), "application/json")
			Expect(err).NotTo(HaveOccurred())
			raw, err := json.Marshal(response)
			Expect(err).NotTo(HaveOccurred())
			dv.AddTurn(&storage.RawTurnRecord{
				ID: int64(i + 1), Provider: "openai", Source: storage.RawTurnSourceWire,
				HarnessID: "semantic-router-poc", HarnessSessionID: "chat-test",
				RawRequest: []byte(requests[i]), Response: raw,
				ReceivedAt: time.Unix(100+int64(i), 0),
			})
		}
		spans := derive.EmitSpans(dv.Finish())
		Expect(spans.Report.LinkKinds[derive.LinkFeeds]).To(Equal(1))
		var found bool
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindTool {
					Expect(span.Output).To(HaveLen(1))
					Expect(span.Output[0].ToolOutput).To(Equal("ok"))
					found = true
				}
			}
		}
		Expect(found).To(BeTrue())
	})
})

var _ = Describe("Chat Completions tool-less turns", func() {
	const (
		request  = `{"model":"poc-cheap","messages":[{"role":"user","content":"Say hello in five words."}]}`
		response = `{"object":"chat.completion","model":"poc-cheap","choices":[{"index":0,"message":{"role":"assistant","content":"Hello there, how are you?"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":7,"total_tokens":19}}`
		auxReq   = `{"model":"poc-cheap","messages":[{"role":"user","content":"Name this conversation: Say hello in five words."}]}`
		auxResp  = `{"object":"chat.completion","model":"poc-cheap","choices":[{"index":0,"message":{"role":"assistant","content":"Five Word Greeting"},"finish_reason":"stop"}]}`
	)
	key := derive.SessionKey{HarnessID: "hermes", HarnessSessionID: "chat-plain"}

	derived := func(envelopes ...string) *derive.SpanSet {
		reqs, resps := []string{request, auxReq}, []string{response, auxResp}
		dv, err := derive.NewDeriver("chat-plain")
		Expect(err).NotTo(HaveOccurred())
		for i, env := range envelopes {
			reduced, err := capture.NewOpenAIReducer().Reduce(context.Background(), strings.NewReader(reqs[i]), strings.NewReader(resps[i]), "application/json")
			Expect(err).NotTo(HaveOccurred())
			raw, err := json.Marshal(reduced)
			Expect(err).NotTo(HaveOccurred())
			rec := &storage.RawTurnRecord{
				ID: int64(i + 1), Provider: "openai", Source: storage.RawTurnSourceWire,
				HarnessID: key.HarnessID, HarnessSessionID: key.HarnessSessionID,
				RawRequest: []byte(reqs[i]), Response: raw,
				ReceivedAt: time.Unix(100+int64(i), 0),
			}
			if env != "" {
				rec.SessionEnvelope = json.RawMessage(env)
			} else {
				rec.HarnessID, rec.HarnessSessionID = "unknown", "0123456789abcdef"
			}
			dv.AddTurn(rec)
		}
		return derive.EmitSpans(dv.Finish())
	}

	It("derives an enveloped chat turn as the spine with a completed status", func() {
		spans := derived(`{"harness_id":"hermes","harness_session_id":"chat-plain"}`)
		Expect(spans.Report.CallKinds).To(Equal(map[string]int{derive.KindMain: 1}))
		Expect(spans.Turns).To(HaveLen(1))
		var llmSpans int
		for _, span := range spans.Turns[0].Spans {
			if span.Kind == derive.SpanKindLLM {
				llmSpans++
				Expect(span.CallKind).To(Equal(derive.KindMain))
			}
		}
		Expect(llmSpans).To(Equal(1))
		Expect(spans.Status[key].DerivedStatus).To(Equal(sessions.StatusCompleted))
	})

	It("keeps an aux-tagged call off the spine", func() {
		spans := derived(
			`{"harness_id":"hermes","harness_session_id":"chat-plain"}`,
			`{"harness_id":"hermes","harness_session_id":"chat-plain","harness_metadata":{"aux_task":"title_generation"}}`,
		)
		Expect(spans.Report.CallKinds).To(Equal(map[string]int{derive.KindMain: 1, derive.KindTitleGen: 1}))
		Expect(spans.Status[key].DerivedStatus).To(Equal(sessions.StatusCompleted))
	})

	It("leaves an un-enveloped tool-less call unknown", func() {
		spans := derived("")
		Expect(spans.Report.CallKinds).To(Equal(map[string]int{derive.KindUnknown: 1}))
	})
})

var _ = Describe("Chat Completions reasoning_content", func() {
	const reasoning = "The user wants tomorrow's weather in Hangzhou.\nI have a get_weather tool that takes a location and a date, so I should call it."
	firstRequest := `{"model":"deepseek-reasoner","stream":true,"messages":[{"role":"user","content":"What's the weather in Hangzhou tomorrow?"}]}`
	// DeepSeek's thinking-mode tool loop re-sends reasoning_content on the
	// assistant message that made the tool call.
	secondRequest := `{"model":"deepseek-reasoner","stream":true,"messages":[{"role":"user","content":"What's the weather in Hangzhou tomorrow?"},{"role":"assistant","content":"Let me check the weather in Hangzhou.","reasoning_content":` + mustJSON(reasoning) + `,"tool_calls":[{"id":"call_00_FIXTURE00000000000000","type":"function","function":{"name":"get_weather","arguments":"{\"location\": \"Hangzhou\", \"date\": \"2026-01-02\"}"}}]},{"role":"tool","tool_call_id":"call_00_FIXTURE00000000000000","content":"Cloudy, 7-13C"}]}`
	secondResponse := `{"id":"00000000-0000-4000-8000-000000000003","object":"chat.completion","created":1767225602,"model":"deepseek-reasoner","choices":[{"index":0,"message":{"role":"assistant","content":"Tomorrow in Hangzhou: cloudy, 7-13C.","reasoning_content":"The tool returned the forecast; summarize it."},"finish_reason":"stop"}],"usage":{"prompt_tokens":420,"completion_tokens":30,"total_tokens":450}}`

	It("surfaces reasoning as thinking on the LLM span and keeps the tool loop on one chain", func() {
		stream, err := fixtures.ReadFile("openai_chat/deepseek_stream.sse")
		Expect(err).NotTo(HaveOccurred())
		requests := []string{firstRequest, secondRequest}
		bodies := [][]byte{stream, []byte(secondResponse)}
		contentTypes := []string{"text/event-stream", "application/json"}

		dv, err := derive.NewDeriver("deepseek-test")
		Expect(err).NotTo(HaveOccurred())
		for i := range requests {
			response, err := capture.NewOpenAIReducer().Reduce(context.Background(), strings.NewReader(requests[i]), bytes.NewReader(bodies[i]), contentTypes[i])
			Expect(err).NotTo(HaveOccurred())
			raw, err := json.Marshal(response)
			Expect(err).NotTo(HaveOccurred())
			dv.AddTurn(&storage.RawTurnRecord{
				ID: int64(i + 1), Provider: "openai", Source: storage.RawTurnSourceWire,
				HarnessID: "deepseek", HarnessSessionID: "deepseek-test",
				RawRequest: []byte(requests[i]), Response: raw,
				ReceivedAt: time.Unix(100+int64(i), 0),
			})
		}
		set := dv.Finish()

		// The second call's history assistant node IS the first call's
		// response node: reasoning on both sides hashes identically.
		first, second := set.SpanSources[0].Chain, set.SpanSources[1].Chain
		Expect(second[1].Node.Hash).To(Equal(first[len(first)-1].Node.Hash))

		spans := derive.EmitSpans(set)
		Expect(spans.Report.LinkKinds[derive.LinkFeeds]).To(Equal(1))
		var thoughts []string
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindLLM {
					Expect(span.Output).NotTo(BeEmpty())
					Expect(span.Output[0].Type).To(Equal("thinking"))
					thoughts = append(thoughts, span.Output[0].Thinking)
				}
			}
		}
		Expect(thoughts).To(Equal([]string{reasoning, "The tool returned the forecast; summarize it."}))
	})

	DescribeTable("keeps the tool loop on one chain whichever reasoning field the client re-sends", func(field string) {
		const vllmReasoning = "The user asked for the square root of 1764.\nI'll use the calculator tool to be exact."
		stream, err := fixtures.ReadFile("openai_chat/vllm_reasoning_stream.sse")
		Expect(err).NotTo(HaveOccurred())
		requests := []string{
			`{"model":"Qwen/Qwen3-32B","stream":true,"messages":[{"role":"user","content":"What is sqrt(1764)?"}]}`,
			`{"model":"Qwen/Qwen3-32B","stream":true,"messages":[{"role":"user","content":"What is sqrt(1764)?"},{"role":"assistant","content":"Let me compute that.","` + field + `":` + mustJSON(vllmReasoning) + `,"tool_calls":[{"id":"chatcmpl-tool-FIXTURE00000000000000000001","type":"function","function":{"name":"calculator","arguments":"{\"expression\": \"sqrt(1764)\"}"}}]},{"role":"tool","tool_call_id":"chatcmpl-tool-FIXTURE00000000000000000001","content":"42"}]}`,
		}
		bodies := [][]byte{stream, []byte(`{"object":"chat.completion","model":"Qwen/Qwen3-32B","choices":[{"index":0,"message":{"role":"assistant","content":"42","reasoning":"The tool says 42."},"finish_reason":"stop"}]}`)}
		contentTypes := []string{"text/event-stream", "application/json"}

		dv, err := derive.NewDeriver("vllm-test")
		Expect(err).NotTo(HaveOccurred())
		for i := range requests {
			response, err := capture.NewOpenAIReducer().Reduce(context.Background(), strings.NewReader(requests[i]), bytes.NewReader(bodies[i]), contentTypes[i])
			Expect(err).NotTo(HaveOccurred())
			raw, err := json.Marshal(response)
			Expect(err).NotTo(HaveOccurred())
			dv.AddTurn(&storage.RawTurnRecord{
				ID: int64(i + 1), Provider: "openai", Source: storage.RawTurnSourceWire,
				HarnessID: "vllm", HarnessSessionID: "vllm-test",
				RawRequest: []byte(requests[i]), Response: raw,
				ReceivedAt: time.Unix(100+int64(i), 0),
			})
		}
		set := dv.Finish()
		first, second := set.SpanSources[0].Chain, set.SpanSources[1].Chain
		Expect(second[1].Node.Hash).To(Equal(first[len(first)-1].Node.Hash))
		Expect(derive.EmitSpans(set).Report.LinkKinds[derive.LinkFeeds]).To(Equal(1))
	},
		Entry("reasoning", "reasoning"),
		Entry("reasoning_content", "reasoning_content"),
	)

	It("keeps one trace when the client strips reasoning from history", func() {
		// deepseek-reasoner clients drop reasoning_content between user
		// turns. Each reasoned response then hangs off the spine as its own
		// leaf (the history copy without reasoning hashes apart, as an
		// Anthropic client dropping thinking would), but the trace keeps
		// one LLM span per call, each showing its reasoning.
		requests := []string{
			`{"model":"deepseek-reasoner","messages":[{"role":"user","content":"q1"}]}`,
			`{"model":"deepseek-reasoner","messages":[{"role":"user","content":"q1"},{"role":"assistant","content":"a1"},{"role":"user","content":"q2"}]}`,
		}
		responses := []string{
			`{"object":"chat.completion","model":"deepseek-reasoner","choices":[{"index":0,"message":{"role":"assistant","content":"a1","reasoning_content":"r1"},"finish_reason":"stop"}]}`,
			`{"object":"chat.completion","model":"deepseek-reasoner","choices":[{"index":0,"message":{"role":"assistant","content":"a2","reasoning_content":"r2"},"finish_reason":"stop"}]}`,
		}
		dv, err := derive.NewDeriver("deepseek-strip")
		Expect(err).NotTo(HaveOccurred())
		for i := range requests {
			response, err := capture.NewOpenAIReducer().Reduce(context.Background(), strings.NewReader(requests[i]), strings.NewReader(responses[i]), "application/json")
			Expect(err).NotTo(HaveOccurred())
			raw, err := json.Marshal(response)
			Expect(err).NotTo(HaveOccurred())
			dv.AddTurn(&storage.RawTurnRecord{
				ID: int64(i + 1), Provider: "openai", Source: storage.RawTurnSourceWire,
				HarnessID: "deepseek", HarnessSessionID: "deepseek-strip",
				RawRequest: []byte(requests[i]), Response: raw,
				ReceivedAt: time.Unix(100+int64(i), 0),
			})
		}
		spans := derive.EmitSpans(dv.Finish())
		Expect(spans.Report.Traces).To(Equal(1))
		Expect(spans.Report.SpanKinds[derive.SpanKindLLM]).To(Equal(2))
		var thoughts []string
		for _, turn := range spans.Turns {
			for _, span := range turn.Spans {
				if span.Kind == derive.SpanKindLLM {
					Expect(span.Output).To(HaveLen(2))
					thoughts = append(thoughts, span.Output[0].Thinking)
				}
			}
		}
		Expect(thoughts).To(Equal([]string{"r1", "r2"}))
	})

	It("parses re-sent reasoning_content into the same thinking block the reducer captured", func() {
		req, err := openai.New().ParseRequest([]byte(secondRequest))
		Expect(err).NotTo(HaveOccurred())
		oneshot, err := fixtures.ReadFile("openai_chat/deepseek_oneshot.json")
		Expect(err).NotTo(HaveOccurred())
		resp, err := capture.NewOpenAIChatCompletionsReducer().Reduce(context.Background(), nil, bytes.NewReader(oneshot), "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages[1].Content).To(Equal(resp.Message.Content))
	})
})

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

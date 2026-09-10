package derive_test

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/derive"
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

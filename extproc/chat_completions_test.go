package extproc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/capture"
)

var _ = Describe("Chat Completions capture lanes", func() {
	DescribeTable("captures the same response live and from stored bytes", func(mode RawResponseMode, streaming bool) {
		request := []byte(`{"model":"poc-cheap","messages":[{"role":"user","content":"hello"}],"stream":false}`)
		body := `{"id":"chatcmpl_test","object":"chat.completion","model":"poc-cheap","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":4,"total_tokens":16}}`
		ct := "application/json"
		if streaming {
			request = bytes.ReplaceAll(request, []byte("false"), []byte("true"))
			ct = "text/event-stream"
			body = "data: " + strings.ReplaceAll(strings.ReplaceAll(body, `"chat.completion"`, `"chat.completion.chunk"`), `"message":`, `"delta":`) + "\n\ndata: [DONE]\n\n"
		}
		payloads := make(chan ingest.TurnPayload, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var payload ingest.TurnPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			payloads <- payload
			w.WriteHeader(http.StatusAccepted)
		}))
		defer server.Close()
		proc, err := NewProcessor(Config{IngestURL: server.URL, MaxInflight: 4, RawResponseMode: mode})
		Expect(err).NotTo(HaveOccurred())
		stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{":method": "POST", ":path": "/v1/chat/completions", "x-tapes-harness-id": "unknown", "x-tapes-harness-session-id": "chat-test"}),
			reqBodyReq(request, true), respHeaderReq("200", ct),
			respBodyReq([]byte(body[:len(body)/2]), false), respBodyReq([]byte(body[len(body)/2:]), true),
		}}
		Expect(proc.Process(stream)).To(Succeed())
		var payload ingest.TurnPayload
		Eventually(payloads).WithTimeout(3 * time.Second).Should(Receive(&payload))
		Expect(payload.Provider).To(Equal("openai"))
		Expect(payload.Session).NotTo(BeNil())
		Expect(payload.Session.HarnessID).To(Equal("unknown"))
		Expect(payload.Session.HarnessSessionID).To(Equal("chat-test"))
		Expect(payload.RawRequest).To(MatchJSON(request))
		if mode == RawResponseOff {
			Expect(payload.RawResponse).To(BeEmpty())
		} else {
			Expect(string(payload.RawResponse)).To(Equal(body))
		}
		if mode != RawResponseRaw {
			Expect(payload.Response.Message.Content[0].Text).To(Equal("hello"))
		} else {
			Expect(ingest.ReducedResponseAbsent(payload.Response)).To(BeTrue())
		}
		if mode != RawResponseOff {
			stored, err := ingest.ReduceStoredRawTurn(context.Background(), ingest.StoredRawTurn{Provider: payload.Provider, RawRequest: payload.RawRequest, RawResponse: payload.RawResponse, Meta: payload.Meta})
			Expect(err).NotTo(HaveOccurred())
			live, err := capture.NewOpenAIReducer().Reduce(context.Background(), bytes.NewReader(request), strings.NewReader(body), ct)
			Expect(err).NotTo(HaveOccurred())
			Expect(stored.Response.Message).To(Equal(live.Message))
			Expect(stored.Response.Model).To(Equal(live.Model))
			Expect(stored.Response.Usage.TotalTokens).To(Equal(live.Usage.TotalTokens))
			Expect(stored.Response.Done).To(BeTrue())
		}
	},
		Entry("JSON off", RawResponseOff, false), Entry("JSON dual", RawResponseDual, false), Entry("JSON raw", RawResponseRaw, false),
		Entry("SSE off", RawResponseOff, true), Entry("SSE dual", RawResponseDual, true), Entry("SSE raw", RawResponseRaw, true),
	)
})

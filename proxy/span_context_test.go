package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/proxy/header"
)

type proxySpanDriver struct {
	*inmemory.Driver
	mu    sync.Mutex
	calls []storage.IngestSpanTurnRequest
}

func (d *proxySpanDriver) IngestSpanTurn(_ context.Context, req storage.IngestSpanTurnRequest) (storage.IngestSpanTurnResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, req)
	return storage.IngestSpanTurnResult{SessionID: "session", TurnID: "turn", TraceID: req.SpanContext.TraceID, SpanCount: 2}, nil
}

func (d *proxySpanDriver) lastCall() storage.IngestSpanTurnRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls[len(d.calls)-1]
}

var _ = Describe("Proxy span context headers", func() {
	It("captures extension-provided trace/span ids and strips them before upstream", func() {
		var upstreamSawTrace string
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upstreamSawTrace = r.Header.Get(header.PiTraceIDHeader) + r.Header.Get(header.TraceIDHeader)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(makeOllamaResponseBody("test-model", "assistant", "pong"))
		}))
		defer upstream.Close()

		driver := &proxySpanDriver{Driver: inmemory.NewDriver()}
		p, err := New(Config{ListenAddr: ":0", UpstreamURL: upstream.URL, ProviderType: "ollama"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{{Role: "user", Content: "ping"}}, boolPtr(false))
		req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody)))
		req.Header.Set(header.PiTraceIDHeader, "trc_pi_123")
		req.Header.Set(header.PiTurnIDHeader, "turn_pi_123")
		req.Header.Set(header.PiRootSpanIDHeader, "agent_pi_123")
		req.Header.Set(header.PiLLMSpanIDHeader, "llm_pi_123")
		req.Header.Set(header.PiParentSpanIDHeader, "agent_pi_123")

		resp, err := p.server.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()
		p.Close()

		Expect(upstreamSawTrace).To(BeEmpty(), "proxy-internal span headers must not leak upstream")
		call := driver.lastCall()
		Expect(call.SpanContext).NotTo(BeNil())
		Expect(call.SpanContext.TraceID).To(Equal("trc_pi_123"))
		Expect(call.SpanContext.TurnID).To(Equal("turn_pi_123"))
		Expect(call.SpanContext.RootSpanID).To(Equal("agent_pi_123"))
		Expect(call.SpanContext.LLMSpanID).To(Equal("llm_pi_123"))
		Expect(call.SpanContext.ParentSpanID).To(Equal("agent_pi_123"))
	})
})

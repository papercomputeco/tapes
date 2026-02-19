package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// openaiTestRequest is a minimal OpenAI-format request for test fixtures.
type openaiTestRequest struct {
	Model    string               `json:"model"`
	Messages []openaiTestMsgEntry `json:"messages"`
	Stream   *bool                `json:"stream,omitempty"`
}

type openaiTestMsgEntry struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// newOpenAITestProxy creates a Proxy pointed at the given upstream URL,
// using an in-memory storage driver and the openai provider.
func newOpenAITestProxy(upstreamURL string) (*Proxy, *inmemory.Driver) {
	logger := tapeslogger.Nop()
	driver := inmemory.NewDriver()

	p, err := New(
		Config{
			ListenAddr:   ":0",
			UpstreamURL:  upstreamURL,
			ProviderType: "openai",
		},
		driver,
		logger,
	)
	Expect(err).NotTo(HaveOccurred())
	return p, driver
}

// makeOpenAIRequestBody builds a JSON-encoded OpenAI chat request.
func makeOpenAIRequestBody(model string, messages []openaiTestMsgEntry, stream *bool) []byte {
	body, err := json.Marshal(openaiTestRequest{
		Model:    model,
		Messages: messages,
		Stream:   stream,
	})
	Expect(err).NotTo(HaveOccurred())
	return body
}

var _ = Describe("SSE Streaming Proxy", func() {
	var (
		p        *Proxy
		driver   *inmemory.Driver
		upstream *httptest.Server
	)

	AfterEach(func() {
		if p != nil {
			p.Close()
		}
		if upstream != nil {
			upstream.Close()
		}
	})

	Context("when upstream returns an OpenAI SSE streaming response", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				flusher, ok := w.(http.Flusher)
				Expect(ok).To(BeTrue())

				events := []string{
					"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"}}]}\n\n",
					"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
					"data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"!\"}}]}\n\n",
					"data: [DONE]\n\n",
				}

				for _, event := range events {
					fmt.Fprint(w, event)
					flusher.Flush()
				}
			}))
			p, driver = newOpenAITestProxy(upstream.URL)
		})

		It("preserves SSE event boundaries with \\n\\n delimiters", func() {
			reqBody := makeOpenAIRequestBody("gpt-4", []openaiTestMsgEntry{
				{Role: "user", Content: "Say hello"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			bodyStr := string(body)

			// The critical assertion: SSE event boundaries must be preserved.
			// Each event must end with \n\n, not just \n.
			Expect(bodyStr).To(ContainSubstring("data: {\"id\":\"chatcmpl-1\""))
			Expect(bodyStr).To(ContainSubstring("data: [DONE]\n\n"))

			// Verify individual events are separated by \n\n
			Expect(strings.Count(bodyStr, "\n\n")).To(BeNumerically(">=", 4))
		})

		It("streams all OpenAI chunks to the client", func() {
			reqBody := makeOpenAIRequestBody("gpt-4", []openaiTestMsgEntry{
				{Role: "user", Content: "Say hello"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			bodyStr := string(body)

			Expect(bodyStr).To(ContainSubstring(`"content":"Hello"`))
			Expect(bodyStr).To(ContainSubstring(`"content":" world"`))
			Expect(bodyStr).To(ContainSubstring(`"content":"!"`))
			Expect(bodyStr).To(ContainSubstring("[DONE]"))
		})

		It("accumulates content and stores the conversation after SSE streaming", func() {
			reqBody := makeOpenAIRequestBody("gpt-4", []openaiTestMsgEntry{
				{Role: "user", Content: "Say hello"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			// Drain the worker pool to ensure async storage completes
			p.Close()
			p = nil

			ctx := GinkgoT().Context()
			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			// 1 user message + 1 assistant response = 2 nodes
			Expect(nodes).To(HaveLen(2))

			leaves, err := driver.Leaves(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(leaves).To(HaveLen(1))
			Expect(leaves[0].Bucket.Role).To(Equal("assistant"))
			// The accumulated content from all SSE chunks
			Expect(leaves[0].Bucket.ExtractText()).To(Equal("Hello world!"))
		})
	})

	Context("when upstream returns an Anthropic-style SSE response with event types", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				flusher, ok := w.(http.Flusher)
				Expect(ok).To(BeTrue())

				events := []string{
					"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-3\"}}\n\n",
					"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hi there\"}}\n\n",
					"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
				}

				for _, event := range events {
					fmt.Fprint(w, event)
					flusher.Flush()
				}
			}))
			p, driver = newOpenAITestProxy(upstream.URL)
		})

		It("preserves event type and data fields with \\n\\n delimiters", func() {
			reqBody := makeOpenAIRequestBody("claude-3", []openaiTestMsgEntry{
				{Role: "user", Content: "Hi"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			bodyStr := string(body)

			// Event type lines must be preserved
			Expect(bodyStr).To(ContainSubstring("event: message_start\n"))
			Expect(bodyStr).To(ContainSubstring("event: content_block_delta\n"))
			Expect(bodyStr).To(ContainSubstring("event: message_stop\n"))

			// Data lines must be present
			Expect(bodyStr).To(ContainSubstring("data: {\"type\":\"message_start\""))
			Expect(bodyStr).To(ContainSubstring("data: {\"type\":\"content_block_delta\""))

			// Event boundaries must use \n\n
			Expect(strings.Count(bodyStr, "\n\n")).To(BeNumerically(">=", 3))
		})
	})

	Context("when upstream SSE includes comment lines", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				flusher, ok := w.(http.Flusher)
				Expect(ok).To(BeTrue())

				// Some providers send comment lines as keep-alives
				events := []string{
					": keep-alive\n\n",
					"data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n",
					"data: [DONE]\n\n",
				}

				for _, event := range events {
					fmt.Fprint(w, event)
					flusher.Flush()
				}
			}))
			p, driver = newOpenAITestProxy(upstream.URL)
		})

		It("forwards comment lines verbatim to the client", func() {
			reqBody := makeOpenAIRequestBody("gpt-4", []openaiTestMsgEntry{
				{Role: "user", Content: "test"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			bodyStr := string(body)

			Expect(bodyStr).To(ContainSubstring(": keep-alive\n"))
			Expect(bodyStr).To(ContainSubstring("data: {\"choices\""))
		})
	})
})

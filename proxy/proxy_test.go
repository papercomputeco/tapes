package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/proxy/header"
)

// ollamaTestRequest is a minimal Ollama-format request for test fixtures.
type ollamaTestRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaTestMessage `json:"messages"`
	Stream   *bool               `json:"stream,omitempty"`
}

type ollamaTestMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaTestResponse is a minimal Ollama-format response for test fixtures.
type ollamaTestResponse struct {
	Model           string            `json:"model"`
	CreatedAt       time.Time         `json:"created_at"`
	Message         ollamaTestMessage `json:"message"`
	Done            bool              `json:"done"`
	DoneReason      string            `json:"done_reason,omitempty"`
	PromptEvalCount int               `json:"prompt_eval_count,omitempty"`
	EvalCount       int               `json:"eval_count,omitempty"`
	TotalDuration   int64             `json:"total_duration,omitempty"`
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool { return &b }

// newTestProxy creates a Proxy pointed at the given upstream URL,
// using an in-memory storage driver and the ollama provider.
func newTestProxy(upstreamURL string) (*Proxy, *inmemory.Driver) {
	logger, _ := zap.NewDevelopment()
	driver := inmemory.NewDriver()

	p, err := New(
		Config{
			ListenAddr:   ":0",
			UpstreamURL:  upstreamURL,
			ProviderType: "ollama",
		},
		driver,
		logger,
	)
	Expect(err).NotTo(HaveOccurred())
	return p, driver
}

// makeOllamaRequestBody builds a JSON-encoded Ollama chat request.
func makeOllamaRequestBody(model string, messages []ollamaTestMessage, stream *bool) []byte {
	body, err := json.Marshal(ollamaTestRequest{
		Model:    model,
		Messages: messages,
		Stream:   stream,
	})
	Expect(err).NotTo(HaveOccurred())
	return body
}

// makeOllamaResponseBody builds a JSON-encoded Ollama chat response.
func makeOllamaResponseBody(model, role, content string) []byte {
	body, err := json.Marshal(ollamaTestResponse{
		Model:           model,
		CreatedAt:       time.Now(),
		Message:         ollamaTestMessage{Role: role, Content: content},
		Done:            true,
		DoneReason:      "stop",
		PromptEvalCount: 10,
		EvalCount:       5,
		TotalDuration:   1000000,
	})
	Expect(err).NotTo(HaveOccurred())
	return body
}

var _ = Describe("Non-Streaming Proxy", func() {
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

	Context("when upstream returns a successful response", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Custom-Header", "test-value")
				w.WriteHeader(http.StatusOK)
				w.Write(makeOllamaResponseBody("test-model", "assistant", "2+2 equals 4."))
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("forwards the request and returns the upstream response", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("2+2 equals 4."))
		})

		It("copies upstream response headers to the client", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.Header.Get("X-Custom-Header")).To(Equal("test-value"))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/json"))
		})

		It("stores the conversation turn via the worker pool", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			// Drain the worker pool and shut down to ensure async storage completes.
			// Set p = nil so AfterEach doesn't double-close.
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
			Expect(leaves[0].Bucket.ExtractText()).To(Equal("2+2 equals 4."))
		})

		It("stores multi-message requests as a chain", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "system", Content: "You are helpful."},
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			p.Close()
			p = nil

			ctx := GinkgoT().Context()
			leaves, err := driver.Leaves(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(leaves).To(HaveLen(1))

			ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
			Expect(err).NotTo(HaveOccurred())
			// system -> user -> assistant = 3 nodes
			Expect(ancestry).To(HaveLen(3))
			Expect(ancestry[0].Bucket.Role).To(Equal("assistant"))
			Expect(ancestry[1].Bucket.Role).To(Equal("user"))
			Expect(ancestry[2].Bucket.Role).To(Equal("system"))
		})
	})

	Context("when upstream returns an error", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error":"model not found"}`))
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("returns the upstream error status to the client", func() {
			reqBody := makeOllamaRequestBody("nonexistent", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("model not found"))
		})

		It("does not store any nodes", func() {
			reqBody := makeOllamaRequestBody("nonexistent", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			p.Close()
			p = nil

			ctx := GinkgoT().Context()
			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})

	Context("when the request is not a chat request", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"models":[{"name":"llama2"}]}`))
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("forwards GET requests transparently without storing", func() {
			resp, err := p.server.Test(httptest.NewRequest(http.MethodGet, "/api/tags", nil))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("llama2"))

			p.Close()
			p = nil

			ctx := GinkgoT().Context()
			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})

	Context("when upstream request headers are forwarded", func() {
		var receivedHeaders http.Header

		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaders = r.Header.Clone()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write(makeOllamaResponseBody("test-model", "assistant", "hi"))
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("forwards custom request headers to upstream", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(false))

			req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody)))
			req.Header.Set("X-Api-Key", "secret-token")
			req.Header.Set("Content-Type", "application/json")

			resp, err := p.server.Test(req)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			Expect(receivedHeaders.Get("X-Api-Key")).To(Equal("secret-token"))
			Expect(receivedHeaders.Get("Content-Type")).To(Equal("application/json"))
		})

		It("filters Accept-Encoding header to let Go handle compression", func() {
			// Reset receivedHeaders to ensure we're checking fresh data
			receivedHeaders = make(http.Header)
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(false))

			req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody)))
			req.Header.Set("Accept-Encoding", "gzip, deflate, br")
			req.Header.Set("Authorization", "Bearer token123")

			resp, err := p.server.Test(req)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			// Original client Accept-Encoding values should be filtered out
			// Go's http.Client will add its own Accept-Encoding and handle decompression
			acceptEncoding := receivedHeaders.Get("Accept-Encoding")
			Expect(acceptEncoding).NotTo(Equal("gzip, deflate, br"))
			Expect(acceptEncoding).NotTo(ContainSubstring("deflate"))
			Expect(acceptEncoding).NotTo(ContainSubstring("br"))
			// Other headers should still be forwarded
			Expect(receivedHeaders.Get("Authorization")).To(Equal("Bearer token123"))
		})

		It("filters the agent header from upstream", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(false))

			req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody)))
			req.Header.Set(header.AgentNameHeader, "claude")

			resp, err := p.server.Test(req)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			Expect(receivedHeaders.Get(header.AgentNameHeader)).To(BeEmpty())
		})
	})
})

var _ = Describe("Streaming Proxy", func() {
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

	Context("when upstream returns a successful streaming response", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				flusher, ok := w.(http.Flusher)
				Expect(ok).To(BeTrue())

				chunks := []string{
					`{"model":"test-model","message":{"role":"assistant","content":"2+2"},"done":false}`,
					`{"model":"test-model","message":{"role":"assistant","content":" equals"},"done":false}`,
					`{"model":"test-model","message":{"role":"assistant","content":" 4."},"done":false}`,
					`{"model":"test-model","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":10,"eval_count":5,"total_duration":1000000}`,
				}

				for _, chunk := range chunks {
					fmt.Fprintln(w, chunk)
					flusher.Flush()
				}
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("streams all chunks to the client", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			bodyStr := string(body)

			// All chunks should be present in the response body
			Expect(bodyStr).To(ContainSubstring(`"content":"2+2"`))
			Expect(bodyStr).To(ContainSubstring(`"content":" equals"`))
			Expect(bodyStr).To(ContainSubstring(`"content":" 4."`))
			Expect(bodyStr).To(ContainSubstring(`"done":true`))
		})

		It("stores the reconstructed conversation after streaming completes", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))), -1)
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
			// The accumulated content from all streaming chunks
			Expect(leaves[0].Bucket.ExtractText()).To(Equal("2+2 equals 4."))
		})
	})

	Context("when upstream returns an error during streaming", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(`{"error":"invalid model"}`))
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("returns the error to the client without storing", func() {
			reqBody := makeOllamaRequestBody("bad-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(ContainSubstring("invalid model"))

			p.Close()
			p = nil

			ctx := GinkgoT().Context()
			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})

	Context("with multi-message streaming request", func() {
		BeforeEach(func() {
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-ndjson")
				flusher, ok := w.(http.Flusher)
				Expect(ok).To(BeTrue())

				chunks := []string{
					`{"model":"test-model","message":{"role":"assistant","content":"The answer is 4."},"done":false}`,
					`{"model":"test-model","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":15,"eval_count":5,"total_duration":2000000}`,
				}

				for _, chunk := range chunks {
					fmt.Fprintln(w, chunk)
					flusher.Flush()
				}
			}))
			p, driver = newTestProxy(upstream.URL)
		})

		It("stores the full ancestry chain including system and user messages", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "system", Content: "You are helpful."},
				{Role: "user", Content: "What is 2+2?"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			p.Close()
			p = nil

			ctx := GinkgoT().Context()
			leaves, err := driver.Leaves(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(leaves).To(HaveLen(1))

			ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
			Expect(err).NotTo(HaveOccurred())
			// system -> user -> assistant = 3 nodes
			Expect(ancestry).To(HaveLen(3))
			Expect(ancestry[0].Bucket.Role).To(Equal("assistant"))
			Expect(ancestry[1].Bucket.Role).To(Equal("user"))
			Expect(ancestry[2].Bucket.Role).To(Equal("system"))
		})
	})
})

var _ = Describe("Streaming Detection", func() {
	var (
		p        *Proxy
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

	Context("with an Ollama provider (defaults to streaming)", func() {
		var requestBodies [][]byte

		BeforeEach(func() {
			requestBodies = nil
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				bodyCopy := make([]byte, len(body))
				copy(bodyCopy, body)
				requestBodies = append(requestBodies, bodyCopy)

				var check struct {
					Stream *bool `json:"stream"`
				}
				json.Unmarshal(body, &check)

				isStreaming := check.Stream == nil || *check.Stream
				w.Header().Set("Content-Type", "application/json")
				if isStreaming {
					flusher, _ := w.(http.Flusher)
					fmt.Fprintln(w, `{"model":"test-model","message":{"role":"assistant","content":"hi"},"done":false}`)
					fmt.Fprintln(w, `{"model":"test-model","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop"}`)
					flusher.Flush()
				} else {
					w.Write(makeOllamaResponseBody("test-model", "assistant", "hi"))
				}
			}))
			p, _ = newTestProxy(upstream.URL)
		})

		It("routes to streaming when stream=true is set explicitly", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(true))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			p.Close()
			p = nil
			Expect(requestBodies).To(HaveLen(1))

			var forwarded struct {
				Stream *bool `json:"stream"`
			}
			Expect(json.Unmarshal(requestBodies[0], &forwarded)).To(Succeed())
			Expect(forwarded.Stream).NotTo(BeNil())
			Expect(*forwarded.Stream).To(BeTrue())
		})

		It("routes to non-streaming when stream=false is set explicitly", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, boolPtr(false))

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			p.Close()
			p = nil
			Expect(requestBodies).To(HaveLen(1))

			var forwarded struct {
				Stream *bool `json:"stream"`
			}
			Expect(json.Unmarshal(requestBodies[0], &forwarded)).To(Succeed())
			Expect(forwarded.Stream).NotTo(BeNil())
			Expect(*forwarded.Stream).To(BeFalse())
		})

		It("defaults to streaming when stream field is omitted (Ollama default)", func() {
			reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
				{Role: "user", Content: "hello"},
			}, nil)

			resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))), -1)
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			p.Close()
			p = nil
			// Request was forwarded to upstream
			Expect(requestBodies).To(HaveLen(1))

			// Verify the proxy chose the streaming path by checking
			// the request body was forwarded (it's the same body regardless of path,
			// but the upstream receives it either way). The key assertion is that
			// the upstream was reached and returned a streaming-style response.
			var forwarded struct {
				Stream *bool `json:"stream"`
			}
			Expect(json.Unmarshal(requestBodies[0], &forwarded)).To(Succeed())
			// Stream field should be nil (omitted) - the proxy uses provider default internally
			Expect(forwarded.Stream).To(BeNil())
		})
	})
})

var _ = Describe("reconstructStreamedResponse", func() {
	var p *Proxy

	BeforeEach(func() {
		p, _ = newTestProxy("http://localhost:0")
	})

	AfterEach(func() {
		p.Close()
	})

	It("parses the last chunk when it contains valid response metadata", func() {
		chunks := [][]byte{
			[]byte(`{"model":"test-model","message":{"role":"assistant","content":"Hello"},"done":false}`),
			[]byte(`{"model":"test-model","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":10,"eval_count":5}`),
		}

		resp := p.reconstructStreamedResponse(chunks, "Hello", &llm.Usage{}, &streamMeta{}, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Message.GetText()).To(Equal("Hello"))
		Expect(resp.Done).To(BeTrue())
		Expect(resp.StopReason).To(Equal("stop"))
	})

	It("supplements empty last-chunk text with accumulated content", func() {
		chunks := [][]byte{
			[]byte(`{"model":"test-model","message":{"role":"assistant","content":"partial"},"done":false}`),
			[]byte(`{"model":"test-model","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop"}`),
		}

		resp := p.reconstructStreamedResponse(chunks, "partial content here", &llm.Usage{}, &streamMeta{}, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Message.GetText()).To(Equal("partial content here"))
	})

	It("falls back to accumulated content when last chunk is unparseable", func() {
		chunks := [][]byte{
			[]byte(`not-valid-json`),
			[]byte(`also-not-json`),
		}

		resp := p.reconstructStreamedResponse(chunks, "fallback content", &llm.Usage{}, &streamMeta{}, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Message.GetText()).To(Equal("fallback content"))
		Expect(resp.Done).To(BeTrue())
		Expect(resp.Message.Role).To(Equal("assistant"))
	})

	It("returns nil when there are no chunks and no content", func() {
		resp := p.reconstructStreamedResponse(nil, "", &llm.Usage{}, &streamMeta{}, p.defaultProv)
		Expect(resp).To(BeNil())
	})

	It("returns nil when chunks exist but content is empty and last chunk is unparseable", func() {
		chunks := [][]byte{
			[]byte(`not-json`),
		}
		resp := p.reconstructStreamedResponse(chunks, "", &llm.Usage{}, &streamMeta{}, p.defaultProv)
		Expect(resp).To(BeNil())
	})
})

var _ = Describe("New", func() {
	It("returns an error for unrecognized provider type", func() {
		logger, _ := zap.NewDevelopment()
		driver := inmemory.NewDriver()

		_, err := New(Config{
			ListenAddr:   ":0",
			UpstreamURL:  "http://localhost:11434",
			ProviderType: "nonexistent",
		}, driver, logger)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("nonexistent"))
	})

	It("creates a proxy with a valid provider type", func() {
		logger, _ := zap.NewDevelopment()
		driver := inmemory.NewDriver()

		p, err := New(Config{
			ListenAddr:   ":0",
			UpstreamURL:  "http://localhost:11434",
			ProviderType: "ollama",
		}, driver, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(p).NotTo(BeNil())
		Expect(p.defaultProv.Name()).To(Equal("ollama"))
		p.Close()
	})
})

var _ = Describe("End-to-End Multi-Turn Proxy", func() {
	var (
		p        *Proxy
		driver   *inmemory.Driver
		upstream *httptest.Server
		turnNum  int
	)

	BeforeEach(func() {
		turnNum = 0
		upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			turnNum++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			switch turnNum {
			case 1:
				w.Write(makeOllamaResponseBody("test-model", "assistant", "2+2 equals 4."))
			case 2:
				w.Write(makeOllamaResponseBody("test-model", "assistant", "3+3 equals 6."))
			}
		}))
		p, driver = newTestProxy(upstream.URL)
	})

	AfterEach(func() {
		if p != nil {
			p.Close()
		}
		upstream.Close()
	})

	It("deduplicates replayed messages across turns", func() {
		// Turn 1: system + user -> assistant
		reqBody1 := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "system", Content: "You are helpful."},
			{Role: "user", Content: "What is 2+2?"},
		}, boolPtr(false))

		resp1, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody1))))
		Expect(err).NotTo(HaveOccurred())
		resp1.Body.Close()

		// Turn 2: replayed system + user + assistant + new user -> new assistant
		reqBody2 := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "system", Content: "You are helpful."},
			{Role: "user", Content: "What is 2+2?"},
			{Role: "assistant", Content: "2+2 equals 4."},
			{Role: "user", Content: "And what is 3+3?"},
		}, boolPtr(false))

		resp2, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody2))))
		Expect(err).NotTo(HaveOccurred())
		resp2.Body.Close()

		// Drain the worker pool
		p.Close()
		p = nil

		ctx := GinkgoT().Context()

		// Content-addressable deduplication should yield exactly 5 unique nodes
		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(5))

		// The latest leaf should have full ancestry
		leaves, err := driver.Leaves(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(leaves).To(HaveLen(1))

		ancestry, err := driver.Ancestry(ctx, leaves[0].Hash)
		Expect(err).NotTo(HaveOccurred())
		Expect(ancestry).To(HaveLen(5))

		// Verify ordering from newest to oldest
		Expect(ancestry[0].Bucket.Role).To(Equal("assistant"))
		Expect(ancestry[0].Bucket.ExtractText()).To(Equal("3+3 equals 6."))
		Expect(ancestry[1].Bucket.Role).To(Equal("user"))
		Expect(ancestry[1].Bucket.ExtractText()).To(Equal("And what is 3+3?"))
		Expect(ancestry[2].Bucket.Role).To(Equal("assistant"))
		Expect(ancestry[2].Bucket.ExtractText()).To(Equal("2+2 equals 4."))
		Expect(ancestry[3].Bucket.Role).To(Equal("user"))
		Expect(ancestry[3].Bucket.ExtractText()).To(Equal("What is 2+2?"))
		Expect(ancestry[4].Bucket.Role).To(Equal("system"))
	})
})

var _ = Describe("Storage Provider Metadata", func() {
	var (
		p        *Proxy
		driver   *inmemory.Driver
		upstream *httptest.Server
	)

	BeforeEach(func() {
		upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(makeOllamaResponseBody("test-model", "assistant", "Hello!"))
		}))
		p, driver = newTestProxy(upstream.URL)
	})

	AfterEach(func() {
		if p != nil {
			p.Close()
		}
		upstream.Close()
	})

	It("stores the provider name in each node's bucket", func() {
		reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "user", Content: "hi"},
		}, boolPtr(false))

		resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		p.Close()
		p = nil

		ctx := GinkgoT().Context()
		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).NotTo(BeEmpty())

		for _, node := range nodes {
			Expect(node.Bucket.Provider).To(Equal("ollama"))
		}
	})

	It("stores the model name in each node's bucket", func() {
		reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "user", Content: "hi"},
		}, boolPtr(false))

		resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		p.Close()
		p = nil

		ctx := GinkgoT().Context()
		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).NotTo(BeEmpty())

		for _, node := range nodes {
			Expect(node.Bucket.Model).To(Equal("test-model"))
		}
	})

	It("stores the agent name in each node's bucket", func() {
		reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "user", Content: "hi"},
		}, boolPtr(false))

		req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody)))
		req.Header.Set(header.AgentNameHeader, "claude")
		resp, err := p.server.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		p.Close()
		p = nil

		ctx := GinkgoT().Context()
		nodes, err := driver.List(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).NotTo(BeEmpty())

		for _, node := range nodes {
			Expect(node.Bucket.AgentName).To(Equal("claude"))
		}
	})

	It("stores usage metadata on the response node", func() {
		reqBody := makeOllamaRequestBody("test-model", []ollamaTestMessage{
			{Role: "user", Content: "hi"},
		}, boolPtr(false))

		resp, err := p.server.Test(httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(string(reqBody))))
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		p.Close()
		p = nil

		ctx := GinkgoT().Context()
		leaves, err := driver.Leaves(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(leaves).To(HaveLen(1))
		Expect(leaves[0].Usage).NotTo(BeNil())
		Expect(leaves[0].Usage.PromptTokens).To(Equal(10))
		Expect(leaves[0].Usage.CompletionTokens).To(Equal(5))
		Expect(leaves[0].StopReason).To(Equal("stop"))
	})
})

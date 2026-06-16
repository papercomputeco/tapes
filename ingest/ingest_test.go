package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
)

// ollamaRequest is a minimal Ollama-format request for test fixtures.
type ollamaRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   *bool           `json:"stream,omitempty"`
}

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openaiRequest is a minimal OpenAI-format request for test fixtures.
type openaiRequest struct {
	Model    string          `json:"model"`
	Messages []openaiMessage `json:"messages"`
}

type openaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func reducedResponse(model, text string, usage *llm.Usage) llm.ChatResponse {
	return llm.ChatResponse{
		Model:      model,
		Message:    llm.NewTextMessage("assistant", text),
		Done:       true,
		StopReason: "stop",
		Usage:      usage,
	}
}

func newTestServer() (*ingest.Server, *captureDriver, string) {
	logger := tapeslogger.NewNoop()
	driver := newCaptureDriver()

	s, err := ingest.New(
		ingest.Config{
			ListenAddr: ":0",
			Project:    "test-project",
		},
		driver,
		logger,
	)
	Expect(err).NotTo(HaveOccurred())

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	go func() {
		_ = s.RunWithListener(ln)
	}()

	baseURL := "http://" + ln.Addr().String()
	return s, driver, baseURL
}

var _ = Describe("Ingest Server", func() {
	var (
		server  *ingest.Server
		driver  *captureDriver
		baseURL string
		client  *http.Client
	)

	BeforeEach(func() {
		server, driver, baseURL = newTestServer()
		client = &http.Client{Timeout: 5 * time.Second}
	})

	AfterEach(func() {
		Expect(server.Close()).To(Succeed())
	})

	Describe("GET /ping", func() {
		It("returns ok", func() {
			resp, err := client.Get(baseURL + "/ping")
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			body, _ := io.ReadAll(resp.Body)
			Expect(string(body)).To(ContainSubstring("ok"))
		})
	})

	Describe("GET /metrics", func() {
		It("returns a scrapeable Prometheus body", func() {
			resp, err := client.Get(baseURL + "/metrics")
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("text/plain"))
			body, _ := io.ReadAll(resp.Body)
			// Gauges always render; counters only render once a label tuple
			// has been observed. Queue depth is the gauge, and it proves the
			// endpoint is wired to our registry.
			Expect(string(body)).To(ContainSubstring("tapes_ingest_worker_queue_depth"))
		})

		It("increments writes_total{status=accepted} on a valid turn", func() {
			payload := ingest.TurnPayload{
				Provider: "ollama",
				RawRequest: mustJSON(ollamaRequest{
					Model:    "llama3",
					Messages: []ollamaMessage{{Role: "user", Content: "Hello"}},
				}),
				Response: reducedResponse("llama3", "Hi", nil),
			}
			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			scrape, err := client.Get(baseURL + "/metrics")
			Expect(err).NotTo(HaveOccurred())
			defer scrape.Body.Close()
			txt, _ := io.ReadAll(scrape.Body)
			Expect(string(txt)).To(ContainSubstring(`tapes_ingest_writes_total{provider="ollama",status="accepted"}`))
		})

		It("increments writes_total{status=reject_envelope} on malformed JSON", func() {
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader([]byte(`{bad`)))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))

			scrape, err := client.Get(baseURL + "/metrics")
			Expect(err).NotTo(HaveOccurred())
			defer scrape.Body.Close()
			txt, _ := io.ReadAll(scrape.Body)
			Expect(string(txt)).To(ContainSubstring(`status="reject_envelope"`))
		})

		It("increments writes_total{status=unknown_provider} on unsupported provider", func() {
			payload := ingest.TurnPayload{
				Provider:   "bogus-provider",
				RawRequest: json.RawMessage(`{}`),
				Response:   reducedResponse("", "ok", nil),
			}
			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			scrape, err := client.Get(baseURL + "/metrics")
			Expect(err).NotTo(HaveOccurred())
			defer scrape.Body.Close()
			txt, _ := io.ReadAll(scrape.Body)
			Expect(string(txt)).To(ContainSubstring(`status="unknown_provider"`))
		})
	})

	Describe("POST /v1/ingest", func() {
		It("accepts a valid ollama turn and captures it into the raw layer", func() {
			payload := ingest.TurnPayload{
				Provider:  "ollama",
				AgentName: "test-agent",
				RawRequest: mustJSON(ollamaRequest{
					Model: "llama3",
					Messages: []ollamaMessage{
						{Role: "user", Content: "Hello"},
					},
				}),
				Response: reducedResponse("llama3", "Hi there!", nil),
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			respBody, _ := io.ReadAll(resp.Body)
			Expect(string(respBody)).To(ContainSubstring("accepted"))

			// The raw-turn row lands synchronously on the accepted path; the
			// deriver projects sessions/traces/spans from it. The node DAG is
			// retired, so there is no node store to assert against.
			Eventually(driver.CountRaw).
				WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
				Should(Equal(1))
			Expect(driver.RawTurns()[0].Provider).To(Equal("ollama"))
		})

		It("accepts a valid openai turn", func() {
			payload := ingest.TurnPayload{
				Provider:  "openai",
				AgentName: "codex",
				RawRequest: mustJSON(openaiRequest{
					Model: "gpt-4",
					Messages: []openaiMessage{
						{Role: "user", Content: "Explain Go interfaces"},
					},
				}),
				Response: reducedResponse("gpt-4", "In Go, an interface...", &llm.Usage{
					PromptTokens:     10,
					CompletionTokens: 20,
					TotalTokens:      30,
				}),
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
		})

		It("rejects an unsupported provider", func() {
			payload := ingest.TurnPayload{
				Provider:   "unknown-provider",
				RawRequest: json.RawMessage(`{}`),
				Response:   reducedResponse("", "ok", nil),
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
			respBody, _ := io.ReadAll(resp.Body)
			Expect(string(respBody)).To(ContainSubstring("unsupported provider"))
		})

		It("rejects a payload with unparseable raw request JSON", func() {
			// Manually construct a payload where "request" is not valid JSON.
			// We build the outer envelope by hand to embed a broken inner value.
			payload := `{"provider":"openai","request":"not-valid-json-object","response":{}}`

			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader([]byte(payload)))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			// A well-formed envelope wrapping an unparseable inner request
			// is 422 (unprocessable) rather than 400 (bad envelope).
			Expect(resp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
			respBody, _ := io.ReadAll(resp.Body)
			Expect(string(respBody)).To(ContainSubstring("cannot parse request"))
		})

		It("rejects malformed JSON", func() {
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader([]byte(`{bad`)))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		})
	})

	Describe("POST /v1/ingest/batch", func() {
		It("accepts multiple valid turns", func() {
			payload := ingest.BatchPayload{
				Turns: []ingest.TurnPayload{
					{
						Provider:  "ollama",
						AgentName: "agent-1",
						RawRequest: mustJSON(ollamaRequest{
							Model:    "llama3",
							Messages: []ollamaMessage{{Role: "user", Content: "First"}},
						}),
						Response: reducedResponse("llama3", "Response 1", nil),
					},
					{
						Provider:  "ollama",
						AgentName: "agent-2",
						RawRequest: mustJSON(ollamaRequest{
							Model:    "llama3",
							Messages: []ollamaMessage{{Role: "user", Content: "Second"}},
						}),
						Response: reducedResponse("llama3", "Response 2", nil),
					},
				},
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest/batch", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			var result ingest.BatchResult
			Expect(json.NewDecoder(resp.Body).Decode(&result)).To(Succeed())
			Expect(result.Accepted).To(Equal(2))
			Expect(result.Rejected).To(Equal(0))
			Expect(result.Errors).To(BeEmpty())
		})

		It("reports partial failures in a batch", func() {
			payload := ingest.BatchPayload{
				Turns: []ingest.TurnPayload{
					{
						Provider: "ollama",
						RawRequest: mustJSON(ollamaRequest{
							Model:    "llama3",
							Messages: []ollamaMessage{{Role: "user", Content: "Valid"}},
						}),
						Response: reducedResponse("llama3", "OK", nil),
					},
					{
						Provider:   "bad-provider",
						RawRequest: json.RawMessage(`{}`),
						Response:   reducedResponse("", "ok", nil),
					},
				},
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest/batch", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			var result ingest.BatchResult
			Expect(json.NewDecoder(resp.Body).Decode(&result)).To(Succeed())
			Expect(result.Accepted).To(Equal(1))
			Expect(result.Rejected).To(Equal(1))
			Expect(result.Errors).To(HaveLen(1))
			Expect(result.Errors[0]).To(ContainSubstring("unsupported provider"))
		})

		It("rejects an empty batch", func() {
			payload := ingest.BatchPayload{Turns: []ingest.TurnPayload{}}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest/batch", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		})

		It("emits Prometheus metrics for both accepted and rejected turns", func() {
			payload := ingest.BatchPayload{
				Turns: []ingest.TurnPayload{
					{
						Provider: "ollama",
						RawRequest: mustJSON(ollamaRequest{
							Model:    "llama3",
							Messages: []ollamaMessage{{Role: "user", Content: "hi"}},
						}),
						Response: reducedResponse("llama3", "ok", nil),
					},
					{
						Provider:   "bad-provider",
						RawRequest: json.RawMessage(`{}`),
						Response:   reducedResponse("", "ok", nil),
					},
				},
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest/batch", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			resp.Body.Close()

			scrape, err := client.Get(baseURL + "/metrics")
			Expect(err).NotTo(HaveOccurred())
			defer scrape.Body.Close()
			txt, _ := io.ReadAll(scrape.Body)
			Expect(string(txt)).To(ContainSubstring(`tapes_ingest_writes_total{provider="ollama",status="accepted"}`))
			Expect(string(txt)).To(ContainSubstring(`status="unknown_provider"`))
		})
	})
})

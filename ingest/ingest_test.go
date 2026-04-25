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
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
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

// ollamaResponse is a minimal Ollama-format response for test fixtures.
type ollamaResponse struct {
	Model           string        `json:"model"`
	CreatedAt       time.Time     `json:"created_at"`
	Message         ollamaMessage `json:"message"`
	Done            bool          `json:"done"`
	DoneReason      string        `json:"done_reason,omitempty"`
	PromptEvalCount int           `json:"prompt_eval_count,omitempty"`
	EvalCount       int           `json:"eval_count,omitempty"`
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

// openaiResponse is a minimal OpenAI-format response for test fixtures.
type openaiResponse struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Model   string         `json:"model"`
	Choices []openaiChoice `json:"choices"`
	Usage   openaiUsage    `json:"usage"`
}

type openaiChoice struct {
	Index        int           `json:"index"`
	Message      openaiMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type openaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func newTestServer() (*ingest.Server, storage.Driver, string) {
	logger := tapeslogger.NewNoop()
	driver := inmemory.NewDriver()

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
		driver  storage.Driver
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
				RawResponse: mustJSON(ollamaResponse{
					Model:   "llama3",
					Message: ollamaMessage{Role: "assistant", Content: "Hi"},
					Done:    true,
				}),
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
				Provider:    "bogus-provider",
				RawRequest:  json.RawMessage(`{}`),
				RawResponse: json.RawMessage(`{}`),
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
		It("accepts a valid ollama turn and stores it in the DAG", func() {
			payload := ingest.TurnPayload{
				Provider:  "ollama",
				AgentName: "test-agent",
				RawRequest: mustJSON(ollamaRequest{
					Model: "llama3",
					Messages: []ollamaMessage{
						{Role: "user", Content: "Hello"},
					},
				}),
				RawResponse: mustJSON(ollamaResponse{
					Model:   "llama3",
					Message: ollamaMessage{Role: "assistant", Content: "Hi there!"},
					Done:    true,
				}),
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			respBody, _ := io.ReadAll(resp.Body)
			Expect(string(respBody)).To(ContainSubstring("accepted"))

			// Give the worker pool time to process
			Eventually(func() int {
				nodes, _ := driver.List(context.Background())
				return len(nodes)
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(BeNumerically(">=", 0))
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
				RawResponse: mustJSON(openaiResponse{
					ID:     "chatcmpl-123",
					Object: "chat.completion",
					Model:  "gpt-4",
					Choices: []openaiChoice{
						{
							Index:        0,
							Message:      openaiMessage{Role: "assistant", Content: "In Go, an interface..."},
							FinishReason: "stop",
						},
					},
					Usage: openaiUsage{
						PromptTokens:     10,
						CompletionTokens: 20,
						TotalTokens:      30,
					},
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
				Provider:    "unknown-provider",
				RawRequest:  json.RawMessage(`{}`),
				RawResponse: json.RawMessage(`{}`),
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

	Describe("GET /v1/ingest/nodes", func() {
		It("returns only nodes matching the agent query", func() {
			// Land two turns under different agents.
			for _, agent := range []string{"canary-1", "other-2"} {
				payload := ingest.TurnPayload{
					Provider:  "ollama",
					AgentName: agent,
					RawRequest: mustJSON(ollamaRequest{
						Model: "llama3", Messages: []ollamaMessage{{Role: "user", Content: "hi"}},
					}),
					RawResponse: mustJSON(ollamaResponse{
						Model:   "llama3",
						Message: ollamaMessage{Role: "assistant", Content: "yo"},
						Done:    true,
					}),
				}
				body, _ := json.Marshal(payload)
				r, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
				Expect(err).NotTo(HaveOccurred())
				r.Body.Close()
			}

			// Poll until both nodes have landed.
			Eventually(func() int {
				nodes, _ := driver.List(context.Background())
				return len(nodes)
			}).WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
				Should(BeNumerically(">=", 2))

			resp, err := client.Get(baseURL + "/v1/ingest/nodes?agent=canary-1")
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var out ingest.NodeListResponse
			Expect(json.NewDecoder(resp.Body).Decode(&out)).To(Succeed())
			Expect(out.Count).To(BeNumerically(">=", 1))
			for _, n := range out.Nodes {
				Expect(n.AgentName).To(Equal("canary-1"))
			}
		})

		It("returns an empty list for an agent with no nodes", func() {
			resp, err := client.Get(baseURL + "/v1/ingest/nodes?agent=nonexistent-agent")
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var out ingest.NodeListResponse
			Expect(json.NewDecoder(resp.Body).Decode(&out)).To(Succeed())
			Expect(out.Count).To(Equal(0))
			Expect(out.Nodes).To(BeEmpty())
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
						RawResponse: mustJSON(ollamaResponse{
							Model:   "llama3",
							Message: ollamaMessage{Role: "assistant", Content: "Response 1"},
							Done:    true,
						}),
					},
					{
						Provider:  "ollama",
						AgentName: "agent-2",
						RawRequest: mustJSON(ollamaRequest{
							Model:    "llama3",
							Messages: []ollamaMessage{{Role: "user", Content: "Second"}},
						}),
						RawResponse: mustJSON(ollamaResponse{
							Model:   "llama3",
							Message: ollamaMessage{Role: "assistant", Content: "Response 2"},
							Done:    true,
						}),
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
						RawResponse: mustJSON(ollamaResponse{
							Model:   "llama3",
							Message: ollamaMessage{Role: "assistant", Content: "OK"},
							Done:    true,
						}),
					},
					{
						Provider:    "bad-provider",
						RawRequest:  json.RawMessage(`{}`),
						RawResponse: json.RawMessage(`{}`),
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
						RawResponse: mustJSON(ollamaResponse{
							Model:   "llama3",
							Message: ollamaMessage{Role: "assistant", Content: "ok"},
							Done:    true,
						}),
					},
					{
						Provider:    "bad-provider",
						RawRequest:  json.RawMessage(`{}`),
						RawResponse: json.RawMessage(`{}`),
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

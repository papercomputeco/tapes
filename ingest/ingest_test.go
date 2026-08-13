package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
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

		It("exposes tapes_ingest_body_bytes buckets topping out at 64 MiB", func() {
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

			// 64<<20 renders as 6.7108864e+07; the top finite bucket must sit
			// there so a near-limit envelope doesn't just saturate into +Inf.
			Expect(string(txt)).To(ContainSubstring(`tapes_ingest_body_bytes_bucket{provider="ollama",le="6.7108864e+07"}`))
			Expect(string(txt)).To(ContainSubstring("# HELP tapes_ingest_body_bytes Size of accepted ingest envelopes by provider."))

			// 14 mirrors the bucket count in metrics.go's ExponentialBucketsRange.
			finite := 0
			for line := range strings.SplitSeq(string(txt), "\n") {
				if strings.HasPrefix(line, "tapes_ingest_body_bytes_bucket{") && !strings.Contains(line, `le="+Inf"`) {
					finite++
				}
			}
			Expect(finite).To(Equal(14))
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

		It("stores a client-asserted envelope org under the single-tenant sentinel", func() {
			// The deployment settles the org, not the caller: an envelope
			// naming its own org must not create rows the nil-scoped read
			// side will never surface.
			payload := ingest.TurnPayload{
				Provider:  "ollama",
				AgentName: "test-agent",
				Session: &sessions.IngestEnvelope{
					OrgID:            "9f9f9f9f-9f9f-4f9f-8f9f-9f9f9f9f9f9f",
					HarnessID:        "claude-code",
					HarnessSessionID: "b6e7c3f2-0000-4000-8000-000000000001",
				},
				RawRequest: mustJSON(ollamaRequest{
					Model:    "llama3",
					Messages: []ollamaMessage{{Role: "user", Content: "Hello"}},
				}),
				Response: reducedResponse("llama3", "Hi!", nil),
			}

			body, _ := json.Marshal(payload)
			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			Eventually(driver.CountRaw).
				WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
				Should(Equal(1))
			Expect(driver.RawTurns()[0].OrgID).To(BeEmpty(),
				"the asserted org must be canonicalized to the sentinel, not stored")
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

		It("stores the verbatim request/response/meta sub-fields after the single-pass parse", func() {
			// Distinct sub-fields, no RawResponse: the Response is stored as
			// sent, not server-reduced.
			wantRequest := mustJSON(ollamaRequest{
				Model:    "llama3",
				Messages: []ollamaMessage{{Role: "user", Content: "single-pass request"}},
			})
			response := reducedResponse("llama3", "single-pass response", &llm.Usage{
				PromptTokens: 3, CompletionTokens: 5, TotalTokens: 8,
			})
			meta := ingest.TurnMeta{RequestID: "req-single-pass", ContentType: "application/json"}
			session := &sessions.IngestEnvelope{
				HarnessID:        "claude-code",
				HarnessSessionID: "b6e7c3f2-0000-4000-8000-0000000000aa",
			}

			body, err := json.Marshal(ingest.TurnPayload{
				Provider:   "ollama",
				AgentName:  "test-agent",
				RawRequest: wantRequest,
				Response:   response,
				Meta:       meta,
				Session:    session,
			})
			Expect(err).NotTo(HaveOccurred())

			resp, err := client.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(http.StatusAccepted))

			Eventually(driver.CountRaw).
				WithTimeout(2 * time.Second).WithPolling(25 * time.Millisecond).
				Should(Equal(1))

			// Each sub-field must land byte-faithfully to what the wire carried.
			rec := driver.RawTurns()[0]
			Expect(string(rec.RawRequest)).To(Equal(string(wantRequest)))
			Expect(string(rec.Response)).To(Equal(string(mustJSON(response))))
			Expect(string(rec.Meta)).To(Equal(string(mustJSON(meta))))
			Expect(string(rec.SessionEnvelope)).To(Equal(string(mustJSON(session))))
		})
	})
})

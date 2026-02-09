package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/agenttrace"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("Agent Trace Handlers", func() {
	var (
		server          *Server
		agentTraceStore *inmemory.AgentTraceStore
	)

	BeforeEach(func() {
		logger, _ := zap.NewDevelopment()
		inMem := inmemory.NewDriver()
		agentTraceStore = inmemory.NewAgentTraceStore()
		var err error
		server, err = NewServer(Config{ListenAddr: ":0"}, inMem, inMem, agentTraceStore, logger)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("POST /v1/agent-traces", func() {
		It("creates a valid agent trace and returns 201", func() {
			trace := agenttrace.AgentTrace{
				Version:   "0.1.0",
				ID:        "550e8400-e29b-41d4-a716-446655440000",
				Timestamp: "2026-01-23T14:30:00Z",
				Files: []agenttrace.File{
					{
						Path: "src/app.ts",
						Conversations: []agenttrace.Conversation{
							{
								Contributor: &agenttrace.Contributor{Type: "ai"},
								Ranges: []agenttrace.Range{
									{StartLine: 1, EndLine: 50},
								},
							},
						},
					},
				},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusCreated))

			var result agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &result)).To(Succeed())
			Expect(result.ID).To(Equal("550e8400-e29b-41d4-a716-446655440000"))
			Expect(result.Version).To(Equal("0.1.0"))
			Expect(result.Files).To(HaveLen(1))
			Expect(result.Files[0].Path).To(Equal("src/app.ts"))
		})

		It("creates a trace with VCS and Tool fields", func() {
			trace := agenttrace.AgentTrace{
				Version:   "0.1.0",
				ID:        "trace-with-extras",
				Timestamp: "2026-01-23T14:30:00Z",
				VCS:       &agenttrace.VCS{Type: "git", Revision: "abc123"},
				Tool:      &agenttrace.Tool{Name: "claude-code", Version: "1.0.0"},
				Files: []agenttrace.File{
					{Path: "main.go"},
				},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusCreated))

			var result agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &result)).To(Succeed())
			Expect(result.VCS).NotTo(BeNil())
			Expect(result.VCS.Type).To(Equal("git"))
			Expect(result.VCS.Revision).To(Equal("abc123"))
			Expect(result.Tool).NotTo(BeNil())
			Expect(result.Tool.Name).To(Equal("claude-code"))
		})

		It("returns 400 when version is missing", func() {
			trace := map[string]any{
				"id":        "test-id",
				"timestamp": "2026-01-23T14:30:00Z",
				"files":     []any{map[string]any{"path": "test.go"}},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("version is required"))
		})

		It("returns 400 when id is missing", func() {
			trace := map[string]any{
				"version":   "0.1.0",
				"timestamp": "2026-01-23T14:30:00Z",
				"files":     []any{map[string]any{"path": "test.go"}},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("id is required"))
		})

		It("returns 400 when timestamp is missing", func() {
			trace := map[string]any{
				"version": "0.1.0",
				"id":      "test-id",
				"files":   []any{map[string]any{"path": "test.go"}},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("timestamp is required"))
		})

		It("returns 400 when files is empty", func() {
			trace := map[string]any{
				"version":   "0.1.0",
				"id":        "test-id",
				"timestamp": "2026-01-23T14:30:00Z",
				"files":     []any{},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("at least one file is required"))
		})

		It("returns 400 when file path is empty", func() {
			trace := map[string]any{
				"version":   "0.1.0",
				"id":        "test-id",
				"timestamp": "2026-01-23T14:30:00Z",
				"files":     []any{map[string]any{"path": ""}},
			}

			body, err := json.Marshal(trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("file path is required"))
		})

		It("returns 400 for invalid JSON body", func() {
			req, err := http.NewRequest(http.MethodPost, "/v1/agent-traces", bytes.NewReader([]byte("not json")))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/json")

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
		})
	})

	Describe("GET /v1/agent-traces/:id", func() {
		It("returns the trace when it exists", func() {
			trace := &agenttrace.AgentTrace{
				Version:   "0.1.0",
				ID:        "get-test-id",
				Timestamp: "2026-01-23T14:30:00Z",
				Files: []agenttrace.File{
					{Path: "test.go"},
				},
			}
			_, err := agentTraceStore.CreateAgentTrace(context.Background(), trace)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces/get-test-id", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var result agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &result)).To(Succeed())
			Expect(result.ID).To(Equal("get-test-id"))
			Expect(result.Files).To(HaveLen(1))
		})

		It("returns 404 when trace does not exist", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces/nonexistent", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("agent trace not found"))
		})
	})

	Describe("GET /v1/agent-traces", func() {
		BeforeEach(func() {
			traces := []*agenttrace.AgentTrace{
				{
					Version:   "0.1.0",
					ID:        "trace-1",
					Timestamp: "2026-01-23T14:30:00Z",
					VCS:       &agenttrace.VCS{Revision: "abc123"},
					Tool:      &agenttrace.Tool{Name: "claude-code"},
					Files:     []agenttrace.File{{Path: "src/app.ts"}},
				},
				{
					Version:   "0.1.0",
					ID:        "trace-2",
					Timestamp: "2026-01-24T14:30:00Z",
					VCS:       &agenttrace.VCS{Revision: "def456"},
					Tool:      &agenttrace.Tool{Name: "copilot"},
					Files:     []agenttrace.File{{Path: "src/main.go"}},
				},
				{
					Version:   "0.1.0",
					ID:        "trace-3",
					Timestamp: "2026-01-25T14:30:00Z",
					VCS:       &agenttrace.VCS{Revision: "abc123"},
					Tool:      &agenttrace.Tool{Name: "claude-code"},
					Files:     []agenttrace.File{{Path: "src/app.ts"}},
				},
			}
			for _, t := range traces {
				_, err := agentTraceStore.CreateAgentTrace(context.Background(), t)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("returns all traces when no filters", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var results []*agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &results)).To(Succeed())
			Expect(results).To(HaveLen(3))
		})

		It("filters by file_path", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces?file_path=src/app.ts", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var results []*agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &results)).To(Succeed())
			Expect(results).To(HaveLen(2))
		})

		It("filters by revision", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces?revision=abc123", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var results []*agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &results)).To(Succeed())
			Expect(results).To(HaveLen(2))
		})

		It("filters by tool_name", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces?tool_name=copilot", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var results []*agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &results)).To(Succeed())
			Expect(results).To(HaveLen(1))
			Expect(results[0].ID).To(Equal("trace-2"))
		})

		It("applies limit", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces?limit=2", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var results []*agenttrace.AgentTrace
			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(respBody, &results)).To(Succeed())
			Expect(results).To(HaveLen(2))
		})

		It("returns 400 for invalid limit", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces?limit=abc", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("limit must be a non-negative integer"))
		})

		It("returns 400 for invalid offset", func() {
			req, err := http.NewRequest(http.MethodGet, "/v1/agent-traces?offset=xyz", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))

			respBody, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(respBody)).To(ContainSubstring("offset must be a non-negative integer"))
		})
	})
})

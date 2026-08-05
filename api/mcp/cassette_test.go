package mcp_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/api/mcp"
	"github.com/papercomputeco/tapes/pkg/cassette"
)

type requestHeaderTransport struct{ base http.RoundTripper }

func (transport requestHeaderTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	cloned.Header.Set("Authorization", "Bearer cassette-test")
	cloned.Header.Set("Accept-Encoding", "gzip")
	cloned.Header.Set("If-None-Match", `"stale"`)
	cloned.Header.Set("X-Forwarded-For", "spoofed")
	return transport.base.RoundTrip(cloned)
}

var _ = Describe("Cassette MCP tools", func() {
	It("lists and invokes the current registry without core search", func(ctx SpecContext) {
		var received struct {
			path           string
			authorization  string
			cassette       string
			acceptEncoding string
			ifNoneMatch    string
			forwardedFor   string
			body           map[string]any
		}
		upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			received.path = request.URL.Path
			received.authorization = request.Header.Get("Authorization")
			received.cassette = request.Header.Get("X-Tapes-Cassette")
			received.acceptEncoding = request.Header.Get("Accept-Encoding")
			received.ifNoneMatch = request.Header.Get("If-None-Match")
			received.forwardedFor = request.Header.Get("X-Forwarded-For")
			received.body = nil
			Expect(json.NewDecoder(request.Body).Decode(&received.body)).To(Succeed())
			writer.Header().Set("Content-Type", "application/json")
			writer.Header().Set("Content-Encoding", "gzip")
			compressed := gzip.NewWriter(writer)
			_, _ = io.WriteString(compressed, `{"summary":"done"}`)
			Expect(compressed.Close()).To(Succeed())
		}))
		DeferCleanup(upstream.Close)

		registry := cassetterunner.NewRegistry()
		server, err := mcp.NewServer(mcp.Config{Cassettes: registry})
		Expect(err).NotTo(HaveOccurred())
		httpServer := httptest.NewServer(server.Handler())
		DeferCleanup(httpServer.Close)

		client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil)
		session, err := client.Connect(ctx, &mcpsdk.StreamableClientTransport{
			Endpoint: httpServer.URL,
			HTTPClient: &http.Client{Transport: requestHeaderTransport{
				base: http.DefaultTransport,
			}},
		}, nil)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(session.Close)

		Expect(registry.Put(&cassetterunner.Instance{
			Name:    "summary",
			URL:     upstream.URL,
			Anchors: cassette.Anchors{Prefix: "api"},
			MCPTools: []cassetterunner.MCPTool{{
				Name:        "summary.summarize_session",
				Title:       "Summarize a session",
				Description: "Creates a concise summary.",
				Method:      http.MethodPost,
				Path:        "/v1/cassettes/summary/summarize",
				InputSchema: map[string]any{
					"type": "object",
					"$ref": "#/$defs/Input",
					"$defs": map[string]any{"Input": map[string]any{
						"type":       "object",
						"properties": map[string]any{"session_id": map[string]any{"type": "string"}},
						"required":   []string{"session_id"},
					}},
				},
			}, {
				Name: "summary.optional", Method: http.MethodPost,
				Path:        "/v1/cassettes/summary/summarize",
				InputSchema: map[string]any{"type": "object"},
			}},
		})).To(Succeed())

		names := make([]string, 0, 2)
		for tool, listErr := range session.Tools(ctx, nil) {
			Expect(listErr).NotTo(HaveOccurred())
			names = append(names, tool.Name)
		}
		Expect(names).To(ConsistOf("summary.summarize_session", "summary.optional"))

		result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "summary.summarize_session",
			Arguments: map[string]any{"session_id": "session-1"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeFalse())
		Expect(result.StructuredContent).To(Equal(map[string]any{"summary": "done"}))
		Expect(received.path).To(Equal("/api/summary/summarize"))
		Expect(received.authorization).To(Equal("Bearer cassette-test"))
		Expect(received.cassette).To(Equal("summary"))
		Expect(received.acceptEncoding).To(Equal("gzip"))
		Expect(received.ifNoneMatch).To(BeEmpty())
		Expect(received.forwardedFor).NotTo(Equal("spoofed"))
		Expect(received.body).To(HaveKeyWithValue("session_id", "session-1"))

		result, err = session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "summary.optional"})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.IsError).To(BeFalse())
		Expect(received.body).To(BeEmpty())
		Expect(received.body).NotTo(BeNil())
	})

	It("validates arguments before invoking a cassette", func(ctx SpecContext) {
		calls := 0
		upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
		DeferCleanup(upstream.Close)
		registry := cassetterunner.NewRegistry()
		Expect(registry.Put(&cassetterunner.Instance{
			Name: "summary", URL: upstream.URL, Anchors: cassette.Anchors{Prefix: "api"},
			MCPTools: []cassetterunner.MCPTool{{
				Name: "summary.run", Method: http.MethodPost, Path: "/v1/cassettes/summary/run",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"id": map[string]any{"type": "string"}},
					"required":   []string{"id"},
				},
			}},
		})).To(Succeed())
		server, err := mcp.NewServer(mcp.Config{Cassettes: registry})
		Expect(err).NotTo(HaveOccurred())
		httpServer := httptest.NewServer(server.Handler())
		DeferCleanup(httpServer.Close)
		session, err := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil).Connect(
			ctx, &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(session.Close)

		_, err = session.CallTool(context.Background(), &mcpsdk.CallToolParams{
			Name: "summary.run", Arguments: map[string]any{},
		})
		Expect(err).To(HaveOccurred())
		Expect(calls).To(BeZero())
	})
})

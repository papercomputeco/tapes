package mcp_test

import (
	"net/http"
	"net/http/httptest"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/api/mcp"
	"github.com/papercomputeco/tapes/pkg/cassette"
)

var _ = Describe("Cassette tool errors", func() {
	It("surfaces cassette error bodies as MCP tool errors", func(ctx SpecContext) {
		// The stub answers the shape an attach-style mutation returns when a
		// supplied name is unknown: a 404 whose JSON body lists the unknown
		// names with near-match suggestions. The bridge's standard non-2xx
		// rule must deliver that body to the MCP caller as a tool error —
		// swallowing it into a generic bridge message would strand the
		// calling agent with no way to correct the call.
		const errorBody = `{"error":"unknown flavors","unknown":[{"name":"swete","near_matches":["sweet"]}]}`
		upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusNotFound)
			_, _ = writer.Write([]byte(errorBody))
		}))
		DeferCleanup(upstream.Close)

		registry := cassetterunner.NewRegistry()
		Expect(registry.Put(&cassetterunner.Instance{
			Name:    "notes",
			URL:     upstream.URL,
			Anchors: cassette.Anchors{Prefix: "api"},
			MCPTools: []cassetterunner.MCPTool{{
				Name:   "notes.add_note",
				Method: http.MethodPost,
				Path:   "/v1/cassettes/notes/add",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"session_id": map[string]any{"type": "string"},
						"name":       map[string]any{"type": "string"},
					},
					"required": []string{"session_id", "name"},
				},
			}},
		})).To(Succeed())

		server, err := mcp.NewServer(mcp.Config{Cassettes: registry})
		Expect(err).NotTo(HaveOccurred())
		httpServer := httptest.NewServer(server.Handler())
		DeferCleanup(httpServer.Close)

		session, err := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test", Version: "1"}, nil).
			Connect(ctx, &mcpsdk.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(session.Close)

		result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "notes.add_note",
			Arguments: map[string]any{"session_id": "s-1", "name": "swete"},
		})
		Expect(err).NotTo(HaveOccurred(),
			"a failing tool call is a tool error result, not a protocol failure")
		Expect(result.IsError).To(BeTrue())

		var joined strings.Builder
		for _, content := range result.Content {
			if block, ok := content.(*mcpsdk.TextContent); ok {
				joined.WriteString(block.Text)
			}
		}
		text := joined.String()
		Expect(text).To(ContainSubstring("404"),
			"the cassette's status must travel with the error")
		Expect(text).To(ContainSubstring(`"near_matches":["sweet"]`),
			"the cassette's error body must arrive verbatim, not as a generic bridge message")
		Expect(text).To(ContainSubstring("swete"),
			"the unknown name the caller sent must be reflected back")
	})
})

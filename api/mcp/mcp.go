// Package mcp provides an MCP (Model Context Protocol) server for the Tapes system.
package mcp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/utils"
)

// SpanSearcher performs vector-similarity search over span embeddings.
type SpanSearcher interface {
	Search(ctx context.Context, orgID string, embedding []float32, topK int) ([]spanembed.Hit, error)
}

type Config struct {
	// SpanSearcher and Embedder optionally retain the legacy core search tool
	// while search moves to a cassette. They must be supplied together.
	SpanSearcher SpanSearcher
	Embedder     embeddings.Embedder

	// Cassettes is the live registry whose advertised tools are exposed.
	Cassettes *cassetterunner.Registry

	// Client invokes admitted cassette operations. Nil uses a redirect-refusing client.
	Client *http.Client

	Logger *slog.Logger
}

type Server struct {
	config  Config
	client  *http.Client
	handler *mcpsdk.StreamableHTTPHandler
}

// NewServer creates a stateless MCP server over the current cassette registry.
func NewServer(config Config) (*Server, error) {
	if (config.SpanSearcher == nil) != (config.Embedder == nil) {
		return nil, errors.New("span searcher and embedder must be configured together")
	}
	if config.SpanSearcher != nil && config.Logger == nil {
		return nil, errors.New("logger is required when core search is configured")
	}

	client := config.Client
	if client == nil {
		client = cassetterunner.NewHTTPClient()
	}
	s := &Server{config: config, client: client}
	s.handler = mcpsdk.NewStreamableHTTPHandler(s.serverForRequest, &mcpsdk.StreamableHTTPOptions{Stateless: true})
	return s, nil
}

// serverForRequest takes a fresh snapshot because the HTTP transport is
// stateless and cassettes can be admitted, refreshed, or removed at runtime.
func (s *Server) serverForRequest(request *http.Request) *mcpsdk.Server {
	server := mcpsdk.NewServer(
		&mcpsdk.Implementation{Name: "tapes", Version: utils.Version},
		&mcpsdk.ServerOptions{
			Logger: s.config.Logger,
			Capabilities: &mcpsdk.ServerCapabilities{
				Tools: &mcpsdk.ToolCapabilities{ListChanged: false},
			},
		},
	)

	if s.config.SpanSearcher != nil {
		mcpsdk.AddTool(server, &mcpsdk.Tool{
			Name:        searchToolName,
			Description: searchDescription,
		}, s.handleSearch)
	}
	if s.config.Cassettes == nil {
		return server
	}

	for _, instance := range s.config.Cassettes.Instances() {
		for _, advertised := range instance.MCPTools {
			mcpsdk.AddTool[map[string]any, any](server, &mcpsdk.Tool{
				Name:        advertised.Name,
				Title:       advertised.Title,
				Description: advertised.Description,
				InputSchema: advertised.InputSchema,
				Annotations: &mcpsdk.ToolAnnotations{
					DestructiveHint: advertised.Annotations.DestructiveHint,
					IdempotentHint:  advertised.Annotations.IdempotentHint,
					OpenWorldHint:   advertised.Annotations.OpenWorldHint,
					ReadOnlyHint:    advertised.Annotations.ReadOnlyHint,
				},
			}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, input map[string]any) (*mcpsdk.CallToolResult, any, error) {
				output, err := s.callCassette(ctx, request, instance, advertised, input)
				return nil, output, err
			})
		}
	}
	return server
}

// Handler returns the HTTP handler for the MCP server.
func (s *Server) Handler() http.Handler { return s.handler }

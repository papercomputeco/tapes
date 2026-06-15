// Package mcp provides an MCP (Model Context Protocol) server for the Tapes system.
package mcp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/utils"
)

// SpanSearcher performs vector-similarity search over span embeddings.
// *spanembed.Store implements it; it is the exact same search path the
// HTTP GET /v1/search/spans handler uses. Defined here (rather than
// imported from package api) so the MCP server reuses the span search
// without creating an import cycle with package api.
type SpanSearcher interface {
	Search(ctx context.Context, orgID string, embedding []float32, topK int) ([]spanembed.Hit, error)
}

type Config struct {
	// SpanSearcher runs semantic search over the span-embedding
	// projection (main llm spans, delta-only content) — the same search
	// the HTTP /v1/search/spans surface uses.
	SpanSearcher SpanSearcher

	// Embedder converts the query text to a vector for SpanSearcher.
	Embedder embeddings.Embedder

	// Noop for empty MCP server
	Noop bool

	// Logger is the configured logger
	Logger *slog.Logger
}

type Server struct {
	config    Config
	mcpServer *mcp.Server
	handler   *mcp.StreamableHTTPHandler
}

// NewServer creates a new MCP server with the search tool.
func NewServer(c Config) (*Server, error) {
	s := &Server{
		config: c,
	}

	// Create the MCP server
	mcpServer := mcp.NewServer(
		&mcp.Implementation{
			Name:    "tapes",
			Version: utils.Version,
		},
		&mcp.ServerOptions{},
	)

	if c.Noop {
		// return the empty MCP server with no tools configured
		// if the noop flag is set (i.e., MCP capabilities are disabled)
		s.mcpServer = mcpServer
		return s, nil
	}

	if c.SpanSearcher == nil {
		return nil, errors.New("span searcher is required")
	}
	if c.Embedder == nil {
		return nil, errors.New("embedder is required")
	}
	if c.Logger == nil {
		return nil, errors.New("logger is required")
	}

	// Add tools
	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        searchToolName,
		Description: searchDescription,
	}, s.handleSearch)

	s.mcpServer = mcpServer

	// Create a streamable HTTP net/http handler for stateless operations
	s.handler = mcp.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcp.Server {
			return mcpServer
		},
		&mcp.StreamableHTTPOptions{
			Stateless: true,
		},
	)

	return s, nil
}

// Handler returns the HTTP handler for the MCP server.
func (s *Server) Handler() http.Handler {
	return s.handler
}

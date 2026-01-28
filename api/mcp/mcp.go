// Package mcp provides an MCP (Model Context Protocol) server for the Tapes system.
package mcp

import (
	"fmt"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/utils"
	"github.com/papercomputeco/tapes/pkg/vector"
)

type Config struct {
	// Storage driver for looking up full node ancestry
	StorageDriver storage.Driver

	// VectorDriver for semantic search
	VectorDriver vector.VectorDriver

	// Embedder for converting query text to vectors for semantic search with
	// configured VectorDriver
	Embedder embeddings.Embedder

	// Noop for empty MCP server
	Noop bool

	// Logger is the configured zap logger
	Logger *zap.Logger
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

	if c.StorageDriver == nil {
		return nil, fmt.Errorf("storage driver is required")
	}
	if c.VectorDriver == nil {
		return nil, fmt.Errorf("vector driver is required")
	}
	if c.Embedder == nil {
		return nil, fmt.Errorf("embedder is required")
	}
	if c.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Add tools
	mcp.AddTool(mcpServer, &mcp.Tool{
		Name:        searchToolName,
		Description: searchDescription,
	}, s.handleSearch)

	s.mcpServer = mcpServer

	// Create a streamable HTTP net/http handler for stateless operations
	s.handler = mcp.NewStreamableHTTPHandler(
		func(r *http.Request) *mcp.Server {
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

// Package api provides an HTTP API server for inspecting and managing the Merkle DAG.
package api

import (
	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/vector"
)

// Config is the API server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8081")
	ListenAddr string

	// VectorDriver for semantic search (optional, enables MCP server)
	VectorDriver vector.VectorDriver

	// Embedder for converting query text to vectors (optional, enables MCP server)
	Embedder embeddings.Embedder
}

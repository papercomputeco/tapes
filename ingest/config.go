// Package ingest provides an HTTP server that accepts completed LLM conversation
// turns for storage in the Merkle DAG. This enables "sidecar mode" where an
// external gateway (e.g., Envoy AI Gateway) handles upstream LLM traffic and
// tapes only stores, embeds, and publishes the data.
package ingest

import (
	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/publisher"
	"github.com/papercomputeco/tapes/pkg/vector"
)

// Config is the ingest server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8082")
	ListenAddr string

	// VectorDriver is an optional vector store for storing embeddings.
	// If nil, vector storage is disabled.
	VectorDriver vector.Driver

	// Embedder is an optional embedder for generating embeddings.
	// Required if VectorDriver is set.
	Embedder embeddings.Embedder

	// Publisher is an optional event publisher for new DAG nodes.
	// If nil, publishing is disabled.
	Publisher publisher.Publisher

	// Project is the git repository or project name to tag on stored nodes.
	Project string
}

// Package ingest provides an HTTP server that accepts completed LLM conversation
// turns for storage in the Merkle DAG. This enables "sidecar mode" where an
// external gateway (e.g., Envoy AI Gateway) handles upstream LLM traffic and
// tapes only stores and publishes the data. Embeddings are written
// downstream by the derive worker family (pkg/spanembed), never at
// ingest time.
package ingest

import (
	"github.com/papercomputeco/tapes/pkg/publisher"
)

// Config is the ingest server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8082")
	ListenAddr string

	// Publisher is an optional event publisher for new DAG nodes.
	// If nil, publishing is disabled.
	Publisher publisher.Publisher

	// Project is the git repository or project name to tag on stored nodes.
	Project string
}

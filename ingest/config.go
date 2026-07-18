// Package ingest provides an HTTP server that accepts completed LLM conversation
// turns and appends them to the immutable raw-turn capture log. This enables
// "sidecar mode" where an external gateway (e.g., Envoy AI Gateway) handles
// upstream LLM traffic and tapes only captures the turns for the deriver.
// Embeddings are written downstream by the derive worker family
// (pkg/spanembed), never at ingest time.
package ingest

// Config is the ingest server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8082")
	ListenAddr string

	// Project is the git repository or project name to tag on captured turns.
	Project string
}

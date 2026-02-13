package proxy

import (
	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/vector"
)

// Config is the proxy server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8080")
	ListenAddr string

	// UpstreamURL is the upstream LLM provider URL (e.g., "http://localhost:11434")
	UpstreamURL string

	// ProviderType specifies the LLM provider type (e.g., "anthropic", "openai", "ollama")
	// This determines how requests and responses are parsed.
	ProviderType string

	// VectorDriver is an optional vector store for storing embeddings.
	// If nil, vector storage is disabled.
	VectorDriver vector.Driver

	// Embedder is an optional embedder for generating embeddings.
	// Required if VectorDriver is set.
	Embedder embeddings.Embedder

	// Project is the git repository or project name to tag on stored nodes.
	Project string
}

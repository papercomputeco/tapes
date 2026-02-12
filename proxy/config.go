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

	// AgentRoutes maps agent names to provider routing configuration.
	AgentRoutes map[string]AgentRoute

	// VectorDriver is an optional vector store for storing embeddings.
	// If nil, vector storage is disabled.
	VectorDriver vector.Driver

	// Embedder is an optional embedder for generating embeddings.
	// Required if VectorDriver is set.
	Embedder embeddings.Embedder
}

// AgentRoute defines proxy routing for a specific agent.
type AgentRoute struct {
	ProviderType string
	UpstreamURL  string
}

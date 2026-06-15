package proxy

import (
	"github.com/papercomputeco/tapes/pkg/publisher"
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

	// ProviderUpstreams optionally overrides upstream URLs per provider.
	ProviderUpstreams map[string]string

	// Publisher is an optional event publisher for new DAG nodes.
	// If nil, publishing is disabled.
	Publisher publisher.Publisher

	// Project is the git repository or project name to tag on stored nodes.
	Project string
}

// AgentRoute defines proxy routing for a specific agent.
type AgentRoute struct {
	ProviderType string
	UpstreamURL  string
}

package proxy

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

	// Project is the git repository or project name to tag on captured turns.
	Project string

	// QueueByteBudget bounds the total retained request bytes of
	// queued-and-in-flight capture jobs. Zero selects the worker pool's
	// default. Exposed so the byte budget can be tuned (and exercised in
	// tests) rather than hard-wired to the default.
	QueueByteBudget int64
}

// AgentRoute defines proxy routing for a specific agent.
type AgentRoute struct {
	ProviderType string
	UpstreamURL  string
}

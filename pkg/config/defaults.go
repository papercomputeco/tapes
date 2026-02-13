package config

const (
	defaultProvider    = "ollama"
	defaultUpstream    = "http://localhost:11434"
	defaultProxyListen = ":8080"
	defaultAPIListen   = ":8081"

	defaultClientProxyTarget = "http://localhost:8080"
	defaultClientAPITarget   = "http://localhost:8081"

	defaultVectorProvider = "sqlite"

	defaultEmbeddingModel      = "embeddinggemma"
	defaultEmbeddingDimensions = 768
	defaultEmbeddingTarget     = "http://localhost:11434"

	defaultMemoryProvider = "local"
)

// NewDefaultConfig returns a Config with sane defaults for all fields.
// This is the single source of truth for default values.
func NewDefaultConfig() *Config {
	return &Config{
		Version: CurrentV,
		Proxy: ProxyConfig{
			Provider: defaultProvider,
			Upstream: defaultUpstream,
			Listen:   defaultProxyListen,
		},
		API: APIConfig{
			Listen: defaultAPIListen,
		},
		Client: ClientConfig{
			ProxyTarget: defaultClientProxyTarget,
			APITarget:   defaultClientAPITarget,
		},
		VectorStore: VectorStoreConfig{
			Provider: defaultVectorProvider,
		},
		Embedding: EmbeddingConfig{
			Provider:   defaultProvider,
			Target:     defaultUpstream,
			Model:      defaultEmbeddingModel,
			Dimensions: defaultEmbeddingDimensions,
		},
		Memory: MemoryConfig{
			Provider: defaultMemoryProvider,
			Enabled:  true,
		},
	}
}

package config

// Config represents the persistent tapes configuration stored as config.toml
// in the .tapes/ directory. The TOML layout uses sections for logical grouping.
type Config struct {
	Version     int               `toml:"version"       mapstructure:"version"`
	Storage     StorageConfig     `toml:"storage"       mapstructure:"storage"`
	Proxy       ProxyConfig       `toml:"proxy"         mapstructure:"proxy"`
	API         APIConfig         `toml:"api"           mapstructure:"api"`
	Client      ClientConfig      `toml:"client"        mapstructure:"client"`
	VectorStore VectorStoreConfig `toml:"vector_store"  mapstructure:"vector_store"`
	Embedding   EmbeddingConfig   `toml:"embedding"     mapstructure:"embedding"`
}

// StorageConfig holds shared storage settings used by both proxy and API.
type StorageConfig struct {
	SQLitePath string `toml:"sqlite_path,omitempty" mapstructure:"sqlite_path"`
}

// ProxyConfig holds proxy-specific settings.
type ProxyConfig struct {
	Provider string `toml:"provider,omitempty" mapstructure:"provider"`
	Upstream string `toml:"upstream,omitempty" mapstructure:"upstream"`
	Listen   string `toml:"listen,omitempty"   mapstructure:"listen"`
}

// APIConfig holds API server settings.
type APIConfig struct {
	Listen string `toml:"listen,omitempty" mapstructure:"listen"`
}

// ClientConfig holds settings for CLI commands that connect to the running
// proxy and API servers (e.g. tapes chat, tapes search, tapes checkout).
// Values are full URLs (scheme + host + port).
type ClientConfig struct {
	ProxyTarget string `toml:"proxy_target,omitempty" mapstructure:"proxy_target"`
	APITarget   string `toml:"api_target,omitempty"   mapstructure:"api_target"`
}

// VectorStoreConfig holds vector store settings.
type VectorStoreConfig struct {
	Provider string `toml:"provider,omitempty" mapstructure:"provider"`
	Target   string `toml:"target,omitempty"   mapstructure:"target"`
}

// EmbeddingConfig holds embedding provider settings.
type EmbeddingConfig struct {
	Provider   string `toml:"provider,omitempty"   mapstructure:"provider"`
	Target     string `toml:"target,omitempty"     mapstructure:"target"`
	Model      string `toml:"model,omitempty"      mapstructure:"model"`
	Dimensions uint   `toml:"dimensions,omitempty" mapstructure:"dimensions"`
}

// validConfigKeys is the authoritative set of all supported config keys.
// Keys use dotted notation matching the TOML section structure.
var validConfigKeys = map[string]bool{
	"storage.sqlite_path":   true,
	"proxy.provider":        true,
	"proxy.upstream":        true,
	"proxy.listen":          true,
	"api.listen":            true,
	"client.proxy_target":   true,
	"client.api_target":     true,
	"vector_store.provider": true,
	"vector_store.target":   true,
	"embedding.provider":    true,
	"embedding.target":      true,
	"embedding.model":       true,
	"embedding.dimensions":  true,
}

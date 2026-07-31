package config

// Config represents the persistent tapes configuration stored as config.toml
// in the .tapes/ directory. The TOML layout uses sections for logical grouping.
type Config struct {
	Version     int               `toml:"version"       mapstructure:"version"`
	Storage     StorageConfig     `toml:"storage"       mapstructure:"storage"`
	Proxy       ProxyConfig       `toml:"proxy"         mapstructure:"proxy"`
	API         APIConfig         `toml:"api"           mapstructure:"api"`
	Ingest      IngestConfig      `toml:"ingest"        mapstructure:"ingest"`
	Client      ClientConfig      `toml:"client"        mapstructure:"client"`
	VectorStore VectorStoreConfig `toml:"vector_store"  mapstructure:"vector_store"`
	Embedding   EmbeddingConfig   `toml:"embedding"     mapstructure:"embedding"`
	OpenCode    OpenCodeConfig    `toml:"opencode"      mapstructure:"opencode"`
	Logging     LoggingConfig     `toml:"logging"       mapstructure:"logging"`
	Telemetry   TelemetryConfig   `toml:"telemetry"     mapstructure:"telemetry"`
	Update      UpdateConfig      `toml:"update"        mapstructure:"update"`
	// Cassettes contains exact OpenAPI document URLs for externally managed cassettes.
	Cassettes []string `toml:"cassettes" mapstructure:"cassettes"`
}

// StorageConfig holds shared storage settings used by both proxy and API.
type StorageConfig struct {
	// PostgresDSN selects the hosted, multi-player store when set.
	PostgresDSN string `toml:"postgres_dsn,omitempty" mapstructure:"postgres_dsn"`
	// SQLitePath is the local single-player core database. Empty resolves to
	// <config-dir>/core.sqlite for the combined serve/start commands.
	SQLitePath string `toml:"sqlite_path,omitempty" mapstructure:"sqlite_path"`
}

// ProxyConfig holds proxy-specific settings.
type ProxyConfig struct {
	Provider string `toml:"provider,omitempty" mapstructure:"provider"`
	Upstream string `toml:"upstream,omitempty" mapstructure:"upstream"`
	Listen   string `toml:"listen,omitempty"   mapstructure:"listen"`
	Project  string `toml:"project,omitempty"  mapstructure:"project"`
}

// APIConfig holds API server settings.
type APIConfig struct {
	Listen string `toml:"listen,omitempty" mapstructure:"listen"`
	WebUI  bool   `toml:"web_ui,omitempty" mapstructure:"web_ui"`
}

// IngestConfig holds ingest server settings for sidecar mode.
type IngestConfig struct {
	Listen string `toml:"listen,omitempty" mapstructure:"listen"`
}

// ClientConfig holds settings for CLI commands that connect to the running
// proxy and API servers (e.g. tapes search, tapes export).
// Values are full URLs (scheme + host + port).
type ClientConfig struct {
	ProxyTarget string `toml:"proxy_target,omitempty" mapstructure:"proxy_target"`
	APITarget   string `toml:"api_target,omitempty"   mapstructure:"api_target"`
}

// VectorStoreConfig holds vector store settings.
type VectorStoreConfig struct {
	Target string `toml:"target,omitempty"   mapstructure:"target"`
}

// EmbeddingConfig holds embedding provider settings.
type EmbeddingConfig struct {
	Provider   string `toml:"provider,omitempty"   mapstructure:"provider"`
	Target     string `toml:"target,omitempty"     mapstructure:"target"`
	Model      string `toml:"model,omitempty"      mapstructure:"model"`
	Dimensions uint   `toml:"dimensions,omitempty" mapstructure:"dimensions"`
}

// OpenCodeConfig holds OpenCode agent settings for provider and model selection.
type OpenCodeConfig struct {
	Provider string `toml:"provider,omitempty" mapstructure:"provider"`
	Model    string `toml:"model,omitempty"    mapstructure:"model"`
}

// LoggingConfig holds process-wide logging settings.
type LoggingConfig struct {
	Level  string `toml:"level,omitempty"  mapstructure:"level"`
	Format string `toml:"format,omitempty" mapstructure:"format"`
	Color  string `toml:"color,omitempty"  mapstructure:"color"`
}

// TelemetryConfig holds anonymous telemetry settings.
type TelemetryConfig struct {
	Disabled bool `toml:"disabled,omitempty" mapstructure:"disabled"`
}

// UpdateConfig holds update-check settings.
type UpdateConfig struct {
	Disabled bool `toml:"disabled,omitempty" mapstructure:"disabled"`
}

// configKeySet is the authoritative set of all supported user-facing config keys.
// Keys use dotted notation matching the TOML section structure.
var configKeySet = map[string]bool{
	"proxy.provider":        true,
	"proxy.upstream":        true,
	"proxy.listen":          true,
	"proxy.project":         true,
	"api.listen":            true,
	"api.web_ui":            true,
	"ingest.listen":         true,
	"client.proxy_target":   true,
	"client.api_target":     true,
	"vector_store.provider": true,
	"vector_store.target":   true,
	"embedding.provider":    true,
	"embedding.target":      true,
	"embedding.model":       true,
	"embedding.dimensions":  true,
	"opencode.provider":     true,
	"opencode.model":        true,

	"logging.level":  true,
	"logging.format": true,
	"logging.color":  true,

	"storage.postgres_dsn": true,
	"storage.sqlite_path":  true,

	"telemetry.disabled": true,

	"update.disabled": true,
}

package config

import "time"

const (
	defaultProvider     = "ollama"
	defaultUpstream     = "http://localhost:11434"
	defaultProxyListen  = ":8080"
	defaultAPIListen    = ":8081"
	defaultIngestListen = ":8082"

	defaultClientProxyTarget = "http://localhost:8080"
	defaultClientAPITarget   = "http://localhost:8081"

	defaultLogLevel  = "info"
	defaultLogFormat = "auto"
	defaultLogColor  = "auto"

	// The API read guards. The deadline sits under the gateway's 30 s so a
	// read the gateway has given up on is cancelled server-side rather than
	// left running under the retry; the cap is per replica.
	defaultAPIReadDeadline       = 20 * time.Second
	defaultAPIPayloadConcurrency = 4
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
			Listen:             defaultAPIListen,
			WebUI:              false,
			ReadDeadline:       defaultAPIReadDeadline,
			PayloadConcurrency: defaultAPIPayloadConcurrency,
		},
		Ingest: IngestConfig{
			Listen: defaultIngestListen,
		},
		Client: ClientConfig{
			ProxyTarget: defaultClientProxyTarget,
			APITarget:   defaultClientAPITarget,
		},
		Logging: LoggingConfig{
			Level:  defaultLogLevel,
			Format: defaultLogFormat,
			Color:  defaultLogColor,
		},
	}
}

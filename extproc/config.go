package main

import (
	"log/slog"
	"os"
)

// Config holds configuration for the tapes-extproc adapter.
type Config struct {
	// IngestURL is the base URL for the tapes ingest server.
	IngestURL string
	// ListenAddr is the gRPC TCP listen address.
	ListenAddr string
	// MetricsAddr is the HTTP metrics/health listen address.
	MetricsAddr string
	// ProviderMapFile is the path to the provider mapping YAML file.
	ProviderMapFile string
	// MaxInflight is the dispatch semaphore capacity.
	MaxInflight int
	// RawResponseMode selects whether the dispatch envelope carries the
	// verbatim upstream response bytes, the adapter's reduction, or both.
	// Zero value is RawResponseOff, so a Config built by a test or an
	// older caller keeps the historical wire shape.
	RawResponseMode RawResponseMode
}

// ConfigFromEnv reads configuration from environment variables.
func ConfigFromEnv() Config {
	// An unparseable mode falls back to off and says so. The raw lane is
	// additive — off is always a safe, working configuration — so a bad
	// value should not stop the process from serving traffic.
	rawMode, err := ParseRawResponseMode(os.Getenv("TAPES_RAW_RESPONSE_MODE"))
	if err != nil {
		slog.Error("invalid TAPES_RAW_RESPONSE_MODE, falling back to off",
			"error", err,
			"mode", RawResponseOff,
		)
	}

	cfg := Config{
		IngestURL:       envOrDefault("TAPES_INGEST_URL", "http://tapes-ingest:8090"),
		ListenAddr:      envOrDefault("TAPES_LISTEN_ADDR", "0.0.0.0:50051"),
		MetricsAddr:     envOrDefault("TAPES_METRICS_ADDR", "0.0.0.0:9090"),
		ProviderMapFile: envOrDefault("TAPES_PROVIDER_MAP_FILE", ""),
		MaxInflight:     100,
		RawResponseMode: rawMode,
	}
	return cfg
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

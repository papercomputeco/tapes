package extproc

import (
	"log/slog"
	"os"
	"strconv"

	"google.golang.org/grpc"
)

// Defaults for the numeric env-configured capacities. The recv limit must
// cover the 32 MB Anthropic Messages contract plus gRPC framing and coalescing
// headroom: the upstream aigw filter buffers, so a body arrives as one message.
const (
	defaultGRPCMaxRecvBytes = 64 << 20
	defaultMaxInflight      = 100
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
	// GRPCMaxRecvBytes is the gRPC server's maximum receive message size.
	GRPCMaxRecvBytes int
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
		IngestURL:        envOrDefault("TAPES_INGEST_URL", "http://tapes-ingest:8090"),
		ListenAddr:       envOrDefault("TAPES_LISTEN_ADDR", "0.0.0.0:50051"),
		MetricsAddr:      envOrDefault("TAPES_METRICS_ADDR", "0.0.0.0:9090"),
		ProviderMapFile:  envOrDefault("TAPES_PROVIDER_MAP_FILE", ""),
		MaxInflight:      int(envIntOrDefault("TAPES_MAX_INFLIGHT_DISPATCHES", defaultMaxInflight)),
		GRPCMaxRecvBytes: int(envIntOrDefault("TAPES_GRPC_MAX_RECV_BYTES", defaultGRPCMaxRecvBytes)),
		RawResponseMode:  rawMode,
	}
	return cfg
}

// GRPCServerOptions builds the server options main wires into grpc.NewServer;
// the recv limit default is justified at defaultGRPCMaxRecvBytes.
func GRPCServerOptions(cfg Config) []grpc.ServerOption {
	return []grpc.ServerOption{grpc.MaxRecvMsgSize(cfg.GRPCMaxRecvBytes)}
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// envIntOrDefault reads a positive integer environment variable. Every
// consumer is a capacity that must stay positive, so an unparseable or
// non-positive value falls back to the default and says so.
func envIntOrDefault(key string, defaultVal int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		slog.Error("invalid integer env value, falling back to default",
			"error", err,
			"key", key,
			"value", v,
			"default", defaultVal,
		)
		return defaultVal
	}
	if n <= 0 {
		slog.Error("invalid integer env value, falling back to default",
			"key", key,
			"value", v,
			"default", defaultVal,
		)
		return defaultVal
	}
	return n
}

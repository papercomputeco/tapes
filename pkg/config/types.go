package config

import (
	"fmt"
	"strconv"
)

// Config represents the persistent tapes configuration stored as config.toml
// in the .tapes/ directory. The TOML layout uses sections for logical grouping.
type Config struct {
	Version     int               `toml:"version"`
	Storage     StorageConfig     `toml:"storage"`
	Proxy       ProxyConfig       `toml:"proxy"`
	API         APIConfig         `toml:"api"`
	Client      ClientConfig      `toml:"client"`
	VectorStore VectorStoreConfig `toml:"vector_store"`
	Embedding   EmbeddingConfig   `toml:"embedding"`
	Memory      MemoryConfig      `toml:"memory"`
}

// StorageConfig holds shared storage settings used by both proxy and API.
type StorageConfig struct {
	SQLitePath string `toml:"sqlite_path,omitempty"`
}

// ProxyConfig holds proxy-specific settings.
type ProxyConfig struct {
	Provider string `toml:"provider,omitempty"`
	Upstream string `toml:"upstream,omitempty"`
	Listen   string `toml:"listen,omitempty"`
}

// APIConfig holds API server settings.
type APIConfig struct {
	Listen string `toml:"listen,omitempty"`
}

// ClientConfig holds settings for CLI commands that connect to the running
// proxy and API servers (e.g. tapes chat, tapes search, tapes checkout).
// Values are full URLs (scheme + host + port).
type ClientConfig struct {
	ProxyTarget string `toml:"proxy_target,omitempty"`
	APITarget   string `toml:"api_target,omitempty"`
}

// VectorStoreConfig holds vector store settings.
type VectorStoreConfig struct {
	Provider string `toml:"provider,omitempty"`
	Target   string `toml:"target,omitempty"`
}

// EmbeddingConfig holds embedding provider settings.
type EmbeddingConfig struct {
	Provider   string `toml:"provider,omitempty"`
	Target     string `toml:"target,omitempty"`
	Model      string `toml:"model,omitempty"`
	Dimensions uint   `toml:"dimensions,omitempty"`
}

// MemoryConfig holds memory layer settings.
type MemoryConfig struct {
	Provider string `toml:"provider,omitempty"`
	Enabled  bool   `toml:"enabled,omitempty"`
}

// configKeyInfo maps a user-facing dotted key name to a getter and setter on *Config.
type configKeyInfo struct {
	get func(c *Config) string
	set func(c *Config, v string) error
}

// configKeys is the authoritative map of all supported config keys.
// Keys use dotted notation matching the TOML section structure.
var configKeys = map[string]configKeyInfo{
	"storage.sqlite_path": {
		get: func(c *Config) string { return c.Storage.SQLitePath },
		set: func(c *Config, v string) error { c.Storage.SQLitePath = v; return nil },
	},
	"proxy.provider": {
		get: func(c *Config) string { return c.Proxy.Provider },
		set: func(c *Config, v string) error { c.Proxy.Provider = v; return nil },
	},
	"proxy.upstream": {
		get: func(c *Config) string { return c.Proxy.Upstream },
		set: func(c *Config, v string) error { c.Proxy.Upstream = v; return nil },
	},
	"proxy.listen": {
		get: func(c *Config) string { return c.Proxy.Listen },
		set: func(c *Config, v string) error { c.Proxy.Listen = v; return nil },
	},
	"api.listen": {
		get: func(c *Config) string { return c.API.Listen },
		set: func(c *Config, v string) error { c.API.Listen = v; return nil },
	},
	"client.proxy_target": {
		get: func(c *Config) string { return c.Client.ProxyTarget },
		set: func(c *Config, v string) error { c.Client.ProxyTarget = v; return nil },
	},
	"client.api_target": {
		get: func(c *Config) string { return c.Client.APITarget },
		set: func(c *Config, v string) error { c.Client.APITarget = v; return nil },
	},
	"vector_store.provider": {
		get: func(c *Config) string { return c.VectorStore.Provider },
		set: func(c *Config, v string) error { c.VectorStore.Provider = v; return nil },
	},
	"vector_store.target": {
		get: func(c *Config) string { return c.VectorStore.Target },
		set: func(c *Config, v string) error { c.VectorStore.Target = v; return nil },
	},
	"embedding.provider": {
		get: func(c *Config) string { return c.Embedding.Provider },
		set: func(c *Config, v string) error { c.Embedding.Provider = v; return nil },
	},
	"embedding.target": {
		get: func(c *Config) string { return c.Embedding.Target },
		set: func(c *Config, v string) error { c.Embedding.Target = v; return nil },
	},
	"embedding.model": {
		get: func(c *Config) string { return c.Embedding.Model },
		set: func(c *Config, v string) error { c.Embedding.Model = v; return nil },
	},
	"embedding.dimensions": {
		get: func(c *Config) string {
			if c.Embedding.Dimensions == 0 {
				return ""
			}
			return strconv.FormatUint(uint64(c.Embedding.Dimensions), 10)
		},
		set: func(c *Config, v string) error {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid value for embedding.dimensions: %w", err)
			}
			c.Embedding.Dimensions = uint(n)
			return nil
		},
	},
	"memory.provider": {
		get: func(c *Config) string { return c.Memory.Provider },
		set: func(c *Config, v string) error { c.Memory.Provider = v; return nil },
	},
	"memory.enabled": {
		get: func(c *Config) string { return strconv.FormatBool(c.Memory.Enabled) },
		set: func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("invalid value for memory.enabled: %w", err)
			}
			c.Memory.Enabled = b
			return nil
		},
	},
}

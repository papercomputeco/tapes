package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

// InitViper creates and returns a configured *viper.Viper.
// It sets defaults from NewDefaultConfig(), reads the config.toml file
// (if found via dotdir resolution), and binds environment variables
// with the TAPES_ prefix.
//
// Config precedence (highest to lowest):
//  1. CLI flags (once bound via BindRegisteredFlags)
//  2. Environment variables (TAPES_PROXY_LISTEN, TAPES_API_LISTEN, etc.)
//  3. config.toml file values
//  4. Defaults from NewDefaultConfig()
func InitViper(configDir string) (*viper.Viper, error) {
	v := viper.New()

	// 1. Register all defaults from NewDefaultConfig().
	setViperDefaults(v)

	// 2. Config file discovery via dotdir resolution.
	v.SetConfigName("config")
	v.SetConfigType("toml")

	ddm := dotdir.NewManager()
	target, err := ddm.Target(configDir)
	if err != nil {
		return nil, fmt.Errorf("resolving config dir: %w", err)
	}

	if target != "" {
		v.AddConfigPath(target)
	}

	if err := v.ReadInConfig(); err != nil {
		// Config file not found errors are fine, defaults will apply.
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	}

	// 3. Environment variables: TAPES_PROXY_LISTEN, TAPES_STORAGE_SQLITE_PATH, etc.
	v.SetEnvPrefix("TAPES")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	return v, nil
}

// setViperDefaults registers defaults from NewDefaultConfig() into viper
// using dotted-key notation. This keeps defaults.go as the single source of truth.
func setViperDefaults(v *viper.Viper) {
	d := NewDefaultConfig()

	v.SetDefault("version", d.Version)

	// Storage
	v.SetDefault("storage.sqlite_path", d.Storage.SQLitePath)

	// Proxy
	v.SetDefault("proxy.provider", d.Proxy.Provider)
	v.SetDefault("proxy.upstream", d.Proxy.Upstream)
	v.SetDefault("proxy.listen", d.Proxy.Listen)

	// API
	v.SetDefault("api.listen", d.API.Listen)

	// Client
	v.SetDefault("client.proxy_target", d.Client.ProxyTarget)
	v.SetDefault("client.api_target", d.Client.APITarget)

	// Vector store
	v.SetDefault("vector_store.provider", d.VectorStore.Provider)
	v.SetDefault("vector_store.target", d.VectorStore.Target)

	// Embedding
	v.SetDefault("embedding.provider", d.Embedding.Provider)
	v.SetDefault("embedding.target", d.Embedding.Target)
	v.SetDefault("embedding.model", d.Embedding.Model)
	v.SetDefault("embedding.dimensions", d.Embedding.Dimensions)
}

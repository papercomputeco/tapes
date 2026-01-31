package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const (
	configFile = "config.toml"

	// v0 is the alpha version of the config
	v0 = 0

	// CurrentV is the currently supported version, points to v0
	CurrentV = v0
)

type Configer struct {
	ddm        *dotdir.Manager
	targetPath string
}

func NewConfiger(override string) (*Configer, error) {
	cfger := &Configer{}

	cfger.ddm = dotdir.NewManager()
	target, err := cfger.ddm.Target(override)
	if err != nil {
		return nil, err
	}

	// If no .tapes/ directory was resolved, targetPath stays empty;
	// LoadConfig will return defaults and SaveConfig will error clearly.
	if target == "" {
		return cfger, nil
	}

	path := filepath.Join(target, configFile)
	_, err = os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	// Always set targetPath when the directory exists so SaveConfig
	// can create or overwrite the file.
	cfger.targetPath = path

	return cfger, nil
}

// ValidConfigKeys returns the sorted list of all supported configuration key names.
func ValidConfigKeys() []string {
	keys := make([]string, 0, len(configKeys))
	for k := range configKeys {
		keys = append(keys, k)
	}

	// Return in a stable, logical order matching the TOML section layout.
	ordered := []string{
		"storage.sqlite_path",
		"proxy.provider",
		"proxy.upstream",
		"proxy.listen",
		"api.listen",
		"client.proxy_target",
		"client.api_target",
		"vector_store.provider",
		"vector_store.target",
		"embedding.provider",
		"embedding.target",
		"embedding.model",
		"embedding.dimensions",
	}

	// Sanity: only return keys that actually exist in the map.
	result := make([]string, 0, len(ordered))
	for _, k := range ordered {
		if _, ok := configKeys[k]; ok {
			result = append(result, k)
		}
	}

	// Append any keys in the map that we missed in the ordered list.
	seen := make(map[string]bool, len(result))
	for _, k := range result {
		seen[k] = true
	}
	for _, k := range keys {
		if !seen[k] {
			result = append(result, k)
		}
	}

	return result
}

// IsValidConfigKey returns true if the given key is a supported configuration key.
func IsValidConfigKey(key string) bool {
	_, ok := configKeys[key]
	return ok
}

func (c *Configer) GetTarget() string {
	return c.targetPath
}

// LoadConfig loads the configuration from config.toml in the target .tapes/ directory.
// If the file does not exist, returns DefaultConfig() so callers always receive
// a fully-populated Config with sane defaults. Fields explicitly set in the file
// override the defaults.
// If overrideDir is non-empty, it is used instead of the default .tapes/ location.
func (c *Configer) LoadConfig() (*Config, error) {
	if c.targetPath == "" {
		return NewDefaultConfig(), nil
	}

	data, err := os.ReadFile(c.targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return NewDefaultConfig(), nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	cfg, err := ParseConfigTOML(data)
	if err != nil {
		return nil, err
	}

	// Merge in defaults: fill in any zero-value fields from the loaded config
	applyDefaults(cfg)

	return cfg, nil
}

// applyDefaults fills zero-value fields in cfg with values from DefaultConfig().
func applyDefaults(cfg *Config) {
	defaults := NewDefaultConfig()

	if cfg.Version == 0 {
		cfg.Version = defaults.Version
	}

	if cfg.Proxy.Provider == "" {
		cfg.Proxy.Provider = defaults.Proxy.Provider
	}
	if cfg.Proxy.Upstream == "" {
		cfg.Proxy.Upstream = defaults.Proxy.Upstream
	}
	if cfg.Proxy.Listen == "" {
		cfg.Proxy.Listen = defaults.Proxy.Listen
	}

	if cfg.API.Listen == "" {
		cfg.API.Listen = defaults.API.Listen
	}

	if cfg.Client.ProxyTarget == "" {
		cfg.Client.ProxyTarget = defaults.Client.ProxyTarget
	}
	if cfg.Client.APITarget == "" {
		cfg.Client.APITarget = defaults.Client.APITarget
	}

	if cfg.VectorStore.Provider == "" {
		cfg.VectorStore.Provider = defaults.VectorStore.Provider
	}

	if cfg.Embedding.Provider == "" {
		cfg.Embedding.Provider = defaults.Embedding.Provider
	}
	if cfg.Embedding.Target == "" {
		cfg.Embedding.Target = defaults.Embedding.Target
	}
	if cfg.Embedding.Model == "" {
		cfg.Embedding.Model = defaults.Embedding.Model
	}
	if cfg.Embedding.Dimensions == 0 {
		cfg.Embedding.Dimensions = defaults.Embedding.Dimensions
	}
}

// SaveConfig persists the configuration to config.toml in the target .tapes/ directory.
func (c *Configer) SaveConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("cannot save nil config")
	}

	if c.targetPath == "" {
		return errors.New("cannot save empty target path")
	}

	var buf bytes.Buffer
	encoder := toml.NewEncoder(&buf)
	if err := encoder.Encode(cfg); err != nil {
		return fmt.Errorf("encoding config: %w", err)
	}

	if err := os.WriteFile(c.targetPath, buf.Bytes(), 0o600); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	return nil
}

// SetConfigValue loads the config, sets the given key to the given value, and saves it.
// Returns an error if the key is not a valid config key.
func (c *Configer) SetConfigValue(key string, value string) error {
	info, ok := configKeys[key]
	if !ok {
		return fmt.Errorf("unknown config key: %q", key)
	}

	cfg, err := c.LoadConfig()
	if err != nil {
		return err
	}

	if err := info.set(cfg, value); err != nil {
		return err
	}

	return c.SaveConfig(cfg)
}

// GetConfigValue loads the config and returns the string representation of the given key.
// Returns an error if the key is not a valid config key.
func (c *Configer) GetConfigValue(key string) (string, error) {
	info, ok := configKeys[key]
	if !ok {
		return "", fmt.Errorf("unknown config key: %q", key)
	}

	cfg, err := c.LoadConfig()
	if err != nil {
		return "", err
	}

	return info.get(cfg), nil
}

// PresetConfig returns a Config with sane defaults for the named provider preset.
// Supported presets: "openai", "anthropic", "ollama".
// Returns an error if the preset name is not recognized.
func PresetConfig(name string) (*Config, error) {
	switch strings.ToLower(name) {
	case "openai":
		return &Config{
			Version: CurrentV,
			Proxy: ProxyConfig{
				Provider: "openai",
				Upstream: "https://api.openai.com",
				Listen:   ":8080",
			},
			API: APIConfig{
				Listen: ":8081",
			},
			Client: ClientConfig{
				ProxyTarget: "http://localhost:8080",
				APITarget:   "http://localhost:8081",
			},
		}, nil

	case "anthropic":
		return &Config{
			Version: CurrentV,
			Proxy: ProxyConfig{
				Provider: "anthropic",
				Upstream: "https://api.anthropic.com",
				Listen:   ":8080",
			},
			API: APIConfig{
				Listen: ":8081",
			},
			Client: ClientConfig{
				ProxyTarget: "http://localhost:8080",
				APITarget:   "http://localhost:8081",
			},
		}, nil

	case "ollama":
		return &Config{
			Version: CurrentV,
			Proxy: ProxyConfig{
				Provider: "ollama",
				Upstream: "http://localhost:11434",
				Listen:   ":8080",
			},
			API: APIConfig{
				Listen: ":8081",
			},
			Client: ClientConfig{
				ProxyTarget: "http://localhost:8080",
				APITarget:   "http://localhost:8081",
			},
			Embedding: EmbeddingConfig{
				Provider:   "ollama",
				Target:     "http://localhost:11434",
				Model:      "nomic-embed-text",
				Dimensions: 768,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown preset: %q (available: openai, anthropic, ollama)", name)
	}
}

// ValidPresetNames returns the list of recognized preset names.
func ValidPresetNames() []string {
	return []string{"openai", "anthropic", "ollama"}
}

// ParseConfigTOML parses raw TOML bytes into a Config.
// Returns an error if the version field is present and not equal to CurrentConfigVersion.
func ParseConfigTOML(data []byte) (*Config, error) {
	cfg := &Config{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config TOML: %w", err)
	}

	if cfg.Version != 0 && cfg.Version != CurrentV {
		return nil, fmt.Errorf("unsupported config version %d (expected %d)", cfg.Version, CurrentV)
	}

	return cfg, nil
}

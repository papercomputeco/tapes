package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/viper"

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
		if validConfigKeys[k] {
			result = append(result, k)
		}
	}

	// Append any keys in the map that we missed in the ordered list.
	seen := make(map[string]bool, len(result))
	for _, k := range result {
		seen[k] = true
	}
	for k := range validConfigKeys {
		if !seen[k] {
			result = append(result, k)
		}
	}

	return result
}

// IsValidConfigKey returns true if the given key is a supported configuration key.
func IsValidConfigKey(key string) bool {
	return validConfigKeys[key]
}

func (c *Configer) GetTarget() string {
	return c.targetPath
}

// LoadConfig loads the configuration from config.toml in the target .tapes/ directory.
// If the file does not exist, returns DefaultConfig() so callers always receive
// a fully-populated Config with sane defaults. Fields explicitly set in the file
// override the defaults.
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

	// Use viper to merge defaults into the parsed config.
	// This replaces the old hand-rolled applyDefaults function.
	v := viper.New()
	setViperDefaults(v)
	v.SetConfigType("toml")

	if err := v.ReadConfig(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("reading config into viper: %w", err)
	}

	merged := &Config{}
	if err := v.Unmarshal(merged); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	// Preserve the version from the parsed config (version 0 is valid).
	merged.Version = cfg.Version

	return merged, nil
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
	if !validConfigKeys[key] {
		return fmt.Errorf("unknown config key: %q", key)
	}

	cfg, err := c.LoadConfig()
	if err != nil {
		return err
	}

	// Use viper to set the value and unmarshal back to the Config struct.
	// This handles type coercion (e.g., string to uint for embedding.dimensions).
	v := viper.New()
	setViperDefaults(v)
	v.SetConfigType("toml")

	// Load existing config into viper if the file exists.
	if c.targetPath != "" {
		data, err := os.ReadFile(c.targetPath)
		if err == nil {
			_ = v.ReadConfig(bytes.NewReader(data))
		}
	}

	v.Set(key, value)

	updated := &Config{}
	if err := v.Unmarshal(updated); err != nil {
		return fmt.Errorf("invalid value for %s: %w", key, err)
	}

	// Preserve the version from the loaded config.
	updated.Version = cfg.Version

	return c.SaveConfig(updated)
}

// GetConfigValue loads the config and returns the string representation of the given key.
// Returns an error if the key is not a valid config key.
func (c *Configer) GetConfigValue(key string) (string, error) {
	if !validConfigKeys[key] {
		return "", fmt.Errorf("unknown config key: %q", key)
	}

	v := viper.New()
	setViperDefaults(v)
	v.SetConfigType("toml")

	// Load existing config into viper if the file exists.
	if c.targetPath != "" {
		data, err := os.ReadFile(c.targetPath)
		if err == nil {
			_ = v.ReadConfig(bytes.NewReader(data))
		}
	}

	return v.GetString(key), nil
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

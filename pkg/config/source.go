package config

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Source names the configuration layer that provides a key's effective
// value, mirroring the precedence documented in docs/configuration.md.
type Source string

const (
	// SourceDefault means no higher layer sets the key; the value is the
	// built-in default from NewDefaultConfig.
	SourceDefault Source = "default"
	// SourceConfigFile means the value comes from config.toml.
	SourceConfigFile Source = "config file"
	// SourceEnvironment means the value comes from a TAPES_ environment variable.
	SourceEnvironment Source = "environment"
)

// EnvKeyForConfigKey maps a dotted config key to its TAPES_ environment
// variable, mirroring the AutomaticEnv binding used for value resolution.
func EnvKeyForConfigKey(key string) string {
	return "TAPES_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

// loadViper builds a viper instance with defaults registered, the config file
// (when present) loaded, and TAPES_ environment variables bound, applying the
// documented precedence: environment > config file > defaults.
func (c *Configer) loadViper() *viper.Viper {
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

	// Bind environment variables so TAPES_PROXY_LISTEN etc. are reflected.
	v.SetEnvPrefix("TAPES")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	return v
}

// GetConfigValueSource loads the config and returns the effective string value
// of the given key together with the layer that provided it. Returns an error
// if the key is not a valid config key.
func (c *Configer) GetConfigValueSource(key string) (string, Source, error) {
	if !configKeySet[key] {
		return "", "", fmt.Errorf("unknown config key: %q", key)
	}

	v := c.loadViper()
	value := v.GetString(key)

	if _, ok := os.LookupEnv(EnvKeyForConfigKey(key)); ok {
		return value, SourceEnvironment, nil
	}
	if v.InConfig(key) {
		return value, SourceConfigFile, nil
	}
	return value, SourceDefault, nil
}

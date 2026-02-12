package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Flag is the single source of truth for a CLI flag.
// Commands reference flags by registry key rather than hard-coding names,
// shorthands, defaults, and descriptions inline. This prevents flag drift
// when the same logical flag appears on multiple commands (e.g., --upstream
// on both "tapes serve" and "tapes serve proxy" and "tapes chat").
type Flag struct {
	// Name is the long flag name (e.g. "upstream").
	Name string

	// Shorthand is the one-letter short flag (e.g. "u"). Empty for no shorthand.
	Shorthand string

	// ViperKey is the dotted config key this flag maps to (e.g. "proxy.upstream").
	ViperKey string

	// Description is the help text shown in --help output.
	Description string
}

// FlagSet is a mapping of flag names to Flag structs that hold their name,
// shorthand, viper key, etc.
type FlagSet map[string]Flag

// Flag registry keys.
// Use these constants when calling AddStringFlag, AddUintFlag,
// and BindRegisteredFlags to avoid typos or drift from one command to another.
const (
	FlagProxyListen     = "proxy-listen"
	FlagAPIListen       = "api-listen"
	FlagUpstream        = "upstream"
	FlagProvider        = "provider"
	FlagSQLite          = "sqlite"
	FlagVectorStoreProv = "vector-store-provider"
	FlagVectorStoreTgt  = "vector-store-target"
	FlagEmbeddingProv   = "embedding-provider"
	FlagEmbeddingTgt    = "embedding-target"
	FlagEmbeddingModel  = "embedding-model"
	FlagEmbeddingDims   = "embedding-dimensions"
	FlagAPITarget       = "api-target"
	FlagProxyTarget     = "proxy-target"

	// Standalone subcommand variants use "listen" as the flag name
	// but bind to different viper keys depending on the service.
	FlagProxyListenStandalone = "proxy-listen-standalone"
	FlagAPIListenStandalone   = "api-listen-standalone"
)

// AddStringFlag registers a string flag on cmd from the given FlagSet.
// The flag's name, shorthand, default, and description all come from the
// FlagSet entry so they cannot drift across commands.
func AddStringFlag(cmd *cobra.Command, fs FlagSet, key string, target *string) {
	def, ok := fs[key]
	if !ok {
		return
	}

	defaultVal := defaultString(def.ViperKey)
	if def.Shorthand != "" {
		cmd.Flags().StringVarP(target, def.Name, def.Shorthand, defaultVal, def.Description)
	} else {
		cmd.Flags().StringVar(target, def.Name, defaultVal, def.Description)
	}
}

// AddUintFlag registers a uint flag on cmd from the given FlagSet.
func AddUintFlag(cmd *cobra.Command, fs FlagSet, registryKey string, target *uint) {
	def, ok := fs[registryKey]
	if !ok {
		return
	}

	defaultVal := defaultUint(def.ViperKey)
	if def.Shorthand != "" {
		cmd.Flags().UintVarP(target, def.Name, def.Shorthand, defaultVal, def.Description)
	} else {
		cmd.Flags().UintVar(target, def.Name, defaultVal, def.Description)
	}
}

// BindRegisteredFlags binds already-registered flags to viper using definitions
// from the given FlagSet. Call this in PreRunE after InitViper to connect flags
// to the viper precedence chain (flag > env > config file > default).
func BindRegisteredFlags(v *viper.Viper, cmd *cobra.Command, fs FlagSet, registryKeys []string) {
	for _, registryKey := range registryKeys {
		def, ok := fs[registryKey]
		if !ok {
			continue
		}

		f := cmd.Flags().Lookup(def.Name)
		if f == nil {
			continue
		}

		_ = v.BindPFlag(def.ViperKey, f)
	}
}

// defaultString returns the default string value for a viper key from NewDefaultConfig.
func defaultString(viperKey string) string {
	v := viper.New()
	setViperDefaults(v)
	return v.GetString(viperKey)
}

// defaultUint returns the default uint value for a viper key from NewDefaultConfig.
func defaultUint(viperKey string) uint {
	v := viper.New()
	setViperDefaults(v)
	return v.GetUint(viperKey)
}

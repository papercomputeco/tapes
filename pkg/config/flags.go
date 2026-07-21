package config

import (
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Flag is the single source of truth for a CLI flag.
// Commands reference flags by registry key rather than hard-coding names,
// shorthands, defaults, and descriptions inline. This prevents flag drift
// when the same logical flag appears on multiple commands (e.g., --upstream
// on both "tapes serve" and "tapes serve proxy").
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
// Use these constants when calling AddStringFlag, AddUintFlag, AddBoolFlag,
// and BindRegisteredFlags to avoid typos or drift from one command to another.
const (
	FlagProxyListen         = "proxy-listen"
	FlagAPIListen           = "api-listen"
	FlagAPIWebUI            = "api-web-ui"
	FlagUpstream            = "upstream"
	FlagProvider            = "provider"
	FlagPostgres            = "postgres"
	FlagProject             = "project"
	FlagVectorStoreTgt      = "vector-store-target"
	FlagEmbeddingProv       = "embedding-provider"
	FlagEmbeddingTgt        = "embedding-target"
	FlagEmbeddingModel      = "embedding-model"
	FlagEmbeddingDims       = "embedding-dimensions"
	FlagSkillModel          = "skill-model"
	FlagAPITarget           = "api-target"
	FlagProxyTarget         = "proxy-target"
	FlagTelemetryDisabled   = "telemetry-disabled"
	FlagUpdateCheckDisabled = "update-check-disabled"

	FlagIngestListen = "ingest-listen"

	// Standalone subcommand variants use "listen" as the flag name
	// but bind to different viper keys depending on the service.
	FlagProxyListenStandalone  = "proxy-listen-standalone"
	FlagAPIListenStandalone    = "api-listen-standalone"
	FlagIngestListenStandalone = "ingest-listen-standalone"

	// Derive worker (`tapes serve derive-worker`) tunables.
	FlagDeriveWorkerPoll          = "derive-worker-poll-interval"
	FlagDeriveWorkerDebounce      = "derive-worker-debounce"
	FlagDeriveWorkerSweep         = "derive-worker-sweep-interval"
	FlagDeriveWorkerSweepWindow   = "derive-worker-sweep-window" //nolint:gosec // flag registry key, not a credential
	FlagDeriveWorkerMaxDeriveLag  = "derive-worker-max-derive-lag"
	FlagDeriveWorkerMetricsListen = "derive-worker-metrics-listen"
	FlagDeriveWorkerWaitForDB     = "derive-worker-wait-for-db"

	// Embed worker (`tapes serve embed-worker`) tunables.
	FlagEmbedWorkerInterval      = "embed-worker-interval"
	FlagEmbedWorkerMetricsListen = "embed-worker-metrics-listen"
	FlagEmbedWorkerWaitForDB     = "embed-worker-wait-for-db"
	FlagEmbedWorkerBatchSize     = "embed-worker-batch-size"
	FlagEmbedWorkerMaxTextBytes  = "embed-worker-max-text-bytes"
	FlagEmbedWorkerOrg           = "embed-worker-org"
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

// AddBoolFlag registers a bool flag on cmd from the given FlagSet.
func AddBoolFlag(cmd *cobra.Command, fs FlagSet, registryKey string, target *bool) {
	def, ok := fs[registryKey]
	if !ok {
		return
	}

	defaultVal := defaultBool(def.ViperKey)
	if def.Shorthand != "" {
		cmd.Flags().BoolVarP(target, def.Name, def.Shorthand, defaultVal, def.Description)
	} else {
		cmd.Flags().BoolVar(target, def.Name, defaultVal, def.Description)
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

// AddIntFlag registers an int flag on cmd from the given FlagSet.
func AddIntFlag(cmd *cobra.Command, fs FlagSet, registryKey string, target *int) {
	def, ok := fs[registryKey]
	if !ok {
		return
	}

	defaultVal := defaultInt(def.ViperKey)
	if def.Shorthand != "" {
		cmd.Flags().IntVarP(target, def.Name, def.Shorthand, defaultVal, def.Description)
	} else {
		cmd.Flags().IntVar(target, def.Name, defaultVal, def.Description)
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

// IsRegisteredFlagExplicitlySet reports whether a registered flag's value came
// from a CLI flag, environment variable, or config file rather than defaults.
func IsRegisteredFlagExplicitlySet(v *viper.Viper, cmd *cobra.Command, fs FlagSet, registryKey string) bool {
	def, ok := fs[registryKey]
	if !ok {
		return false
	}

	if f := cmd.Flags().Lookup(def.Name); f != nil && f.Changed {
		return true
	}

	if ExplicitConfigKeySet(v, def.ViperKey) {
		return true
	}

	return false
}

// ExplicitConfigKeySet reports whether a config key was supplied through the
// environment or the loaded config file.
func ExplicitConfigKeySet(v *viper.Viper, key string) bool {
	envKey := "TAPES_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	if _, ok := os.LookupEnv(envKey); ok {
		return true
	}

	return v.InConfig(key)
}

// cachedDefaultViper is a lazily-initialized viper instance with all defaults
// from NewDefaultConfig(). Used by defaultString and defaultUint to avoid
// creating a new viper instance on every flag registration.
var (
	cachedDefaultViper     *viper.Viper
	cachedDefaultViperOnce sync.Once
)

func getDefaultViper() *viper.Viper {
	cachedDefaultViperOnce.Do(func() {
		cachedDefaultViper = viper.New()
		setViperDefaults(cachedDefaultViper)
	})
	return cachedDefaultViper
}

// defaultString returns the default string value for a viper key from NewDefaultConfig.
func defaultString(viperKey string) string {
	return getDefaultViper().GetString(viperKey)
}

// defaultUint returns the default uint value for a viper key from NewDefaultConfig.
func defaultUint(viperKey string) uint {
	return getDefaultViper().GetUint(viperKey)
}

// defaultInt returns the default int value for a viper key from NewDefaultConfig.
func defaultInt(viperKey string) int {
	return getDefaultViper().GetInt(viperKey)
}

// defaultBool returns the default bool value for a viper key from NewDefaultConfig.
func defaultBool(viperKey string) bool {
	return getDefaultViper().GetBool(viperKey)
}

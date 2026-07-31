// Package tapescmder
package tapescmder

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	authcmder "github.com/papercomputeco/tapes/cmd/tapes/auth"
	backfillcmder "github.com/papercomputeco/tapes/cmd/tapes/backfill"
	configcmder "github.com/papercomputeco/tapes/cmd/tapes/config"
	devcmder "github.com/papercomputeco/tapes/cmd/tapes/dev"
	initcmder "github.com/papercomputeco/tapes/cmd/tapes/init"
	localcmder "github.com/papercomputeco/tapes/cmd/tapes/local"
	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	statuscmder "github.com/papercomputeco/tapes/cmd/tapes/status"
	versioncmder "github.com/papercomputeco/tapes/cmd/tapes/version"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/telemetry"
	"github.com/papercomputeco/tapes/pkg/update"
	"github.com/papercomputeco/tapes/pkg/utils"
)

const tapesLongDesc string = `Tapes is automatic telemetry for your agents.

Tapes captures LLM calls into an immutable raw-turn log and derives them into a
browsable model of sessions -> traces -> spans.

This binary is the server. It runs the services, owns the database, and offers
the operator tooling around them. To capture a session or read one back, use the
client, tapesctl:

  tapesctl start <agent>    Launch an agent under a capture proxy
  tapesctl sessions list    Read back what was captured
  tapesctl export <id>      Export a session as JSONL

Run services:
  tapes serve          Run the full local stack: proxy, API, and derive worker
  tapes serve api      Run just the API server
  tapes serve proxy    Run just the proxy server
  tapes serve ingest   Run just the ingest server
  tapes serve derive-worker  Project captured raw turns into sessions/traces/spans
  tapes serve embed-worker   Backfill span embeddings for semantic search

Provision a local environment:
  tapes local up                     Start Postgres and Ollama in Docker
  tapes init                         Initialize a local .tapes directory
  tapes init --preset <preset|url>   Initialize with a provider preset or remote config

Serve cassettes:
  Set cassettes = ["http://host/openapi"] or pass --cassettes, then run tapes serve

Configuration:
  tapes config set <key> <value>    Set a configuration value
  tapes config get <key>            Get a configuration value
  tapes config list                 List all configuration values`

const tapesShortDesc string = "Tapes - Agent Telemetry"

// tapesFlags defines flags registered on the root tapes command.
var tapesFlags = config.FlagSet{
	config.FlagTelemetryDisabled: {
		Name:        "disable-telemetry",
		ViperKey:    "telemetry.disabled",
		Description: "Disable anonymous usage telemetry",
	},
	config.FlagUpdateCheckDisabled: {
		Name:        "disable-update-check",
		ViperKey:    "update.disabled",
		Description: "Disable checking for new versions",
	},
	config.FlagLogLevel: {
		Name:        "log-level",
		ViperKey:    "logging.level",
		Description: "Minimum log level (debug, info, warn, error)",
	},
	config.FlagLogFormat: {
		Name:        "log-format",
		ViperKey:    "logging.format",
		Description: "Log format (auto, console, text, json)",
	},
	config.FlagLogColor: {
		Name:        "log-color",
		ViperKey:    "logging.color",
		Description: "Console log color mode (auto, always, never)",
	},
}

func NewTapesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "tapes",
		Short:              tapesShortDesc,
		Long:               tapesLongDesc,
		PersistentPreRunE:  preRun,
		PersistentPostRunE: closeTelemetry,
	}

	// Global flags
	defaults := config.NewDefaultConfig()
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")
	_ = cmd.PersistentFlags().MarkDeprecated("debug", "use --log-level=debug instead")
	cmd.PersistentFlags().String("log-level", defaults.Logging.Level, "Minimum log level (debug, info, warn, error)")
	cmd.PersistentFlags().String("log-format", defaults.Logging.Format, "Log format (auto, console, text, json)")
	cmd.PersistentFlags().String("log-color", defaults.Logging.Color, "Console log color mode (auto, always, never)")
	cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")
	cmd.PersistentFlags().Bool("disable-telemetry", false, "Disable anonymous usage telemetry")
	cmd.PersistentFlags().Bool("disable-update-check", false, "Disable checking for new versions")
	_ = cmd.PersistentFlags().MarkHidden("disable-update-check")

	// Add subcommands
	cmd.AddCommand(configcmder.NewConfigCmd())
	cmd.AddCommand(devcmder.NewDevCmd())
	cmd.AddCommand(authcmder.NewAuthCmd())
	cmd.AddCommand(backfillcmder.NewBackfillCmd())
	cmd.AddCommand(initcmder.NewInitCmd())
	cmd.AddCommand(localcmder.NewLocalCmd())
	cmd.AddCommand(servecmder.NewServeCmd())
	cmd.AddCommand(statuscmder.NewStatusCmd())
	cmd.AddCommand(versioncmder.NewVersionCmd())

	return cmd
}

// preRun prints an update notice (if available) before the command starts,
// then initializes telemetry. The HTTP call has a 2s timeout cap, so
// worst-case startup delay is bounded. The check can be disabled via the
// hidden --disable-update-check flag, TAPES_UPDATE_DISABLED env var, or
// [update] disabled = true in config.toml.
func preRun(cmd *cobra.Command, args []string) error {
	configDir, _ := cmd.Flags().GetString("config-dir")
	v, err := config.InitViper(configDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	config.BindRegisteredFlags(v, cmd, tapesFlags, []string{
		config.FlagUpdateCheckDisabled,
		config.FlagTelemetryDisabled,
		config.FlagLogLevel,
		config.FlagLogFormat,
		config.FlagLogColor,
	})

	settings, err := resolveLoggingSettings(cmd, v.GetString("logging.level"), v.GetString("logging.format"), v.GetString("logging.color"))
	if err != nil {
		return err
	}
	cmd.SetContext(logger.WithSettings(cmd.Context(), settings))
	slog.SetDefault(settings.New())

	if !v.GetBool("update.disabled") {
		if msg := update.CheckForUpdate(utils.Version); msg != "" {
			printUpdateNotice(os.Stderr, msg, logger.ColorEnabled(os.Stderr, settings.Color))
		}
	}

	return initTelemetry(cmd, args, configDir, v.GetBool("telemetry.disabled"))
}

func printUpdateNotice(w io.Writer, version string, color bool) {
	if color {
		fmt.Fprintln(w, "\033[33;1mTapes Update Available!\033[0m")
		fmt.Fprintf(w, "\033[34m%s → %s\033[0m\n", utils.Version, version)
		fmt.Fprintln(w, "\033[37mRun: curl -sSfL https://download.tapes.dev/install | bash\033[0m")
		return
	}

	fmt.Fprintln(w, "Tapes Update Available!")
	fmt.Fprintf(w, "%s → %s\n", utils.Version, version)
	fmt.Fprintln(w, "Run: curl -sSfL https://download.tapes.dev/install | bash")
}

func resolveLoggingSettings(cmd *cobra.Command, level, format, color string) (logger.Settings, error) {
	debug, err := cmd.Root().PersistentFlags().GetBool("debug")
	if err != nil {
		return logger.Settings{}, fmt.Errorf("could not get debug flag: %w", err)
	}
	if debug && !cmd.Root().PersistentFlags().Changed("log-level") {
		level = "debug"
	}

	settings, err := logger.ParseSettings(level, format, color)
	if err != nil {
		return logger.Settings{}, err
	}
	return settings, nil
}

// initTelemetry initializes anonymous telemetry and stores the client in the
// command context. Telemetry is silently skipped when disabled via config,
// flag, env var, or CI detection — errors during init never block command
// execution. Viper handles the flag > env > config file precedence for the
// telemetry.disabled setting.
func initTelemetry(cmd *cobra.Command, _ []string, configDir string, disabled bool) error {
	initTelemLogger := logger.FromContext(cmd.Context())

	// Single check covers --disable-telemetry flag, TAPES_TELEMETRY_DISABLED
	// env var, and config.toml [telemetry] disabled setting.
	if disabled {
		return nil
	}

	// Check CI environment.
	if telemetry.IsCI() {
		return nil
	}

	client, isFirstRun := newTelemetryClient(configDir, initTelemLogger)
	if client == nil {
		return nil
	}

	if isFirstRun {
		client.CaptureInstall()
	}

	// Capture the command run event now so it is enqueued even if
	// PersistentPostRunE is skipped due to a command error.
	client.CaptureCommandRun(cmd.CommandPath())

	cmd.SetContext(telemetry.WithContext(cmd.Context(), client))

	return nil
}

// newTelemetryClient creates the PostHog telemetry client and loads or creates
// the persistent identity. Returns (nil, false) if any step fails — telemetry
// setup errors are intentionally non-fatal.
func newTelemetryClient(configDir string, l *slog.Logger) (client *telemetry.Client, isFirstRun bool) {
	mgr, err := telemetry.NewManager(configDir)
	if err != nil {
		return nil, false
	}

	state, isFirstRun, err := mgr.LoadOrCreate()
	if err != nil {
		return nil, false
	}

	client, err = telemetry.NewClient(state.UUID, l)
	if err != nil {
		return nil, false
	}

	return client, isFirstRun
}

// closeTelemetry flushes pending events and shuts down the PostHog client.
func closeTelemetry(cmd *cobra.Command, _ []string) error {
	client := telemetry.FromContext(cmd.Context())
	if client == nil {
		return nil
	}

	_ = client.Close()

	return nil
}

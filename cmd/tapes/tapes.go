// Package tapescmder
package tapescmder

import (
	"log/slog"

	"github.com/spf13/cobra"

	authcmder "github.com/papercomputeco/tapes/cmd/tapes/auth"
	chatcmder "github.com/papercomputeco/tapes/cmd/tapes/chat"
	checkoutcmder "github.com/papercomputeco/tapes/cmd/tapes/checkout"
	configcmder "github.com/papercomputeco/tapes/cmd/tapes/config"
	deckcmder "github.com/papercomputeco/tapes/cmd/tapes/deck"
	initcmder "github.com/papercomputeco/tapes/cmd/tapes/init"
	migratecmder "github.com/papercomputeco/tapes/cmd/tapes/migrate"
	searchcmder "github.com/papercomputeco/tapes/cmd/tapes/search"
	seedcmder "github.com/papercomputeco/tapes/cmd/tapes/seed"
	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	skillcmder "github.com/papercomputeco/tapes/cmd/tapes/skill"
	startcmder "github.com/papercomputeco/tapes/cmd/tapes/start"
	statuscmder "github.com/papercomputeco/tapes/cmd/tapes/status"
	synccmder "github.com/papercomputeco/tapes/cmd/tapes/sync"
	versioncmder "github.com/papercomputeco/tapes/cmd/version"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

const tapesLongDesc string = `Tapes is automatic telemetry for your agents.

Run services using:
  tapes start          Start proxy + API (auto ports)
  tapes start <agent>  Start proxy + API and launch an agent
  tapes serve api      Run the API server
  tapes serve proxy    Run the proxy server
  tapes serve          Run both servers together

Experimental: Chat through the proxy:
  tapes chat               Start an interactive chat session
  tapes checkout <hash>    Checkout a conversation point
  tapes checkout           Clear checkout state, start fresh
  tapes status             Show current checkout state
  tapes init                         Initialize a local .tapes directory
  tapes init --preset <preset|url>   Initialize with a provider preset or remote config

Search sessions:
  tapes search         Search sessions using semantic similarity

	Deck sessions:
	  tapes deck           ROI dashboard for sessions
	  tapes deck --web     Local web dashboard
	  tapes seed           Seed demo sessions

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
}

func NewTapesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "tapes",
		Short:              tapesShortDesc,
		Long:               tapesLongDesc,
		PersistentPreRunE:  initTelemetry,
		PersistentPostRunE: closeTelemetry,
	}

	// Global flags
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")
	cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")
	cmd.PersistentFlags().Bool("disable-telemetry", false, "Disable anonymous usage telemetry")

	// Add subcommands
	cmd.AddCommand(synccmder.NewSyncCmd())
	cmd.AddCommand(chatcmder.NewChatCmd())
	cmd.AddCommand(checkoutcmder.NewCheckoutCmd())
	cmd.AddCommand(configcmder.NewConfigCmd())
	cmd.AddCommand(deckcmder.NewDeckCmd())
	cmd.AddCommand(authcmder.NewAuthCmd())
	cmd.AddCommand(initcmder.NewInitCmd())
	cmd.AddCommand(searchcmder.NewSearchCmd())
	cmd.AddCommand(seedcmder.NewSeedCmd())
	cmd.AddCommand(migratecmder.NewMigrateCmd())
	cmd.AddCommand(servecmder.NewServeCmd())
	cmd.AddCommand(skillcmder.NewSkillCmd())
	cmd.AddCommand(startcmder.NewStartCmd())
	cmd.AddCommand(statuscmder.NewStatusCmd())
	cmd.AddCommand(versioncmder.NewVersionCmd())

	return cmd
}

// initTelemetry initializes anonymous telemetry and stores the client in the
// command context. Telemetry is silently skipped when disabled via config,
// flag, env var, or CI detection — errors during init never block command
// execution. Viper handles the flag > env > config file precedence for the
// telemetry.disabled setting.
func initTelemetry(cmd *cobra.Command, _ []string) error {
	initTelemLogger := logger.New(logger.WithDebug(true), logger.WithPretty(true))
	configDir, _ := cmd.Flags().GetString("config-dir")

	v, err := config.InitViper(configDir)
	if err != nil {
		initTelemLogger.Warn("Could not initiate telemetry, continuing", "error", err)
		return nil
	}

	config.BindRegisteredFlags(v, cmd, tapesFlags, []string{
		config.FlagTelemetryDisabled,
	})

	// Single check covers --disable-telemetry flag, TAPES_TELEMETRY_DISABLED
	// env var, and config.toml [telemetry] disabled setting.
	if v.GetBool("telemetry.disabled") {
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

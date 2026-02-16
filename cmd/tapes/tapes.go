// Package tapescmder
package tapescmder

import (
	"github.com/spf13/cobra"

	authcmder "github.com/papercomputeco/tapes/cmd/tapes/auth"
	synccmder "github.com/papercomputeco/tapes/cmd/tapes/sync"
	chatcmder "github.com/papercomputeco/tapes/cmd/tapes/chat"
	checkoutcmder "github.com/papercomputeco/tapes/cmd/tapes/checkout"
	configcmder "github.com/papercomputeco/tapes/cmd/tapes/config"
	deckcmder "github.com/papercomputeco/tapes/cmd/tapes/deck"
	initcmder "github.com/papercomputeco/tapes/cmd/tapes/init"
	searchcmder "github.com/papercomputeco/tapes/cmd/tapes/search"
	seedcmder "github.com/papercomputeco/tapes/cmd/tapes/seed"
	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	startcmder "github.com/papercomputeco/tapes/cmd/tapes/start"
	statuscmder "github.com/papercomputeco/tapes/cmd/tapes/status"
	versioncmder "github.com/papercomputeco/tapes/cmd/version"
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

func NewTapesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tapes",
		Short: tapesShortDesc,
		Long:  tapesLongDesc,
	}

	// Global flags
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")
	cmd.PersistentFlags().String("config-dir", "", "Override path to .tapes/ config directory")

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
	cmd.AddCommand(servecmder.NewServeCmd())
	cmd.AddCommand(startcmder.NewStartCmd())
	cmd.AddCommand(statuscmder.NewStatusCmd())
	cmd.AddCommand(versioncmder.NewVersionCmd())

	return cmd
}

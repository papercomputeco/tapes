// Package tapescmder
package tapescmder

import (
	"github.com/spf13/cobra"

	chatcmder "github.com/papercomputeco/tapes/cmd/tapes/chat"
	checkoutcmder "github.com/papercomputeco/tapes/cmd/tapes/checkout"
	deckcmder "github.com/papercomputeco/tapes/cmd/tapes/deck"
	initcmder "github.com/papercomputeco/tapes/cmd/tapes/init"
	searchcmder "github.com/papercomputeco/tapes/cmd/tapes/search"
	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	statuscmder "github.com/papercomputeco/tapes/cmd/tapes/status"
	versioncmder "github.com/papercomputeco/tapes/cmd/version"
)

const tapesLongDesc string = `Tapes is automatic telemetry for your agents.

Run services using:
  tapes serve api      Run the API server
  tapes serve proxy    Run the proxy server
  tapes serve          Run both servers together

Experimental: Chat through the proxy:
  tapes chat               Start an interactive chat session
  tapes checkout <hash>    Checkout a conversation point
  tapes checkout           Clear checkout state, start fresh
  tapes status             Show current checkout state
  tapes init               Initialize a local .tapes directory

Search sessions:
  tapes search         Search sessions using semantic similarity

Deck sessions:
  tapes deck           ROI dashboard for sessions
  tapes deck --web     Local web dashboard`

const tapesShortDesc string = "Tapes - Agent Telemetry"

func NewTapesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tapes",
		Short: tapesShortDesc,
		Long:  tapesLongDesc,
	}

	// Global flags
	cmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")

	// Add subcommands
	cmd.AddCommand(chatcmder.NewChatCmd())
	cmd.AddCommand(checkoutcmder.NewCheckoutCmd())
	cmd.AddCommand(deckcmder.NewDeckCmd())
	cmd.AddCommand(initcmder.NewInitCmd())
	cmd.AddCommand(searchcmder.NewSearchCmd())
	cmd.AddCommand(servecmder.NewServeCmd())
	cmd.AddCommand(statuscmder.NewStatusCmd())
	cmd.AddCommand(versioncmder.NewVersionCmd())

	return cmd
}

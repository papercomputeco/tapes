// Package tapescmder
package tapescmder

import (
	"github.com/spf13/cobra"

	searchcmder "github.com/papercomputeco/tapes/cmd/tapes/search"
	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	versioncmder "github.com/papercomputeco/tapes/cmd/version"
)

const tapesLongDesc string = `Tapes is automatic telemetry for your agents.

Run services using:
  tapes serve api      Run the API server
  tapes serve proxy    Run the proxy server
  tapes serve          Run both servers together

Search sessions:
  tapes search         Search sessions using semantic similarity`

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
	cmd.AddCommand(servecmder.NewServeCmd())
	cmd.AddCommand(searchcmder.NewSearchCmd())
	cmd.AddCommand(versioncmder.NewVersionCmd())

	return cmd
}

// Package tapescmder
package tapescmder

import (
	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	"github.com/spf13/cobra"
)

const tapesLongDesc string = `Tapes is automatic telemetry for your agents.

Run services using:
  tapes serve api      Run the API server
  tapes serve proxy    Run the proxy server
  tapes serve          Run both servers together`

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

	return cmd
}

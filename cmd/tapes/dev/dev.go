package devcmder

import (
	"github.com/spf13/cobra"
)

const devLongDesc string = `Developer utilities that operate on checked-in corpus data.

These commands never touch a database or a network: they replay
corpus raw layers through the real deriver and API renderers, so
their output is the live read surface by construction.`

// NewDevCmd returns the `tapes dev` command group.
func NewDevCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Developer utilities (corpus fixtures)",
		Long:  devLongDesc,
	}
	cmd.AddCommand(newTraceFixturesCmd())
	return cmd
}

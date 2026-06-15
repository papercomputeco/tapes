package devcmder

import (
	"github.com/spf13/cobra"
)

const devLongDesc string = `Developer and operator utilities.

The corpus-fixture commands never touch a database or a network: they
replay corpus raw layers through the real deriver and API renderers,
so their output is the live read surface by construction.

embed-spans is the exception: it connects to a tapes Postgres database
and an embedding backend to backfill span embeddings.`

// NewDevCmd returns the `tapes dev` command group.
func NewDevCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Developer utilities (corpus fixtures, backfills)",
		Long:  devLongDesc,
	}
	cmd.AddCommand(newTraceFixturesCmd())
	cmd.AddCommand(newEmbedSpansCmd())
	return cmd
}

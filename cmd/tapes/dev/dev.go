package devcmder

import (
	"github.com/spf13/cobra"
)

const devLongDesc string = `Developer and operator utilities.

trace-fixtures never touches a database or a network: it replays corpus
raw layers through the real deriver and API renderers, so its output is
the live read surface by construction.

dump-corpus, rederive, and embed-spans connect to a tapes Postgres
database: dump-corpus exports raw_turns back into corpus files (the
inverse of the fixture replay), rederive rebuilds the projection from
raw (the direct-call form of POST /v1/admin/derive/run), and embed-spans
backfills span embeddings.`

// NewDevCmd returns the `tapes dev` command group.
func NewDevCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Developer utilities (corpus fixtures, backfills)",
		Long:  devLongDesc,
	}
	cmd.AddCommand(newTraceFixturesCmd())
	cmd.AddCommand(newDumpCorpusCmd())
	cmd.AddCommand(newCheckInvariantsCmd())
	cmd.AddCommand(newCheckOpenAPICmd())
	cmd.AddCommand(newRederiveCmd())
	cmd.AddCommand(newEmbedSpansCmd())
	return cmd
}

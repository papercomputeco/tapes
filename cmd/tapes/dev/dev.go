package devcmder

import (
	"github.com/spf13/cobra"
)

const devLongDesc string = `Developer and operator utilities.

trace-fixtures never touches a database or a network: it replays corpus
raw layers through the real deriver and API renderers, so its output is
the live read surface by construction.

dump-corpus and rederive connect to a tapes Postgres database:
dump-corpus exports raw_turns back into corpus files (the inverse of the
fixture replay), and rederive rebuilds the projection from raw (the
direct-call form of POST /v1/admin/derive/run).

openapi compiles a published contract from the route registrations. The
servers publish the same document at their own /openapi; this one adds the
per-field prose a deployed binary cannot read, because the comments it
comes from live in the source tree.`

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
	cmd.AddCommand(newOpenAPICmd())
	cmd.AddCommand(newRederiveCmd())
	return cmd
}

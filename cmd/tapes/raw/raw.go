// Package rawcmder holds the operator tooling for the verbatim-capture layer.
package rawcmder

import (
	"github.com/spf13/cobra"
)

const rawLongDesc string = `Operate on the verbatim response bytes captured alongside each turn.

raw_turns stores two views of the same upstream response: raw_response,
the bytes exactly as they arrived, and response, the reduced turn a
capture adapter produced from them. The reduction is lossy and, until
every capture path shares one reducer, adapter-specific.

The commands here inspect that relationship. They read; nothing under
this group writes to the database.`

// NewRawCmd builds the `tapes raw` command group.
func NewRawCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "raw",
		Short: "Inspect the verbatim capture layer",
		Long:  rawLongDesc,
	}

	cmd.AddCommand(newEquivalenceCmd())
	return cmd
}

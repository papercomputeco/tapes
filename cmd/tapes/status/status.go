// Package statuscmder provides the status command for displaying the current
// checkout state of the local .tapes directory.
package statuscmder

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/utils"
)

const statusLongDesc string = `Show the current tapes checkout state.

Reads the local .tapes/ directory (or ~/.tapes/) to display the checked-out
conversation point, including the hash and message history.

If no checkout state exists, indicates that the next chat session will start
a new conversation.

Examples:
  tapes status`

const statusShortDesc string = "Show current checkout state"

func NewStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: statusShortDesc,
		Long:  statusLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runStatus()
		},
	}

	return cmd
}

func runStatus() error {
	manager := dotdir.NewManager()

	state, err := manager.LoadCheckoutState("")
	if err != nil {
		return fmt.Errorf("loading checkout state: %w", err)
	}

	if state == nil {
		fmt.Printf("  %s No checkout state. Next chat will start a new conversation.\n", cliui.DimStyle.Render("●"))
		return nil
	}

	fmt.Printf("\n  %s  %s\n", cliui.KeyStyle.Render("Checked out:"), cliui.HashStyle.Render(state.Hash))
	fmt.Printf("  %s  %s\n\n", cliui.KeyStyle.Render("Messages:   "), cliui.NameStyle.Render(strconv.Itoa(len(state.Messages))))

	for i, msg := range state.Messages {
		preview := utils.Truncate(msg.Content, 72)
		fmt.Printf("  %s %s %s\n",
			cliui.DimStyle.Render(fmt.Sprintf("%d.", i+1)),
			cliui.RoleStyle.Render("["+msg.Role+"]"),
			cliui.PreviewStyle.Render(preview),
		)
	}

	fmt.Println()
	return nil
}

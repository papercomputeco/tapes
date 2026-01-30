// Package statuscmder provides the status command for displaying the current
// checkout state of the local .tapes directory.
package statuscmder

import (
	"fmt"

	"github.com/spf13/cobra"

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
		fmt.Println("No checkout state. Next chat will start a new conversation.")
		return nil
	}

	fmt.Printf("Checked out: %s\n", state.Hash)
	fmt.Printf("Messages:    %d\n", len(state.Messages))
	fmt.Println()

	for i, msg := range state.Messages {
		preview := utils.Truncate(msg.Content, 72)
		fmt.Printf("  %d. [%s] %s\n", i+1, msg.Role, preview)
	}

	return nil
}

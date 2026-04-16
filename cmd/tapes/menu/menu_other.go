//go:build !darwin

package menucmder

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

func NewMenuCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "menu",
		Short: "Run the Tapes menu bar app (macOS only)",
		RunE: func(_ *cobra.Command, _ []string) error {
			return fmt.Errorf("tapes menu is only supported on macOS (current OS: %s)", runtime.GOOS)
		},
	}
}

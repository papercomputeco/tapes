//go:build darwin

// Package menucmder provides the "menu" spf13/cobra command which runs a
// macOS menu bar app displaying the tapes logo.
package menucmder

import (
	_ "embed"
	"fmt"
	"os"

	"github.com/getlantern/systray"
	"github.com/spf13/cobra"
)

//go:embed tapes-logo.png
var tapesLogoPNG []byte

func NewMenuCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "menu",
		Short: "Run the Tapes menu bar app",
		Long:  "Launches a macOS menu bar app displaying the tapes logo.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")

			// Acquire the menu lock. If held by another live instance we
			// exit silently — the existing menu owns the menu bar slot.
			// The kernel auto-releases the flock when this process exits,
			// so a SIGKILL cannot strand the lock.
			lock, err := acquireMenuLock(configDir)
			if err != nil {
				return fmt.Errorf("acquiring menu lock: %w", err)
			}
			if lock == nil {
				return nil
			}
			defer func() { _ = lock.Close() }()

			if err := writePID(lock, os.Getpid()); err != nil {
				// Informational only — proceed even if we cannot persist
				// the pid for human inspection.
				_ = err
			}

			systray.Run(onReady, onExit)
			return nil
		},
	}
}

func onReady() {
	systray.SetTemplateIcon(tapesLogoPNG, tapesLogoPNG)
	systray.SetTooltip("Tapes")

	mQuit := systray.AddMenuItem("Quit", "Quit the Tapes menu bar app")

	go func() {
		<-mQuit.ClickedCh
		systray.Quit()
	}()
}

func onExit() {}

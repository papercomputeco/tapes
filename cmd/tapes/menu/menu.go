//go:build darwin

// Package menucmder provides the "menu" spf13/cobra command which runs a
// macOS menu bar app displaying the tapes logo.
package menucmder

import (
	_ "embed"
	"fmt"

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

			lock, err := acquireMenuLock(configDir)
			if err != nil {
				return fmt.Errorf("acquiring menu lock: %w", err)
			}
			if lock == nil {
				return nil
			}
			defer func() { _ = lock.Close() }()

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

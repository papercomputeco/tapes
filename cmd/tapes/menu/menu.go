//go:build darwin

// Package menucmder provides the "menu" spf13/cobra command which runs a
// macOS menu bar app displaying the tapes logo. The icon is pre-rendered at
// build time by ./internal/icongen so the menu binary does not pull SVG
// rasterization dependencies into the runtime.
package menucmder

//go:generate go run ./internal/icongen -in tapes-logo.svg -out tapes-logo.png

import (
	_ "embed"
	"fmt"
	"time"

	"github.com/getlantern/systray"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/start"
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
			systray.Run(onReadyFor(configDir), onExit)
			return nil
		},
	}
}

func onReadyFor(configDir string) func() {
	return func() {
		systray.SetTemplateIcon(tapesLogoPNG, tapesLogoPNG)
		systray.SetTooltip("Tapes")

		status := systray.AddMenuItem("Running: …", "Number of running tapes agents")
		status.Disable()
		systray.AddSeparator()
		mQuit := systray.AddMenuItem("Quit", "Quit the Tapes menu bar app")

		go pollAgentCount(configDir, status)

		go func() {
			<-mQuit.ClickedCh
			systray.Quit()
		}()
	}
}

func onExit() {}

// pollAgentCount rereads the tapes start state every few seconds and pushes
// the agent count into the given menu item. A missing or unreadable state is
// reported as "Running: 0". The manager is created once and reused across
// ticks to avoid repeated filesystem resolution of the config directory.
func pollAgentCount(configDir string, item *systray.MenuItem) {
	mgr, err := start.NewManager(configDir)
	if err != nil {
		item.SetTitle("Running: 0")
		return
	}
	render := func() {
		n := readRunningAgents(mgr)
		item.SetTitle(fmt.Sprintf("Running: %d", n))
	}
	render()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		render()
	}
}

func readRunningAgents(mgr *start.Manager) int {
	state, err := mgr.LoadState()
	if err != nil || state == nil {
		return 0
	}
	return len(state.Agents)
}

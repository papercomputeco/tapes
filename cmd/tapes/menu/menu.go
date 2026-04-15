// Package menucmder provides the "menu" spf13/cobra command which runs a
// macOS menu bar app displaying the tapes logo. For now the icon is inert —
// it shows up in the menu bar and offers a Quit item plus a live count of
// running tapes agents.
package menucmder

import (
	"bytes"
	_ "embed"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"time"

	"github.com/getlantern/systray"
	"github.com/spf13/cobra"
	"github.com/srwiley/oksvg"
	"github.com/srwiley/rasterx"

	"github.com/papercomputeco/tapes/pkg/start"
)

//go:embed tapes-logo.svg
var tapesLogoSVG []byte

// iconSize is 22pt at @2x for a Retina menu bar.
const iconSize = 44

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
		icon, err := renderTemplateIcon(tapesLogoSVG, iconSize)
		if err != nil {
			systray.SetTitle("tapes")
		} else {
			systray.SetTemplateIcon(icon, icon)
		}
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
// reported as "Running: 0".
func pollAgentCount(configDir string, item *systray.MenuItem) {
	render := func() {
		n := readRunningAgents(configDir)
		item.SetTitle(fmt.Sprintf("Running: %d", n))
	}
	render()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		render()
	}
}

func readRunningAgents(configDir string) int {
	mgr, err := start.NewManager(configDir)
	if err != nil {
		return 0
	}
	state, err := mgr.LoadState()
	if err != nil || state == nil {
		return 0
	}
	return len(state.Agents)
}

// renderTemplateIcon rasterizes the embedded SVG at the requested size and
// flattens every opaque pixel to black while preserving alpha, producing a
// macOS template image that AppKit will auto-tint for light/dark menu bars.
func renderTemplateIcon(svgBytes []byte, size int) ([]byte, error) {
	icon, err := oksvg.ReadIconStream(bytes.NewReader(svgBytes))
	if err != nil {
		return nil, fmt.Errorf("parse svg: %w", err)
	}
	icon.SetTarget(0, 0, float64(size), float64(size))

	rgba := image.NewRGBA(image.Rect(0, 0, size, size))
	scanner := rasterx.NewScannerGV(size, size, rgba, rgba.Bounds())
	raster := rasterx.NewDasher(size, size, scanner)
	icon.Draw(raster, 1.0)

	// Template conversion: keep coloured body pixels as black and promote
	// near-white pixels (the cassette's white details) to transparent holes
	// so the menu bar shows through them.
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			r, g, b, a := rgba.At(x, y).RGBA()
			if a == 0 {
				continue
			}
			r8, g8, b8 := uint8(r>>8), uint8(g>>8), uint8(b>>8)
			if r8 > 200 && g8 > 200 && b8 > 200 {
				rgba.Set(x, y, color.RGBA{})
				continue
			}
			rgba.Set(x, y, color.RGBA{R: 0, G: 0, B: 0, A: uint8(a >> 8)})
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, rgba); err != nil {
		return nil, fmt.Errorf("encode png: %w", err)
	}
	return buf.Bytes(), nil
}

// Command icongen rasterizes the tapes logo SVG into a macOS template PNG
// that the menu bar app embeds at compile time. Re-run via `go generate
// ./cmd/tapes/menu/...` whenever the SVG changes.
//
// The runtime menu binary should not depend on oksvg/rasterx — that is the
// whole point of pre-rendering here.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"log"
	"os"

	"github.com/srwiley/oksvg"
	"github.com/srwiley/rasterx"
)

// iconHeight matches the runtime constant in cmd/tapes/menu/menu.go: 22pt at
// @2x for a Retina menu bar. Width is derived from the SVG viewBox so the
// rasterized template keeps the artwork's aspect ratio.
const iconHeight = 44

func main() {
	in := flag.String("in", "tapes-logo.svg", "input SVG path")
	out := flag.String("out", "tapes-logo.png", "output PNG path")
	height := flag.Int("height", iconHeight, "output height in pixels; width is derived from the SVG aspect ratio")
	flag.Parse()

	svgBytes, err := os.ReadFile(*in)
	if err != nil {
		log.Fatalf("read svg %s: %v", *in, err)
	}

	pngBytes, err := rasterizeTemplate(svgBytes, *height)
	if err != nil {
		log.Fatalf("rasterize: %v", err)
	}

	if err := os.WriteFile(*out, pngBytes, 0o600); err != nil {
		log.Fatalf("write png %s: %v", *out, err)
	}

	fmt.Fprintf(os.Stderr, "wrote %s (%d bytes)\n", *out, len(pngBytes))
}

// rasterizeTemplate flattens every opaque pixel of the SVG to black while
// preserving alpha and dropping near-white pixels, producing a macOS template
// image AppKit will auto-tint for light/dark menu bars. The output width is
// derived from the SVG's intrinsic aspect ratio so vertical logos do not get
// stretched into a square.
func rasterizeTemplate(svgBytes []byte, height int) ([]byte, error) {
	icon, err := oksvg.ReadIconStream(bytes.NewReader(svgBytes))
	if err != nil {
		return nil, fmt.Errorf("parse svg: %w", err)
	}
	vbW, vbH := icon.ViewBox.W, icon.ViewBox.H
	if vbW <= 0 || vbH <= 0 {
		return nil, fmt.Errorf("svg has non-positive viewBox %vx%v", vbW, vbH)
	}
	width := max(int(float64(height)*vbW/vbH+0.5), 1)

	icon.SetTarget(0, 0, float64(width), float64(height))

	rgba := image.NewRGBA(image.Rect(0, 0, width, height))
	scanner := rasterx.NewScannerGV(width, height, rgba, rgba.Bounds())
	raster := rasterx.NewDasher(width, height, scanner)
	icon.Draw(raster, 1.0)

	for y := range height {
		for x := range width {
			r, g, b, a := rgba.At(x, y).RGBA()
			if a == 0 {
				continue
			}
			// RGBA() returns 16-bit channels; >>8 shifts down to the 8-bit
			// range, so these conversions cannot overflow.
			r8, g8, b8 := uint8(r>>8), uint8(g>>8), uint8(b>>8) //nolint:gosec
			if r8 > 200 && g8 > 200 && b8 > 200 {
				rgba.Set(x, y, color.RGBA{})
				continue
			}
			rgba.Set(x, y, color.RGBA{R: 0, G: 0, B: 0, A: uint8(a >> 8)}) //nolint:gosec
		}
	}

	var buf bytes.Buffer
	if err := png.Encode(&buf, rgba); err != nil {
		return nil, fmt.Errorf("encode png: %w", err)
	}
	return buf.Bytes(), nil
}

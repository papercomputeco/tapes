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

// iconSize matches the runtime constant in cmd/tapes/menu/menu.go: 22pt at @2x
// for a Retina menu bar.
const iconSize = 44

func main() {
	in := flag.String("in", "tapes-logo.svg", "input SVG path")
	out := flag.String("out", "tapes-logo.png", "output PNG path")
	size := flag.Int("size", iconSize, "square output size in pixels")
	flag.Parse()

	svgBytes, err := os.ReadFile(*in)
	if err != nil {
		log.Fatalf("read svg %s: %v", *in, err)
	}

	pngBytes, err := rasterizeTemplate(svgBytes, *size)
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
// image AppKit will auto-tint for light/dark menu bars.
func rasterizeTemplate(svgBytes []byte, size int) ([]byte, error) {
	icon, err := oksvg.ReadIconStream(bytes.NewReader(svgBytes))
	if err != nil {
		return nil, fmt.Errorf("parse svg: %w", err)
	}
	icon.SetTarget(0, 0, float64(size), float64(size))

	rgba := image.NewRGBA(image.Rect(0, 0, size, size))
	scanner := rasterx.NewScannerGV(size, size, rgba, rgba.Bounds())
	raster := rasterx.NewDasher(size, size, scanner)
	icon.Draw(raster, 1.0)

	for y := range size {
		for x := range size {
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

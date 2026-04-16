//go:build darwin

package menucmder

import (
	"bytes"
	"image/color"
	"image/png"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("NewMenuCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := NewMenuCmd()
		Expect(cmd.Use).To(Equal("menu"))
		Expect(cmd.Short).NotTo(BeEmpty())
	})
})

var _ = Describe("readRunningAgents", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-menu-agents-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("returns 0 when no state file exists", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(readRunningAgents(mgr)).To(Equal(0))
	})

	It("returns 0 when state has no agents", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SaveState(&start.State{
			DaemonPID: 123,
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
		})).To(Succeed())

		Expect(readRunningAgents(mgr)).To(Equal(0))
	})

	It("returns the correct agent count", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SaveState(&start.State{
			DaemonPID: 123,
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
			Agents: []start.AgentSession{
				{Name: "claude", PID: 100, StartedAt: time.Now()},
				{Name: "opencode", PID: 200, StartedAt: time.Now()},
			},
		})).To(Succeed())

		Expect(readRunningAgents(mgr)).To(Equal(2))
	})

	It("returns 0 when state file is corrupted", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(mgr.StatePath, []byte("not json"), 0o600)).To(Succeed())

		Expect(readRunningAgents(mgr)).To(Equal(0))
	})
})

var _ = Describe("renderTemplateIcon", func() {
	It("rasterizes the embedded SVG into a valid PNG", func() {
		data, err := renderTemplateIcon(tapesLogoSVG, iconSize)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).NotTo(BeEmpty())

		img, err := png.Decode(bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())
		Expect(img.Bounds().Dx()).To(Equal(iconSize))
		Expect(img.Bounds().Dy()).To(Equal(iconSize))
	})

	It("produces only black and transparent pixels (template image)", func() {
		data, err := renderTemplateIcon(tapesLogoSVG, iconSize)
		Expect(err).NotTo(HaveOccurred())

		img, err := png.Decode(bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())

		bounds := img.Bounds()
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			for x := bounds.Min.X; x < bounds.Max.X; x++ {
				r, g, b, a := img.At(x, y).RGBA()
				if a == 0 {
					continue
				}
				// Non-transparent pixels must be black.
				Expect(r).To(BeZero(), "red channel should be 0 at (%d,%d)", x, y)
				Expect(g).To(BeZero(), "green channel should be 0 at (%d,%d)", x, y)
				Expect(b).To(BeZero(), "blue channel should be 0 at (%d,%d)", x, y)
			}
		}
	})

	It("works at different sizes", func() {
		for _, size := range []int{16, 32, 64} {
			data, err := renderTemplateIcon(tapesLogoSVG, size)
			Expect(err).NotTo(HaveOccurred())

			img, err := png.Decode(bytes.NewReader(data))
			Expect(err).NotTo(HaveOccurred())
			Expect(img.Bounds().Dx()).To(Equal(size))
			Expect(img.Bounds().Dy()).To(Equal(size))
		}
	})

	It("produces a valid PNG even with empty SVG input", func() {
		// oksvg is lenient and never errors on bad input — it produces an
		// empty icon. Verify we still get a valid (blank) PNG back.
		data, err := renderTemplateIcon([]byte{}, 44)
		Expect(err).NotTo(HaveOccurred())

		img, err := png.Decode(bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())
		Expect(img.Bounds().Dx()).To(Equal(44))
	})

	It("converts near-white pixels to transparent", func() {
		// Minimal valid SVG with a white rectangle.
		whiteSVG := []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10" width="10" height="10">
			<rect width="10" height="10" fill="white"/>
		</svg>`)
		data, err := renderTemplateIcon(whiteSVG, 10)
		Expect(err).NotTo(HaveOccurred())

		img, err := png.Decode(bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())

		// All pixels should be fully transparent since the fill is white.
		bounds := img.Bounds()
		for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
			for x := bounds.Min.X; x < bounds.Max.X; x++ {
				_, _, _, a := img.At(x, y).RGBA()
				Expect(a).To(BeZero(), "white pixel at (%d,%d) should become transparent", x, y)
			}
		}
	})

	It("converts coloured pixels to black", func() {
		// Minimal valid SVG with a red rectangle.
		redSVG := []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10" width="10" height="10">
			<rect width="10" height="10" fill="#EA4335"/>
		</svg>`)
		data, err := renderTemplateIcon(redSVG, 10)
		Expect(err).NotTo(HaveOccurred())

		img, err := png.Decode(bytes.NewReader(data))
		Expect(err).NotTo(HaveOccurred())

		// Center pixel should be opaque black (the red was converted).
		r, g, b, a := img.At(5, 5).RGBA()
		Expect(a).NotTo(BeZero(), "coloured pixel should remain opaque")
		nrgba := color.NRGBAModel.Convert(img.At(5, 5)).(color.NRGBA)
		Expect(nrgba.R).To(BeZero())
		Expect(nrgba.G).To(BeZero())
		Expect(nrgba.B).To(BeZero())
		_ = r
		_ = g
		_ = b
	})
})

var _ = Describe("onExit", func() {
	It("does not panic", func() {
		Expect(func() { onExit() }).NotTo(Panic())
	})
})

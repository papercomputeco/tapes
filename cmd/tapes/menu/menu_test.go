//go:build darwin

package menucmder

import (
	"bytes"
	"image/png"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewMenuCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := NewMenuCmd()
		Expect(cmd.Use).To(Equal("menu"))
		Expect(cmd.Short).NotTo(BeEmpty())
	})
})

var _ = Describe("tapesLogoPNG", func() {
	It("decodes as a non-empty PNG", func() {
		Expect(tapesLogoPNG).NotTo(BeEmpty())

		img, err := png.Decode(bytes.NewReader(tapesLogoPNG))
		Expect(err).NotTo(HaveOccurred())
		Expect(img.Bounds().Dx()).To(BeNumerically(">", 0))
		Expect(img.Bounds().Dy()).To(BeNumerically(">", 0))
	})
})

var _ = Describe("onExit", func() {
	It("does not panic", func() {
		Expect(func() { onExit() }).NotTo(Panic())
	})
})

//go:build darwin

package menucmder

import (
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

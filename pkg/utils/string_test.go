package utils

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("truncate", func() {
	It("returns the string unchanged when within the limit", func() {
		Expect(Truncate("short", 10)).To(Equal("short"))
	})

	It("returns the string unchanged when exactly at the limit", func() {
		Expect(Truncate("12345", 5)).To(Equal("12345"))
	})

	It("truncates with ellipsis when over the limit", func() {
		result := Truncate("this is a long string", 10)
		Expect(result).To(Equal("this is a ..."))
	})
})

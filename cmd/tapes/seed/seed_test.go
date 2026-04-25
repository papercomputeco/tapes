package seedcmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("seed command", func() {
	It("defaults to the local API server and has no postgres flag", func() {
		cmd := NewSeedCmd()

		Expect(cmd.Flags().Lookup("api-target").DefValue).To(Equal("http://localhost:8081"))
		Expect(cmd.Flags().Lookup("postgres")).To(BeNil())
	})
})

var _ = Describe("normalizeAPITarget", func() {
	It("defaults to the local API server", func() {
		Expect(normalizeAPITarget("")).To(Equal("http://localhost:8081"))
	})

	It("adds http when the scheme is omitted", func() {
		Expect(normalizeAPITarget("localhost:8081")).To(Equal("http://localhost:8081"))
	})
})

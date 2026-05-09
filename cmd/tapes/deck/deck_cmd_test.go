package deckcmder

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewDeckCmd", func() {
	It("defaults to an API-friendly overview", func() {
		cmd := NewDeckCmd()

		Expect(cmd.Flags().Lookup("api-target").DefValue).To(Equal("http://localhost:8081"))
		Expect(cmd.Flags().Lookup("sort").DefValue).To(Equal("date"))
		Expect(cmd.Flags().Lookup("refresh").DefValue).To(Equal("0"))
		Expect(cmd.Flags().Lookup("postgres")).NotTo(BeNil())
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

var _ = Describe("deckCommander.parseFilters", func() {
	It("defaults to a bounded recent window when no time flags are provided", func() {
		cmder := &deckCommander{}

		filters, err := cmder.parseFilters()
		Expect(err).NotTo(HaveOccurred())
		Expect(filters.Since).To(Equal(30 * 24 * time.Hour))
	})

	It("preserves explicit time bounds", func() {
		cmder := &deckCommander{since: "24h"}

		filters, err := cmder.parseFilters()
		Expect(err).NotTo(HaveOccurred())
		Expect(filters.Since).To(Equal(24 * time.Hour))
	})
})

package api

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

var _ = Describe("contract versions", func() {
	Describe("DefaultContractVersions", func() {
		It("serves v1", func() {
			Expect(DefaultContractVersions()).To(Equal([]cassette.ContractVersion{"v1"}))
		})

		It("returns a fresh slice a caller cannot mutate into the default", func() {
			mutated := DefaultContractVersions()
			mutated[0] = "v9"

			Expect(DefaultContractVersions()).To(Equal([]cassette.ContractVersion{"v1"}))
		})
	})

	Describe("resolveContractVersions", func() {
		It("falls back to the default when nothing is configured", func() {
			Expect(resolveContractVersions(nil)).To(Equal(DefaultContractVersions()))
			Expect(resolveContractVersions([]cassette.ContractVersion{})).To(Equal(DefaultContractVersions()))
		})

		It("keeps a configured set", func() {
			Expect(resolveContractVersions([]cassette.ContractVersion{"v1", "v2"})).
				To(Equal([]cassette.ContractVersion{"v1", "v2"}))
		})

		It("copies, so a later write to the config slice cannot change what the server serves", func() {
			configured := []cassette.ContractVersion{"v2"}
			resolved := resolveContractVersions(configured)
			configured[0] = "v7"

			Expect(resolved).To(Equal([]cassette.ContractVersion{"v2"}))
		})
	})

	Describe("currentContractVersion", func() {
		It("advertises the only contract when one is served", func() {
			Expect(currentContractVersion([]cassette.ContractVersion{"v1"})).
				To(Equal(cassette.ContractVersion("v1")))
		})

		It("advertises the newest of several", func() {
			Expect(currentContractVersion([]cassette.ContractVersion{"v1", "v2"})).
				To(Equal(cassette.ContractVersion("v2")))
		})

		It("does not depend on the order they were configured in", func() {
			Expect(currentContractVersion([]cassette.ContractVersion{"v3", "v1", "v2"})).
				To(Equal(cassette.ContractVersion("v3")))
		})

		It("compares numerically rather than as strings", func() {
			Expect(currentContractVersion([]cassette.ContractVersion{"v9", "v10"})).
				To(Equal(cassette.ContractVersion("v10")))
		})

		It("skips malformed entries rather than advertising one", func() {
			Expect(currentContractVersion([]cassette.ContractVersion{"v1", "", "banana", "v0", "v1.2.3"})).
				To(Equal(cassette.ContractVersion("v1")))
		})

		It("advertises nothing when it serves nothing", func() {
			Expect(currentContractVersion(nil)).To(BeEmpty())
			Expect(currentContractVersion([]cassette.ContractVersion{"nonsense"})).To(BeEmpty())
		})
	})
})

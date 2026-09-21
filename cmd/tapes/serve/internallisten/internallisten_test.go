package internallisten_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/cmd/tapes/serve/internallisten"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("the internal listener's configuration", func() {
	var server *api.Server

	BeforeEach(func() {
		built, err := api.NewServer(api.Config{ListenAddr: ":0"}, inmemory.NewDriver(), logger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		server = built
	})

	// Every deployment that is not asked for one runs without it, which is
	// what keeps the documented Docker-only cassette loop unchanged.
	It("is off when the environment names no address", func() {
		GinkgoT().Setenv(api.EnvInternalListen, "")
		GinkgoT().Setenv(api.EnvInternalToken, "a-token")

		Expect(internallisten.Config().ListenAddr).To(BeEmpty())

		internal, err := internallisten.New(server, internallisten.Config(), logger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		Expect(internal).To(BeNil())
	})

	It("reads both settings from the environment", func() {
		GinkgoT().Setenv(api.EnvInternalListen, api.DefaultInternalListen)
		GinkgoT().Setenv(api.EnvInternalToken, "a-token")

		config := internallisten.Config()
		Expect(config.ListenAddr).To(Equal(":8092"))
		Expect(config.Token).To(Equal("a-token"))
	})

	It("ignores the surrounding whitespace a mounted value can carry", func() {
		GinkgoT().Setenv(api.EnvInternalListen, "  \n")

		Expect(internallisten.Config().ListenAddr).To(BeEmpty())
	})

	// Failing startup is the point. A listener that came up unauthenticated
	// because a secret failed to mount would leave a deployment looking
	// healthy while answering to anyone who found the port.
	It("refuses to start when an address is configured without a token", func() {
		GinkgoT().Setenv(api.EnvInternalListen, api.DefaultInternalListen)
		GinkgoT().Setenv(api.EnvInternalToken, "")

		internal, err := internallisten.New(server, internallisten.Config(), logger.NewNoop())
		Expect(internal).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring(api.EnvInternalToken)))
	})

	It("builds the listener when both are configured", func() {
		GinkgoT().Setenv(api.EnvInternalListen, api.DefaultInternalListen)
		GinkgoT().Setenv(api.EnvInternalToken, "a-token")

		internal, err := internallisten.New(server, internallisten.Config(), logger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		Expect(internal).NotTo(BeNil())
	})
})

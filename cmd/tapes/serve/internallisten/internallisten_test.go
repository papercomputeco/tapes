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

var _ = Describe("the admin listener choice", func() {
	It("defaults to the API listener", func() {
		GinkgoT().Setenv(api.EnvAdminListener, "")
		GinkgoT().Setenv(api.EnvInternalListen, "")

		Expect(internallisten.AdminListener(internallisten.Config())).To(Equal(api.AdminListenerAPI))
	})

	It("moves to the internal listener when one is configured", func() {
		GinkgoT().Setenv(api.EnvAdminListener, "internal")
		GinkgoT().Setenv(api.EnvInternalListen, api.DefaultInternalListen)
		GinkgoT().Setenv(api.EnvInternalToken, "a-token")

		Expect(internallisten.AdminListener(internallisten.Config())).To(Equal(api.AdminListenerInternal))
	})

	It("refuses to start with admin routes on an internal listener that is not configured", func() {
		GinkgoT().Setenv(api.EnvAdminListener, "internal")
		GinkgoT().Setenv(api.EnvInternalListen, "")

		_, err := internallisten.AdminListener(internallisten.Config())
		Expect(err).To(MatchError(And(
			ContainSubstring(api.EnvAdminListener),
			ContainSubstring(api.EnvInternalListen),
		)))
	})

	It("refuses an unknown value", func() {
		GinkgoT().Setenv(api.EnvAdminListener, "public")

		_, err := internallisten.AdminListener(internallisten.Config())
		Expect(err).To(HaveOccurred())
	})
})

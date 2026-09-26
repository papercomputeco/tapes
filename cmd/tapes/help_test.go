package tapescmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	servecmder "github.com/papercomputeco/tapes/cmd/tapes/serve"
	deriveworkercmder "github.com/papercomputeco/tapes/cmd/tapes/serve/deriveworker"
)

// The embed worker was removed from core when search moved to its cassette
// (#320). These specs pin the help surface so the retired command cannot be
// re-advertised: every mention is a copy-paste trap for operators.
var _ = Describe("retired embed worker", func() {
	It("is not advertised in the root help", func() {
		Expect(tapesLongDesc).NotTo(ContainSubstring("embed-worker"))
	})

	It("is not a serve subcommand", func() {
		subs := servecmder.NewServeCmd().Commands()
		names := make([]string, 0, len(subs))
		for _, sub := range subs {
			names = append(names, sub.Name())
		}
		Expect(names).NotTo(ContainElement("embed-worker"))
	})

	It("is not referenced by the derive worker help", func() {
		Expect(deriveworkercmder.NewDeriveWorkerCmd().Long).NotTo(ContainSubstring("embed-worker"))
	})
})

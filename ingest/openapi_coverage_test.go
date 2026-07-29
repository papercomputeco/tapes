package ingest

// Coverage gate for the ingest write surface's published contract.
//
// This one matters more than its size suggests: every capture path — extproc,
// tapesctl, paperd — writes this envelope, and "identical fidelity whichever
// path captured it" is unenforceable if the shape they must agree on drifts
// away from the document they read.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/openapicheck"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var ingestExemptions = openapicheck.Exemptions{
	Undocumented: map[string]string{
		"/metrics": "Prometheus exposition — scraped by convention, not called by clients",
		"/openapi": "serves the contract; an operation whose response is the document it appears in is circular",
	},
}

var _ = Describe("Ingest OpenAPI route coverage", func() {
	It("publishes exactly the surface it serves", func(ctx SpecContext) {
		server, err := New(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		contract, err := server.OpenAPIParser().Compile(ctx)
		Expect(err).NotTo(HaveOccurred())

		res := openapicheck.Check(
			openapicheck.FromFiberRoutes(server.server),
			contract,
			ingestExemptions,
		)
		Expect(res.OK()).To(BeTrue(), res.Explain("the compiled ingest contract"))
	})
})

package ingest

// Coverage gate for the ingest write surface's published contract.
//
// This one matters more than its size suggests: every capture path — extproc,
// tapesctl, paperd — writes this envelope, and "identical fidelity whichever
// path captured it" is unenforceable if the shape they must agree on drifts
// away from the document they read.

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/openapicheck"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var ingestExemptions = openapicheck.Exemptions{
	Undocumented: map[string]string{
		"/metrics": "Prometheus exposition — scraped by convention, not called by clients",
	},
}

var _ = Describe("Ingest OpenAPI route coverage", func() {
	It("publishes exactly the surface it serves", func() {
		server, err := New(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		// Read from disk rather than embedding: nothing serves this contract,
		// so embedding it would put bytes in every binary that links ingest for
		// the sake of one test.
		spec, err := os.ReadFile("openapi.yaml")
		Expect(err).NotTo(HaveOccurred(), "ingest/openapi.yaml is missing; run `make openapi`")

		res, err := openapicheck.Check(
			openapicheck.FromFiberRoutes(server.server),
			spec,
			ingestExemptions,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.OK()).To(BeTrue(), res.Explain("ingest/openapi.yaml"))
	})
})

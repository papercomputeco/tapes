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

	It("documents the 413 body-limit response for ingest operations", func(ctx SpecContext) {
		server, err := New(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		contract, err := server.OpenAPIParser().Compile(ctx)
		Expect(err).NotTo(HaveOccurred())

		tree := contract.Tree()
		for _, path := range []string{"/v1/ingest", "/v1/ingest/transcript"} {
			responses := treeAt(tree, "paths", path, "post", "responses")
			Expect(treeAt(responses, "413")).NotTo(BeNil(),
				"POST %s does not document the 413 body-limit rejection", path)
			// Same schema as the surface's other rejections, so adapters can
			// parse every error from this surface uniformly.
			Expect(treeAt(responses, "413", "content", "application/json", "schema")).
				To(Equal(treeAt(responses, "400", "content", "application/json", "schema")),
					"POST %s must document the 413 body with the shared error schema", path)
		}
	})
})

// treeAt walks nested map[string]any keys, returning nil when any is absent.
func treeAt(node any, keys ...string) any {
	for _, key := range keys {
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[key]
	}
	return node
}

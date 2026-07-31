package api

// Coverage gate for the read API's published contract. The rule and its
// rendering live in internal/openapicheck; this file supplies only what is
// specific to this server — its routes and its exemptions.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/internal/openapicheck"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var apiExemptions = openapicheck.Exemptions{
	Undocumented: map[string]string{
		"/":                                 "optional web UI; HTML, not an API surface",
		"/metrics":                          "Prometheus exposition — scraped by convention, not called by clients",
		"/openapi":                          "serves the contract; an operation whose response is the document it appears in is circular",
		"/swagger":                          "the spec viewer itself; HTML, not an API surface",
		"/v1/cassettes/{name}":              "reverse-proxy mount; concrete cassette operations are published in the merged spec",
		"/v1/cassettes/{name}/*":            "reverse-proxy mount; concrete cassette operations are published in the merged spec",
		"/v1/cassettes/{name}/openapi.json": "serves a cassette's own spec; describing it in the merged one is circular",
	},
	AllMounted: map[string]string{
		"/v1/mcp": "app.All mount; the MCP transport implements GET, POST, and DELETE",
	},
	Conditional: map[string]string{
		"/": "mounted only when Config.EnableWebUI is set",
	},
}

var _ = Describe("OpenAPI route coverage", func() {
	It("publishes exactly the surface it serves", func(ctx SpecContext) {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		// Compiled from the same parser /openapi serves, so this compares the
		// route table against the document a client would actually receive —
		// not against a file that agreed with the routes at generation time.
		contract, err := server.OpenAPIParser().Compile(ctx)
		Expect(err).NotTo(HaveOccurred())

		res := openapicheck.Check(
			openapicheck.FromFiberRoutes(server.app),
			contract,
			apiExemptions,
		)
		Expect(res.OK()).To(BeTrue(), res.Explain("the compiled read API contract"))
	})
})

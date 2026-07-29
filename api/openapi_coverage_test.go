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
		"/openapi":                          "serves the dynamically merged spec; describing it in the static spec is circular",
		"/swagger":                          "the spec viewer itself",
		"/swagger/doc.json":                 "serves the spec; describing it in the spec is circular",
		"/swagger/openapi.yaml":             "serves the spec; describing it in the spec is circular",
		"/v1/cassettes/{name}":              "reverse-proxy mount; concrete cassette operations are published in the merged spec",
		"/v1/cassettes/{name}/*":            "reverse-proxy mount; concrete cassette operations are published in the merged spec",
		"/v1/cassettes/{name}/openapi.json": "serves a cassette spec; describing it in the static spec is circular",
	},
	AllMounted: map[string]string{
		"/v1/mcp": "app.All mount; the MCP transport implements GET, POST, and DELETE",
	},
	Conditional: map[string]string{
		"/": "mounted only when Config.EnableWebUI is set",
	},
}

var _ = Describe("OpenAPI route coverage", func() {
	It("publishes exactly the surface it serves", func() {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		res, err := openapicheck.Check(
			openapicheck.FromFiberRoutes(server.app),
			OpenAPISpec(),
			apiExemptions,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.OK()).To(BeTrue(), res.Explain("api/openapi.yaml"))
	})
})

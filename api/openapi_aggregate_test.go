package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// cassetteDocument is a hello-world-shaped cassette spec: a manifest core can
// admit, one operation under the cassette's own prefix, and a component to
// namespace.
func cassetteDocument(name string) string {
	return fmt.Sprintf(`{
  "openapi": "3.0.3",
  "info": {"title": %q, "version": "0.0.1"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": %q, "version": "0.0.1"},
    "depends": {"core": "v1"},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"}
  },
  "paths": {
    "/api/%s/hello": {
      "get": {
        "operationId": "getHello",
        "summary": "Greet",
        "responses": {"200": {"description": "the greeting", "content": {"application/json": {
          "schema": {"$ref": "#/components/schemas/Row"}}}}}
      }
    }
  },
  "components": {"schemas": {"Row": {"type": "object", "properties": {"hello": {"type": "string"}}}}}
}`, name, name, name)
}

var _ = Describe("GET /openapi", func() {
	// This is the endpoint a client points a code generator or an API explorer
	// at to learn what this origin serves. That means all of it: core's own
	// surface and every cassette reverse-proxied under it. Publishing only the
	// cassettes — which is what this endpoint used to do — described the
	// smaller and stranger half of the origin.
	fetch := func(server *Server) map[string]any {
		request := httptest.NewRequest(http.MethodGet, "/openapi", nil)
		response, err := server.app.Test(request, -1)
		Expect(err).NotTo(HaveOccurred())
		defer response.Body.Close()

		Expect(response.StatusCode).To(Equal(http.StatusOK))
		body, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())

		var document map[string]any
		Expect(json.Unmarshal(body, &document)).To(Succeed())

		return document
	}

	paths := func(document map[string]any) map[string]any {
		out, ok := document["paths"].(map[string]any)
		Expect(ok).To(BeTrue(), "the aggregate always carries a paths object")

		return out
	}

	It("publishes core's own surface with no cassettes installed", func() {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		document := fetch(server)
		Expect(document["openapi"]).To(Equal("3.0.3"))
		Expect(paths(document)).To(HaveKey("/v1/sessions"))
		Expect(paths(document)).To(HaveKey("/v1/traces/{trace_id}/spans/{span_id}"))
	})

	It("publishes core's routes alongside an installed cassette's", func() {
		upstream := httptest.NewServer(http.HandlerFunc(
			func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(writer, cassetteDocument("hello-world"))
			}))
		DeferCleanup(upstream.Close)

		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		server.SetCassetteSources([]string{upstream.URL + "/openapi"})
		Expect(server.RefreshCassetteSpecs(GinkgoT().Context())).To(BeEmpty())

		document := fetch(server)

		Expect(paths(document)).To(HaveKey("/v1/sessions"),
			"core's own operations stay in the document a cassette joins")
		Expect(paths(document)).To(HaveKey("/v1/cassettes/hello-world/hello"),
			"the cassette is published at the public path clients reach it on, "+
				"not the path it serves on its own listener")

		// operationIds are namespaced per cassette for the same reason components
		// are: two cassettes may each have named an operation `getHello`, and an
		// id has to be unique across the document it appears in, or the generator
		// this endpoint exists to feed refuses the whole thing.
		//
		// The rename is confined to the aggregate. A client generated against the
		// cassette's own document — served verbatim from
		// /v1/cassettes/{name}/openapi.json — still calls it `getHello`.
		hello := paths(document)["/v1/cassettes/hello-world/hello"].(map[string]any)
		Expect(hello["get"].(map[string]any)["operationId"]).To(Equal("hello_world_getHello"))

		// Components are namespaced per cassette, because OpenAPI's component
		// space is flat and two cassettes may each define a `Row`.
		schemas := document["components"].(map[string]any)["schemas"].(map[string]any)
		Expect(schemas).To(HaveKey("hello_world_Row"))
		Expect(schemas).To(HaveKey("SessionItem"), "core's own components are here too")

		reference := hello["get"].(map[string]any)["responses"].(map[string]any)["200"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)["$ref"]
		Expect(reference).To(Equal("#/components/schemas/hello_world_Row"))
	})

	It("does not carry a cassette's manifest into the aggregate", func() {
		upstream := httptest.NewServer(http.HandlerFunc(
			func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(writer, cassetteDocument("hello-world"))
			}))
		DeferCleanup(upstream.Close)

		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		server.SetCassetteSources([]string{upstream.URL + "/openapi"})
		Expect(server.RefreshCassetteSpecs(GinkgoT().Context())).To(BeEmpty())

		// x-tapes-cassette describes one cassette, not the origin. Hoisting it
		// to the root of a document describing several would say this whole API
		// is that cassette.
		Expect(fetch(server)).NotTo(HaveKey("x-tapes-cassette"))
	})
})

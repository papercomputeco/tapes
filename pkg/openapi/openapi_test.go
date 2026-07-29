package openapi_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/openapi"
)

var _ = Describe("Document", func() {
	It("parses, preserves extensions, and rewrites contained paths", func() {
		document, err := openapi.Parse([]byte(`{"openapi":"3.1.0","x-example":{"n":1},"servers":[{"url":"http://internal"}],"paths":{"/api/demo/ping":{}}}`))
		Expect(err).NotTo(HaveOccurred())
		extension, ok, err := document.Extension("x-example")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(extension).To(MatchJSON(`{"n":1}`))
		published, err := document.RewritePrefix("/api/demo", "/v1/demo")
		Expect(err).NotTo(HaveOccurred())
		encoded, err := published.Marshal()
		Expect(err).NotTo(HaveOccurred())
		var decoded map[string]any
		Expect(json.Unmarshal(encoded, &decoded)).To(Succeed())
		Expect(decoded).NotTo(HaveKey("servers"))
		Expect(decoded["paths"]).To(HaveKey("/v1/demo/ping"))
	})

	It("refuses ambiguous or trailing JSON and paths outside the declared prefix", func() {
		_, err := openapi.Parse([]byte(`{"paths":{},"paths":{}}`))
		Expect(err).To(MatchError(ContainSubstring(`duplicate object key "paths"`)))
		_, err = openapi.Parse([]byte(`{} {}`))
		Expect(err).To(HaveOccurred())
		document, err := openapi.Parse([]byte(`{"paths":{"/escape":{}}}`))
		Expect(err).NotTo(HaveOccurred())
		_, err = document.RewritePrefix("/api/demo", "/public")
		Expect(err).To(MatchError(ContainSubstring("outside")))
	})

	It("merges arbitrary namespaced documents without knowing their domain", func() {
		one, err := openapi.Parse([]byte(`{
			"paths":{"/one":{"get":{"responses":{"200":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Row"}}}}}}}},
			"components":{"schemas":{"Row":{"type":"string"}}}
		}`))
		Expect(err).NotTo(HaveOccurred())
		two, err := openapi.Parse([]byte(`{
			"paths":{"/two":{"get":{"responses":{"200":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Row"}}}}}}}},
			"components":{"schemas":{"Row":{"type":"integer"}}}
		}`))
		Expect(err).NotTo(HaveOccurred())

		merged, err := openapi.Merge("example", "v1", map[string]*openapi.Document{"one": one, "two": two})
		Expect(err).NotTo(HaveOccurred())
		var decoded map[string]any
		Expect(json.Unmarshal(merged, &decoded)).To(Succeed())
		components := decoded["components"].(map[string]any)["schemas"].(map[string]any)
		Expect(components).To(HaveKey("one_Row"))
		Expect(components).To(HaveKey("two_Row"))
		Expect(string(merged)).To(ContainSubstring(`#/components/schemas/one_Row`))
		Expect(string(merged)).To(ContainSubstring(`#/components/schemas/two_Row`))
	})
})

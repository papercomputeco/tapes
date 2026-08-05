package tapesoapi_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

var _ = Describe("StandaloneRequestSchema", func() {
	load := func(schema, components string) *tapesoapi.Document {
		GinkgoHelper()
		document, err := tapesoapi.Parse([]byte(`{
			"openapi":"3.1.0",
			"paths":{"/run":{"post":{"requestBody":{"content":{"application/json":{"schema":` + schema + `}}}}}},
			"components":{"schemas":` + components + `}
		}`))
		Expect(err).NotTo(HaveOccurred())
		return document
	}

	It("bundles reachable component references, preserves keywords, and handles cycles", func() {
		document := load(
			`{"$ref":"#/components/schemas/Input"}`,
			`{
				"Input":{"type":"object","properties":{"child":{"$ref":"#/components/schemas/Child"}},"if":{"required":["child"]}},
				"Child":{"type":"object","properties":{"parent":{"$ref":"#/components/schemas/Input"}}}
			}`,
		)

		schema, err := document.StandaloneRequestSchema("POST", "/run", "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(schema).To(HaveKeyWithValue("$ref", "#/$defs/Input"))
		definitions := schema["$defs"].(map[string]any)
		Expect(definitions).To(HaveKey("Input"))
		Expect(definitions).To(HaveKey("Child"))
		Expect(definitions["Input"].(map[string]any)).To(HaveKey("if"))
	})

	It("resolves escaped component names", func() {
		document := load(
			`{"$ref":"#/components/schemas/a~1b~0c"}`,
			`{"a/b~c":{"type":"object"}}`,
		)

		schema, err := document.StandaloneRequestSchema("POST", "/run", "application/json")
		Expect(err).NotTo(HaveOccurred())
		Expect(schema).To(HaveKeyWithValue("$ref", "#/$defs/a~1b~0c"))
		Expect(schema["$defs"]).To(HaveKey("a/b~c"))
	})

	It("refuses references a standalone schema cannot resolve", func() {
		document := load(`{"$ref":"https://example.com/schema.json"}`, `{}`)
		_, err := document.StandaloneRequestSchema("POST", "/run", "application/json")
		Expect(err).To(MatchError(ContainSubstring("only local")))

		document = load(`{"$ref":"#/components/schemas/Missing"}`, `{}`)
		_, err = document.StandaloneRequestSchema("POST", "/run", "application/json")
		Expect(err).To(MatchError(ContainSubstring("does not exist")))
	})
})

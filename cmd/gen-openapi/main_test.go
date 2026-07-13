package main

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestGenOpenAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gen-openapi Suite")
}

var _ = Describe("synthesizeID", func() {
	DescribeTable("builds a deterministic camel-case id from method + path",
		func(method, path, want string) {
			Expect(synthesizeID(method, path)).To(Equal(want))
		},
		Entry("root", "get", "/ping", "getPing"),
		Entry("nested with param", "get", "/v1/sessions/{id}", "getV1SessionsId"),
		Entry("multi param", "get", "/v1/traces/{trace_id}/spans/{span_id}", "getV1TracesTraceIdSpansSpanId"),
		Entry("uppercases method", "DELETE", "/v1/sessions/{id}", "deleteV1SessionsId"),
	)
})

var _ = Describe("bareName", func() {
	It("returns the substring after the final dot", func() {
		Expect(bareName("api.SessionItem")).To(Equal("SessionItem"))
		Expect(bareName("github_com_papercomputeco_tapes_pkg_llm.ErrorResponse")).To(Equal("ErrorResponse"))
	})
	It("returns the whole name when there is no dot", func() {
		Expect(bareName("SessionItem")).To(Equal("SessionItem"))
	})
})

var _ = Describe("stripSchemaPrefixes", func() {
	It("renames unique schemas and rewrites every $ref", func() {
		tree := map[string]any{
			"components": map[string]any{
				"schemas": map[string]any{
					"api.SessionItem": map[string]any{
						"properties": map[string]any{
							"usage": map[string]any{"$ref": "#/components/schemas/api.ModelUsage"},
						},
					},
					"api.ModelUsage":        map[string]any{"type": "object"},
					"pkg_llm.ErrorResponse": map[string]any{"type": "object"},
				},
			},
			"paths": map[string]any{
				"/v1/sessions/{id}": map[string]any{
					"get": map[string]any{
						"responses": map[string]any{
							"200": map[string]any{
								"content": map[string]any{
									"application/json": map[string]any{
										"schema": map[string]any{"$ref": "#/components/schemas/api.SessionItem"},
									},
								},
							},
							"400": map[string]any{
								"content": map[string]any{
									"application/json": map[string]any{
										"schema": map[string]any{"$ref": "#/components/schemas/pkg_llm.ErrorResponse"},
									},
								},
							},
						},
					},
				},
			},
		}

		Expect(stripSchemaPrefixes(tree)).To(Succeed())

		schemas := tree["components"].(map[string]any)["schemas"].(map[string]any)
		Expect(schemas).To(HaveKey("SessionItem"))
		Expect(schemas).To(HaveKey("ModelUsage"))
		Expect(schemas).To(HaveKey("ErrorResponse"))
		Expect(schemas).NotTo(HaveKey("api.SessionItem"))

		// Nested ref inside a renamed schema is rewritten.
		nested := schemas["SessionItem"].(map[string]any)["properties"].(map[string]any)["usage"].(map[string]any)
		Expect(nested["$ref"]).To(Equal("#/components/schemas/ModelUsage"))

		// Refs deep in the paths tree are rewritten.
		get := tree["paths"].(map[string]any)["/v1/sessions/{id}"].(map[string]any)["get"].(map[string]any)
		resps := get["responses"].(map[string]any)
		okRef := resps["200"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)["$ref"]
		Expect(okRef).To(Equal("#/components/schemas/SessionItem"))
		errRef := resps["400"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)["$ref"]
		Expect(errRef).To(Equal("#/components/schemas/ErrorResponse"))
	})

	It("keeps qualified names when two schemas would collapse onto the same bare name", func() {
		tree := map[string]any{
			"components": map[string]any{
				"schemas": map[string]any{
					"pkg_seed.Result": map[string]any{"type": "object"},
					"pkg_other.Result": map[string]any{
						"properties": map[string]any{
							"nested": map[string]any{"$ref": "#/components/schemas/pkg_seed.Result"},
						},
					},
				},
			},
		}

		Expect(stripSchemaPrefixes(tree)).To(Succeed())

		schemas := tree["components"].(map[string]any)["schemas"].(map[string]any)
		// Both collide on "Result", so both retain their qualified keys.
		Expect(schemas).To(HaveKey("pkg_seed.Result"))
		Expect(schemas).To(HaveKey("pkg_other.Result"))
		Expect(schemas).NotTo(HaveKey("Result"))

		// The colliding ref is left untouched.
		nested := schemas["pkg_other.Result"].(map[string]any)["properties"].(map[string]any)["nested"].(map[string]any)
		Expect(nested["$ref"]).To(Equal("#/components/schemas/pkg_seed.Result"))
	})
})

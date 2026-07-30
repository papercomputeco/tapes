package tapesoapi_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
	v30 "github.com/papercomputeco/tapes/pkg/tapesoapi/v3.0"
	v31 "github.com/papercomputeco/tapes/pkg/tapesoapi/v3.1"
)

var _ = Describe("Compile", func() {
	newParser := func(options ...tapesoapi.Option) *tapesoapi.Parser {
		return tapesoapi.NewParser(append([]tapesoapi.Option{
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Aggregate", Version: "1.0.0"}),
		}, options...)...)
	}

	ingest := func(parser *tapesoapi.Parser, fixture []byte, options ...tapesoapi.DocOption) {
		GinkgoHelper()
		Expect(parser.AddDocument(ctx(), fixture,
			append([]tapesoapi.DocOption{tapesoapi.WithoutInfo()}, options...)...)).To(Succeed())
	}

	It("round-trips a complete document", func() {
		parser := newParser()
		ingest(parser, v30.Read(v30.Petstore))

		tree := compileTree(parser)
		Expect(tree["openapi"]).To(Equal("3.0.3"))
		Expect(object(tree, "paths")).To(HaveKey("/pets"))
		Expect(object(tree, "paths")).To(HaveKey("/pets/{petId}"))
		Expect(at(tree, "paths", "/pets", "get", "operationId")).To(Equal("listPets"))
		Expect(object(tree, "components", "schemas")).To(HaveKeys("Pet", "Pets", "Error"))

		// A `default` response key is not a status code and must survive as
		// itself.
		Expect(object(tree, "paths", "/pets", "get", "responses")).To(HaveKey("default"))
	})

	It("produces byte-identical output for the same inputs", func() {
		first := newParser()
		ingest(first, v30.Read(v30.Petstore))
		second := newParser()
		ingest(second, v30.Read(v30.Petstore))

		one, err := first.Compile(ctx())
		Expect(err).NotTo(HaveOccurred())
		two, err := second.Compile(ctx())
		Expect(err).NotTo(HaveOccurred())

		// Determinism is what makes the generated contracts diffable in review
		// and cacheable behind an ETag.
		Expect(two.Fingerprint()).To(Equal(one.Fingerprint()))
		Expect(two.JSON()).To(Equal(one.JSON()))
	})

	It("does not depend on the order documents were added", func() {
		forwards := newParser(tapesoapi.WithConflictPolicy(tapesoapi.PolicyLastWins))
		ingest(forwards, v30.Read(v30.ConflictLeft), tapesoapi.WithComponentNamespace("left_"))
		ingest(forwards, v30.Read(v30.ConflictRight), tapesoapi.WithComponentNamespace("right_"))

		backwards := newParser(tapesoapi.WithConflictPolicy(tapesoapi.PolicyLastWins))
		ingest(backwards, v30.Read(v30.ConflictRight), tapesoapi.WithComponentNamespace("right_"))
		ingest(backwards, v30.Read(v30.ConflictLeft), tapesoapi.WithComponentNamespace("left_"))

		// Lint is off here: last-wins drops one of the colliding `/items`
		// operations, which orphans the component only that operation
		// referenced. That is the policy working, not a defect in the output
		// this spec is about.
		one, err := forwards.Compile(ctx(), tapesoapi.WithLint())
		Expect(err).NotTo(HaveOccurred())
		two, err := backwards.Compile(ctx(), tapesoapi.WithLint())
		Expect(err).NotTo(HaveOccurred())

		// Fragments merge in provenance order rather than insertion order, so
		// concurrent route registration cannot reorder the output.
		Expect(two.Fingerprint()).To(Equal(one.Fingerprint()))
	})

	It("compiles an empty parser rather than failing", func() {
		// The aggregate endpoint must answer before any cassette has resolved.
		tree := compileTree(tapesoapi.NewParser())
		Expect(tree["openapi"]).To(Equal("3.0.3"))
		Expect(object(tree, "paths")).To(BeEmpty())
	})

	Describe("references", func() {
		It("rewrites every component section under a namespace", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.ComponentsAndRefs),
				tapesoapi.WithComponentNamespace("acme_"))

			tree := compileTree(parser)
			components := object(tree, "components")

			Expect(object(components, "schemas")).To(HaveKeys("acme_Widget", "acme_Part", "acme_Problem"))
			Expect(object(components, "parameters")).To(HaveKey("acme_WidgetId"))
			Expect(object(components, "responses")).To(HaveKeys("acme_Widget", "acme_NotFound"))
			Expect(object(components, "requestBodies")).To(HaveKey("acme_Widget"))
			Expect(object(components, "headers")).To(HaveKey("acme_RequestId"))

			// A rewrite that renames the keys but not the referrers compiles
			// into a document whose every reference dangles.
			Expect(at(tree, "paths", "/widgets/{widgetId}", "get", "responses", "200", "$ref")).
				To(Equal("#/components/responses/acme_Widget"))
			Expect(at(tree, "paths", "/widgets/{widgetId}", "put", "requestBody", "$ref")).
				To(Equal("#/components/requestBodies/acme_Widget"))
		})

		It("rewrites example references, in components and in media types", func() {
			parser := newParser()
			ingest(parser, []byte(`{
				"openapi":"3.0.3",
				"info":{"title":"x","version":"0"},
				"paths":{"/things":{"get":{"operationId":"listThings","responses":{"200":{
					"description":"ok",
					"content":{"application/json":{
						"schema":{"type":"string"},
						"examples":{"shared":{"$ref":"#/components/examples/Canonical"}}
					}}
				}}}}},
				"components":{"examples":{
					"Canonical":{"value":"the one true example"},
					"Alias":{"$ref":"#/components/examples/Canonical"}
				}}
			}`), tapesoapi.WithComponentNamespace("acme_"))

			tree := compileTree(parser)
			examples := object(object(tree, "components"), "examples")
			Expect(examples).To(HaveKeys("acme_Canonical", "acme_Alias"))

			// An example that is itself a Reference Object must follow its
			// renamed target, or the aggregate ships a dangling reference —
			// or worse, one that resolves to another cassette's component of
			// the same pre-namespace name.
			Expect(at(tree, "components", "examples", "acme_Alias", "$ref")).
				To(Equal("#/components/examples/acme_Canonical"))
			Expect(at(tree, "paths", "/things", "get", "responses", "200",
				"content", "application/json", "examples", "shared", "$ref")).
				To(Equal("#/components/examples/acme_Canonical"))
		})

		It("rewrites a discriminator mapping, whose values are references", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.DiscriminatedUnion),
				tapesoapi.WithComponentNamespace("events_"))

			mapping := object(compileTree(parser),
				"components", "schemas", "events_Event", "discriminator", "mapping")
			Expect(mapping["created"]).To(Equal("#/components/schemas/events_Created"))
			Expect(mapping["deleted"]).To(Equal("#/components/schemas/events_Deleted"))
		})

		It("rewrites a reference buried in a vendor extension", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.VendorExtensions),
				tapesoapi.WithComponentNamespace("vendor_"))

			tree := compileTree(parser)
			Expect(at(tree, "components", "schemas", "vendor_Thing", "x-vendor-ref", "$ref")).
				To(Equal("#/components/schemas/vendor_Thing"))
		})

		It("preserves vendor extensions at every level", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.VendorExtensions))

			tree := compileTree(parser)
			Expect(tree).To(HaveKeyWithValue("x-root-level", "kept"))
			Expect(object(tree, "paths", "/things")).To(HaveKeyWithValue("x-path-level", "kept"))
			Expect(object(tree, "paths", "/things", "get")).
				To(HaveKeyWithValue("x-operation-level", "kept"))
			Expect(object(tree, "paths", "/things", "get", "responses", "200")).
				To(HaveKeyWithValue("x-response-level", "kept"))
			Expect(object(tree, "components", "schemas", "Thing")).
				To(HaveKeyWithValue("x-schema-level", "kept"))
		})

		It("fails on a reference nothing defines", func() {
			parser := newParser()
			Expect(parser.AddOperation("GET", "/dangling",
				tapesoapi.NewOperation("dangling").
					JSONResponse(200, "a thing", tapesoapi.SchemaRef("Missing")).Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindManual, Name: "spec"})).To(Succeed())

			_, err := parser.Compile(ctx())
			Expect(err).To(MatchError(ContainSubstring(
				"GET /dangling references #/components/schemas/Missing")))
		})

		It("leaves a reference into another document alone", func() {
			parser := newParser()
			ingest(parser, []byte(`{
				"openapi":"3.0.3",
				"paths":{"/remote":{"get":{"operationId":"remote","responses":{
					"200":{"$ref":"https://example.com/spec.yaml#/components/responses/Thing"}}}}}
			}`))

			// This package resolves nothing over the network, so a remote
			// reference is not its to check — and following one would let an
			// ingested document choose what gets fetched.
			tree := compileTree(parser, tapesoapi.WithoutValidation())
			Expect(at(tree, "paths", "/remote", "get", "responses", "200", "$ref")).
				To(Equal("https://example.com/spec.yaml#/components/responses/Thing"))
		})
	})

	Describe("path mounting", func() {
		It("mounts a whole document under a prefix", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.Petstore), tapesoapi.WithPathPrefix("/v2"))

			paths := object(compileTree(parser), "paths")
			Expect(paths).To(HaveKeys("/v2/pets", "/v2/pets/{petId}"))
			Expect(paths).NotTo(HaveKey("/pets"))
		})

		It("normalizes a trailing slash so one route cannot appear twice", func() {
			parser := newParser()
			Expect(parser.AddOperation("GET", "/things/",
				tapesoapi.NewOperation("things").EmptyResponse(204, "no content").Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindManual, Name: "spec"})).To(Succeed())

			Expect(object(compileTree(parser), "paths")).To(HaveKey("/things"))
		})

		It("refuses a path that is not absolute", func() {
			err := tapesoapi.NewParser().AddOperation("GET", "things",
				tapesoapi.NewOperation("things").Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindManual, Name: "spec"})
			Expect(err).To(MatchError(ContainSubstring("must begin with /")))
		})

		It("refuses a method OpenAPI has no slot for", func() {
			err := tapesoapi.NewParser().AddOperation("CONNECT", "/things",
				tapesoapi.NewOperation("things").Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindManual, Name: "spec"})
			Expect(err).To(MatchError(ContainSubstring("no OpenAPI operation slot")))
		})
	})

	Describe("version targeting", func() {
		It("refuses to silently downgrade 3.1-only constructs", func() {
			parser := newParser()
			ingest(parser, v31.Read(v31.WebhooksAndConst))

			_, err := parser.Compile(ctx())
			Expect(err).To(MatchError(ContainSubstring("would lose meaning")))
			// The error names what would be lost, so the reader does not have
			// to bisect the document to find out.
			Expect(err).To(MatchError(ContainSubstring("webhooks")))
			Expect(err).To(MatchError(ContainSubstring("const")))
		})

		It("approximates them when the downgrade is asked for explicitly", func() {
			parser := newParser()
			ingest(parser, v31.Read(v31.WebhooksAndConst))

			tree := compileTree(parser, tapesoapi.WithDowngradeLossy(), tapesoapi.WithoutValidation())

			// `const` has no 3.0 spelling; a single-member enum is exact for
			// validation and is the closest thing 3.0 has.
			Expect(at(tree, "components", "schemas", "Order", "properties", "kind", "enum")).
				To(Equal([]any{"order"}))
			// Webhooks have no 3.0 equivalent at all, so they are dropped
			// rather than mangled into a path.
			Expect(tree).NotTo(HaveKey("webhooks"))
		})

		It("renders 3.1 when asked", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.NullableAndBounds))

			tree := compileTree(parser, tapesoapi.WithTarget(tapesoapi.V31))
			Expect(tree["openapi"]).To(Equal("3.1.0"))

			values := object(tree, "components", "schemas", "Values", "properties")
			Expect(object(values, "nullableString")["type"]).To(Equal([]any{"string", "null"}))
			Expect(object(values, "exclusiveLower")["exclusiveMinimum"]).To(BeEquivalentTo(0))
			Expect(object(values, "exclusiveLower")).NotTo(HaveKey("minimum"))
		})

		It("rejects an unsupported target", func() {
			_, err := tapesoapi.NewParser().Compile(ctx(), tapesoapi.WithTarget("2.0"))
			Expect(err).To(MatchError(ContainSubstring("unsupported compile target")))
		})
	})

	Describe("lint", func() {
		It("rejects an operation with no operationId", func() {
			parser := newParser()
			Expect(parser.AddOperation("GET", "/thing",
				tapesoapi.NewOperation("").EmptyResponse(204, "no content").Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindRoute, Name: "GET /thing"})).To(Succeed())

			// A downstream generator hard-fails without one, so this must not
			// reach a published contract.
			_, err := parser.Compile(ctx())
			Expect(err).To(MatchError(ContainSubstring("has no operationId")))
		})

		It("rejects two operations sharing an operationId", func() {
			parser := newParser()
			for _, path := range []string{"/one", "/two"} {
				Expect(parser.AddOperation("GET", path,
					tapesoapi.NewOperation("duplicated").EmptyResponse(204, "no content").Build(),
					tapesoapi.Provenance{Kind: tapesoapi.KindRoute, Name: "GET " + path})).To(Succeed())
			}

			_, err := parser.Compile(ctx())
			Expect(err).To(MatchError(ContainSubstring(`operationId "duplicated" is used by`)))
		})

		It("reports a component nothing references", func() {
			parser := newParser()
			Expect(parser.AddComponentSchema("Orphan", tapesoapi.Object(nil),
				tapesoapi.Provenance{Kind: tapesoapi.KindManual, Name: "spec"})).To(Succeed())

			_, err := parser.Compile(ctx())
			Expect(err).To(MatchError(ContainSubstring("schemas/Orphan is defined but never referenced")))
		})

		It("does not call a transitively referenced component an orphan", func() {
			parser := newParser()
			ingest(parser, v30.Read(v30.Petstore))

			// `Pet` is reachable only through `Pets`. A one-pass reference
			// count would call it unused and delete a type clients depend on.
			_, err := parser.Compile(ctx())
			Expect(err).NotTo(HaveOccurred())
		})

		It("can be turned off for a document this module did not write", func() {
			parser := newParser()
			Expect(parser.AddOperation("GET", "/thing",
				tapesoapi.NewOperation("").EmptyResponse(204, "no content").Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindRoute, Name: "GET /thing"})).To(Succeed())

			_, err := parser.Compile(ctx(), tapesoapi.WithLint())
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("freezing", func() {
		It("refuses a contribution after the surface is fixed", func() {
			parser := newParser()
			parser.Freeze()

			err := parser.AddOperation("GET", "/late",
				tapesoapi.NewOperation("late").Build(),
				tapesoapi.Provenance{Kind: tapesoapi.KindRoute, Name: "GET /late"})
			// A route registered after startup silently changes a document
			// already served, so it fails at the registration site instead.
			Expect(err).To(MatchError(tapesoapi.ErrFrozen))
		})
	})
})

// HaveKeys asserts on several map keys at once, because the alternative is four
// lines that all say the same thing.
func HaveKeys(keys ...string) OmegaMatcher {
	matchers := make([]OmegaMatcher, 0, len(keys))
	for _, key := range keys {
		matchers = append(matchers, HaveKey(key))
	}

	return SatisfyAll(matchers...)
}

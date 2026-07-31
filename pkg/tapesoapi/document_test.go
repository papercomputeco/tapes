package tapesoapi_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
	v30 "github.com/papercomputeco/tapes/pkg/tapesoapi/v3.0"
	v31 "github.com/papercomputeco/tapes/pkg/tapesoapi/v3.1"
)

var _ = Describe("Parse", func() {
	It("rejects a duplicate key rather than picking a winner", func() {
		_, err := tapesoapi.Parse([]byte(`{"openapi":"3.0.3","openapi":"3.1.0"}`))
		// Last-one-wins would make what core publishes depend on decoder
		// internals, for a document that is ambiguous on its face.
		Expect(err).To(MatchError(ContainSubstring("duplicate object key")))
	})

	It("rejects a trailing value", func() {
		_, err := tapesoapi.Parse([]byte(`{"openapi":"3.0.3"} {"openapi":"3.0.3"}`))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a document that is not an object", func() {
		_, err := tapesoapi.Parse([]byte(`["not", "a", "spec"]`))
		Expect(err).To(HaveOccurred())
	})

	It("reads YAML and JSON as the same document", func() {
		fromYAML, err := tapesoapi.ParseYAML(v30.Read(v30.Petstore))
		Expect(err).NotTo(HaveOccurred())

		encoded, err := fromYAML.Marshal()
		Expect(err).NotTo(HaveOccurred())

		fromJSON, err := tapesoapi.Parse(encoded)
		Expect(err).NotTo(HaveOccurred())

		reencoded, err := fromJSON.Marshal()
		Expect(err).NotTo(HaveOccurred())
		Expect(reencoded).To(Equal(encoded))
	})
})

var _ = Describe("Document", func() {
	load := func(fixture []byte) *tapesoapi.Document {
		GinkgoHelper()
		document, err := tapesoapi.ParseYAML(fixture)
		Expect(err).NotTo(HaveOccurred())

		return document
	}

	It("reports the version it declares", func() {
		version, err := load(v30.Read(v30.Petstore)).Version()
		Expect(err).NotTo(HaveOccurred())
		Expect(version).To(Equal(tapesoapi.V30))

		version, err = load(v31.Read(v31.NullableAndBounds)).Version()
		Expect(err).NotTo(HaveOccurred())
		Expect(version).To(Equal(tapesoapi.V31))
	})

	It("reads a root extension as JSON", func() {
		document := load([]byte(`{"openapi":"3.0.3","x-tapes-cassette":{"kind":"cassette/v1alpha1"}}`))
		encoded, present, err := document.Extension("x-tapes-cassette")
		Expect(err).NotTo(HaveOccurred())
		Expect(present).To(BeTrue())
		Expect(string(encoded)).To(Equal(`{"kind":"cassette/v1alpha1"}`))
	})

	It("distinguishes an absent extension from an empty one", func() {
		_, present, err := load(v30.Read(v30.Petstore)).Extension("x-tapes-cassette")
		Expect(err).NotTo(HaveOccurred())
		// "not a cassette spec" and "a cassette spec that does not parse" have
		// different answers, so they must be distinguishable here.
		Expect(present).To(BeFalse())
	})

	Describe("RewritePrefix", func() {
		It("moves every path and drops the publisher's servers", func() {
			document := load([]byte(`{
				"openapi":"3.0.3",
				"servers":[{"url":"http://cassette.internal:9999"}],
				"paths":{"/api/demo/ping":{},"/api/demo/things/{id}":{}}
			}`))

			rewritten, err := document.RewritePrefix("/api/demo", "/v1/cassettes/demo")
			Expect(err).NotTo(HaveOccurred())

			paths, err := rewritten.Paths()
			Expect(err).NotTo(HaveOccurred())
			Expect(paths).To(ConsistOf(
				"/v1/cassettes/demo/ping", "/v1/cassettes/demo/things/{id}"))

			encoded, err := rewritten.Marshal()
			Expect(err).NotTo(HaveOccurred())
			// The publisher's own origin is not reachable by a client that
			// reaches its paths through core's proxy.
			Expect(string(encoded)).NotTo(ContainSubstring("cassette.internal"))
		})

		It("drops servers overrides at the path item and operation level too", func() {
			document := load([]byte(`{
				"openapi":"3.0.3",
				"servers":[{"url":"http://cassette.internal:9999"}],
				"paths":{"/api/demo/ping":{
					"servers":[{"url":"http://cassette.internal:9999/item"}],
					"get":{
						"servers":[{"url":"http://cassette.internal:9999/op"}],
						"responses":{"200":{"description":"pong"}}
					}
				}}
			}`))

			rewritten, err := document.RewritePrefix("/api/demo", "/v1/cassettes/demo")
			Expect(err).NotTo(HaveOccurred())

			encoded, err := rewritten.Marshal()
			Expect(err).NotTo(HaveOccurred())
			// OpenAPI allows the override at every level, and republication is
			// exactly where none of them may survive: a generated client
			// honoring an operation-level override would call the cassette's
			// private listener and bypass the proxy entirely.
			Expect(string(encoded)).NotTo(ContainSubstring("cassette.internal"))
			// The operation itself survives the strip.
			Expect(string(encoded)).To(ContainSubstring("pong"))
		})

		It("strips servers overrides from webhooks too", func() {
			document := load([]byte(`{
				"openapi":"3.1.0",
				"paths":{"/api/demo/ping":{}},
				"webhooks":{"thing.updated":{
					"servers":[{"url":"http://cassette.internal:9999"}],
					"post":{
						"servers":[{"url":"http://cassette.internal:9999/op"}],
						"responses":{"200":{"description":"ack"}}
					}
				}}
			}`))

			rewritten, err := document.RewritePrefix("/api/demo", "/v1/cassettes/demo")
			Expect(err).NotTo(HaveOccurred())

			encoded, err := rewritten.Marshal()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(encoded)).NotTo(ContainSubstring("cassette.internal"))
			Expect(string(encoded)).To(ContainSubstring("thing.updated"))
			Expect(string(encoded)).To(ContainSubstring("ack"))
		})

		It("refuses the whole document when one path escapes the prefix", func() {
			document := load([]byte(
				`{"openapi":"3.0.3","paths":{"/api/demo/ok":{},"/v1/sessions":{}}}`))

			_, err := document.RewritePrefix("/api/demo", "/v1/cassettes/demo")
			// Partial admission would let a document claim surface its operator
			// never approved, so one bad path refuses all of them.
			Expect(err).To(MatchError(ContainSubstring(`path "/v1/sessions" is outside /api/demo`)))
		})

		It("does not treat a shared string prefix as a path prefix", func() {
			document := load([]byte(`{"openapi":"3.0.3","paths":{"/api/demonstration":{}}}`))

			_, err := document.RewritePrefix("/api/demo", "/v1/cassettes/demo")
			Expect(err).To(HaveOccurred())
		})
	})
})

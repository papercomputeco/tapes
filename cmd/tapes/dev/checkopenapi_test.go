package devcmder

// check-openapi regression: a real derived composite conforms to the
// published SessionTracesResponse schema, and a typed field corrupted to
// the wrong JSON type is caught — so the contract checker can't silently
// pass on the json.RawMessage-as-byte-array drift RFD 00007 Goal 2 warns
// about.

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("check-openapi", func() {
	const corpus = "corpus-cb9a87e5.jsonl.gz"

	It("loads the SessionTracesResponse schema from the embedded spec", func() {
		schema, err := loadTracesResponseSchema()
		Expect(err).NotTo(HaveOccurred())
		Expect(schema).NotTo(BeNil())
	})

	It("passes on a real derived composite", func() {
		schema, err := loadTracesResponseSchema()
		Expect(err).NotTo(HaveOccurred())

		v, err := validateSchemaBytes(schema, compositeFixtureBytes(corpus))
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(BeEmpty(), "real composite should conform to the OpenAPI schema: %v", v)
	})

	// Each mutation sets a typed field to the wrong JSON type; the schema
	// check must reject it.
	DescribeTable("catches a wrong-typed field",
		func(mutate func(map[string]any)) {
			schema, err := loadTracesResponseSchema()
			Expect(err).NotTo(HaveOccurred())

			var doc map[string]any
			Expect(json.Unmarshal(compositeFixtureBytes(corpus), &doc)).To(Succeed())
			mutate(doc)
			raw, err := json.Marshal(doc)
			Expect(err).NotTo(HaveOccurred())

			v, err := validateSchemaBytes(schema, raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(v).NotTo(BeEmpty(), "schema check should reject the wrong-typed field")
		},
		Entry("schema stamp as a number", func(d map[string]any) {
			d["schema"] = 123
		}),
		Entry("traces as an object", func(d map[string]any) {
			d["traces"] = map[string]any{"not": "an array"}
		}),
		Entry("span duration_ns as a string", func(d map[string]any) {
			s := firstSpan(d)
			s["duration_ns"] = "not-an-int"
		}),
	)
})

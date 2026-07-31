package manifest_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/manifest"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

func TestManifest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cassette Manifest Suite")
}

const metadata = `{
  "kind": "cassette/v1alpha1",
  "cassette": {"name": "summary", "version": "0.3.1"},
  "depends": {"core": "v1", "views": ["spans"]},
  "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"}
}`

func document(extension string) *tapesoapi.Document {
	body := `{"openapi": "3.1.0", "paths": {}}`
	if extension != "" {
		body = `{"openapi": "3.1.0", "paths": {}, "x-tapes-cassette": ` + extension + `}`
	}
	parsed, err := tapesoapi.Parse([]byte(body))
	Expect(err).NotTo(HaveOccurred())

	return parsed
}

var _ = Describe("Parse", func() {
	It("dispatches a known kind to its schema package", func() {
		parsed, err := manifest.Parse([]byte(metadata))
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.SchemaKind()).To(Equal(v1alpha1.Kind))
		Expect(parsed.CassetteName()).To(Equal(cassette.Name("summary")))
	})

	It("lifts the schema's anchors into the schema-independent form", func() {
		parsed, err := manifest.Parse([]byte(metadata))
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.Anchors()).To(Equal(cassette.Anchors{
			Health: "/ping", OpenAPI: "/openapi", Prefix: "api",
		}))
	})

	It("names the kinds it knows when handed one it does not", func() {
		_, err := manifest.Parse([]byte(`{"kind": "cassette/v9"}`))
		Expect(err).To(MatchError(ContainSubstring(`unsupported cassette metadata kind "cassette/v9"`)))
		Expect(err).To(MatchError(ContainSubstring(v1alpha1.Kind)),
			"an author who got the kind wrong needs to be told which one is right")
	})

	It("distinguishes a missing kind from a wrong one", func() {
		_, err := manifest.Parse([]byte(`{"cassette": {"name": "summary"}}`))
		Expect(err).To(MatchError(ContainSubstring("missing kind")))
	})

	It("refuses metadata that is not an object", func() {
		_, err := manifest.Parse([]byte(`["not", "metadata"]`))
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("FromDocument", func() {
	It("parses the manifest a cassette embedded in its spec", func() {
		parsed, present, err := manifest.FromDocument(document(metadata))
		Expect(err).NotTo(HaveOccurred())
		Expect(present).To(BeTrue())
		Expect(parsed.CassetteName()).To(Equal(cassette.Name("summary")))
	})

	It("reports a document with no extension as absent rather than broken", func() {
		parsed, present, err := manifest.FromDocument(document(""))
		Expect(err).NotTo(HaveOccurred())
		Expect(present).To(BeFalse())
		Expect(parsed).To(BeNil())
	})

	It("reports a present-but-unparseable manifest as present", func() {
		_, present, err := manifest.FromDocument(document(`{"kind": "cassette/v9"}`))
		Expect(err).To(HaveOccurred())
		Expect(present).To(BeTrue(),
			"a spec that tried to declare a manifest is a different problem from one that never did")
	})

	It("refuses a nil document instead of reporting it as absent", func() {
		_, _, err := manifest.FromDocument(nil)
		Expect(err).To(HaveOccurred())
	})
})

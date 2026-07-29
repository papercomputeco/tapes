package manifest_test

import (
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/manifest"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
)

// The TOML equivalent of the JSON `metadata` the rest of this suite parses.
// The two are asserted to produce the same digest, which is the property that
// makes TOML an encoding of the manifest rather than a second definition of it.
const tomlMetadata = `
kind = "cassette/v1alpha1"

[cassette]
name = "summary"
version = "0.3.1"

[depends]
core = "v1"
views = ["spans"]

[api]
health = "/ping"
openapi = "/openapi"
prefix_path = "api"
`

var _ = Describe("ParseTOML", func() {
	It("produces the same manifest the JSON encoding does", func() {
		fromTOML, err := manifest.ParseTOML([]byte(tomlMetadata))
		Expect(err).NotTo(HaveOccurred())
		fromJSON, err := manifest.Parse([]byte(metadata))
		Expect(err).NotTo(HaveOccurred())

		tomlDigest, err := fromTOML.Digest()
		Expect(err).NotTo(HaveOccurred())
		jsonDigest, err := fromJSON.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(tomlDigest).To(Equal(jsonDigest),
			"an orchestrator and core must be looking at the same document, not two that resemble each other")
	})

	It("dispatches on kind exactly as the JSON parser does", func() {
		_, err := manifest.ParseTOML([]byte(`kind = "cassette/v9"`))
		Expect(err).To(MatchError(ContainSubstring(`unsupported cassette metadata kind "cassette/v9"`)))

		_, err = manifest.ParseTOML([]byte(`[cassette]` + "\n" + `name = "summary"`))
		Expect(err).To(MatchError(ContainSubstring("missing kind")))
	})

	It("refuses a field it does not know, so a typo is not silently ignored", func() {
		// The bare key goes before the first table header: in TOML a key
		// written after one belongs to that table, not to the root.
		_, err := manifest.ParseTOML([]byte("surprise = true\n" + tomlMetadata))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "surprise"`)))

		_, err = manifest.ParseTOML([]byte(tomlMetadata + "\nsurprise = true\n"))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "api.surprise"`)),
			"the JSON parser's field path survives transcoding, so the message still locates the typo")
	})

	It("refuses a key given twice rather than picking one", func() {
		_, err := manifest.ParseTOML([]byte(`
kind = "cassette/v1alpha1"

[cassette]
name = "one"
name = "two"
`))
		Expect(err).To(MatchError(ContainSubstring("decode cassette manifest TOML")))
	})

	It("refuses TOML that is not TOML", func() {
		_, err := manifest.ParseTOML([]byte(`{"kind": "cassette/v1alpha1"}`))
		Expect(err).To(MatchError(ContainSubstring("decode cassette manifest TOML")))
	})

	It("carries integers through transcoding without widening them to floats", func() {
		parsed, err := manifest.ParseTOML([]byte(`
kind = "cassette/v1alpha1"

[cassette]
name = "summary"
version = "0.3.1"
port = 8080

[depends]
core = "v1"

[api]
health = "/ping"
openapi = "/openapi"
prefix_path = "api"
`))
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.(*v1alpha1.Manifest).Cassette.Port).To(Equal(8080))
	})
})

var _ = Describe("Load", func() {
	It("reports the path when the file does not parse", func() {
		_, err := manifest.Load(filepath.Join(GinkgoT().TempDir(), manifest.File))
		Expect(err).To(MatchError(ContainSubstring("read cassette manifest")))
	})
})

// The shipped example is the one manifest in this repository that a person is
// invited to copy, so it is worth asserting that it is admissible and that what
// it derives is what the rest of that directory actually does. Everything
// checked here is duplicated somewhere in the example — provision.sql grants
// the role and schema, main.go serves the prefix, the Dockerfile exposes the
// port — and this is where the copies are held together.
var _ = Describe("the hello-world example manifest", func() {
	var parsed cassette.Manifest

	BeforeEach(func() {
		var err error
		parsed, err = manifest.Load(filepath.Join("..", "examples", "hello-world", manifest.File))
		Expect(err).NotTo(HaveOccurred())
	})

	It("is a manifest a core serving v1 would admit", func() {
		Expect(parsed.SchemaKind()).To(Equal(v1alpha1.Kind))
		Expect(parsed.CassetteName()).To(Equal(cassette.Name("hello-world")))
		Expect(parsed.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
	})

	It("declares the anchors the example serves", func() {
		Expect(parsed.Anchors()).To(Equal(cassette.Anchors{
			Health: "/ping", OpenAPI: "/openapi", Prefix: "api",
		}))
	})

	It("derives exactly what compose provisions for it", func() {
		Expect(parsed.GrantPlan()).To(Equal(cassette.GrantPlan{
			Role:      "cassette_hello-world",
			Schema:    "hello-world",
			OwnSchema: true,
			Selects:   []string{},
			Tables:    []string{"hello"},
		}), "provision.sql creates this role and the cassette creates this schema and table")
	})

	It("declares the port its image exposes", func() {
		Expect(parsed.(*v1alpha1.Manifest).Cassette.Port).To(Equal(9999))
	})
})

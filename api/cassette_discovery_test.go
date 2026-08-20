package api

import (
	"encoding/json"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

const discoveryManifest = `{
  "kind": "cassette/v1alpha1",
  "cassette": {
    "name": "summary",
    "version": "0.3.1",
    "display_name": "Summaries",
    "description": "Rolling summaries"
  },
  "depends": {"core": "v1", "views": ["spans"]},
  "api": {},
  "tables": [{"name": "summary"}],
  "config": [
    {"key": "llm.model", "type": "string", "default": "claude"},
    {"key": "llm.api_key", "env": "OPENAI_API_KEY", "type": "string", "required": true, "secret": true}
  ]
}`

func discoveryInstance(name string) *cassetterunner.Instance {
	return &cassetterunner.Instance{
		Name:    cassette.Name(name),
		URL:     "http://127.0.0.1:9000",
		Anchors: cassette.Anchors{Health: "/ping", OpenAPI: "/openapi", Prefix: "api"},
	}
}

var _ = Describe("cassette discovery", func() {
	var (
		registry *cassetterunner.Registry
		manifest *v1alpha1.Manifest
	)

	BeforeEach(func() {
		var err error
		manifest, err = v1alpha1.Parse([]byte(discoveryManifest))
		Expect(err).NotTo(HaveOccurred())
		Expect(manifest.Validate(DefaultContractVersions())).To(Succeed())

		digest, err := manifest.Digest()
		Expect(err).NotTo(HaveOccurred())

		registry = cassetterunner.NewRegistry()
		one := discoveryInstance("summary")
		one.Manifest = manifest
		one.Digest = digest
		Expect(registry.Put(one)).To(Succeed())
	})

	It("publishes what a cassette is", func() {
		document := buildCassetteDiscovery(registry, "v1", func(cassette.Name) tapesoapi.Status { return tapesoapi.Fresh })

		Expect(document.ContractVersion).To(Equal("v1"))
		Expect(document.Cassettes).To(HaveLen(1))

		entry := document.Cassettes[0]
		Expect(entry.Name).To(Equal("summary"))
		Expect(entry.Version).To(Equal("0.3.1"))
		Expect(entry.DisplayName).To(Equal("Summaries"))
		Expect(entry.RoutePrefix).To(Equal("/v1/cassettes/summary"))
		Expect(entry.Depends.Core).To(Equal("v1"))
		Expect(entry.Depends.Views).To(ConsistOf("spans"))
		Expect(entry.OpenAPIPath).To(Equal("/v1/cassettes/summary/openapi.json"))
		Expect(entry.OpenAPIStatus).To(Equal(tapesoapi.Fresh))
		Expect(entry.ManifestDigest).To(HavePrefix("sha256:"))
	})

	It("qualifies tables with the schema a client writes in a query", func() {
		document := buildCassetteDiscovery(registry, "v1", nil)
		Expect(document.Cassettes[0].Tables).To(ConsistOf("summary.summary"))
	})

	It("publishes configuration as a schema and never as a value", func() {
		settings := buildCassetteDiscovery(registry, "v1", nil).Cassettes[0].Config
		Expect(settings).To(HaveLen(2))
		Expect(settings[0].Key).To(Equal("llm.model"))
		Expect(settings[0].Env).To(Equal("LLM_MODEL"))
		Expect(settings[0].Default).To(Equal("claude"))
		Expect(settings[1].Key).To(Equal("llm.api_key"))
		Expect(settings[1].Env).To(Equal("OPENAI_API_KEY"))
		Expect(settings[1].Required).To(BeTrue())
		Expect(settings[1].Secret).To(BeTrue())
		Expect(settings[1].Default).To(BeNil())
	})

	It("reports missing before a spec is fetched", func() {
		document := buildCassetteDiscovery(registry, "v1", nil)
		Expect(document.Cassettes[0].OpenAPIStatus).To(Equal(tapesoapi.Missing))
	})

	It("publishes sorted rejections", func() {
		registry.SetRejection("./broken.toml", errors.New("kind is required"))
		registry.SetRejection("./absent.toml", errors.New("no such file"))

		document := buildCassetteDiscovery(registry, "v1", nil)
		Expect(document.Problems).To(HaveLen(2))
		Expect(document.Problems[0].Subject).To(Equal("./absent.toml"))
		Expect(document.Problems[1].Subject).To(Equal("./broken.toml"))
	})

	It("encodes empty collections as lists rather than nulls", func() {
		bare := cassetterunner.NewRegistry()
		Expect(bare.Put(discoveryInstance("plain"))).To(Succeed())

		encoded, err := json.Marshal(buildCassetteDiscovery(bare, "v1", nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).To(ContainSubstring(`"problems":[]`))
		Expect(string(encoded)).NotTo(ContainSubstring("legacy_prefixes"))
		Expect(string(encoded)).To(ContainSubstring(`"tables":[]`))
		Expect(string(encoded)).To(ContainSubstring(`"config":[]`))
		Expect(string(encoded)).NotTo(ContainSubstring(`"openapi":`))
	})

	It("still describes a cassette whose manifest schema it cannot project", func() {
		bare := cassetterunner.NewRegistry()
		Expect(bare.Put(discoveryInstance("plain"))).To(Succeed())

		entry := buildCassetteDiscovery(bare, "v1", nil).Cassettes[0]
		Expect(entry.Name).To(Equal("plain"))
		Expect(entry.RoutePrefix).To(Equal("/v1/cassettes/plain"))
		Expect(entry.Depends).To(BeNil())
	})
})

var _ = Describe("cassetteSpecPath", func() {
	It("is where core serves a cassette's cached document", func() {
		Expect(cassetteSpecPath("summary")).To(Equal("/v1/cassettes/summary/openapi.json"))
	})
})

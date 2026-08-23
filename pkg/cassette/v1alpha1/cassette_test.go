package v1alpha1_test

import (
	"encoding/json"
	"errors"
	"math"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
)

const validMetadata = `{
  "kind":"cassette/v1alpha1",
  "cassette":{"name":"summary","version":"0.3.1","display_name":"Summaries"},
  "depends":{"core":"v1","views":["spans","sessions"]},
  "api":{"health":"/ping","openapi":"/openapi"},
  "tables":[{"name":"summary"}],
  "config":[
    {"key":"llm.model","type":"string","required":true,"default":"claude","enum":["other","claude"]},
    {"key":"batch_size","type":"int","default":9007199254740993,"min":1,"max":9007199254740994}
  ]
}`

var _ = Describe("versioned cassette metadata", func() {
	It("strictly parses JSON, preserves numbers, defaults, validates, and derives grants", func() {
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())
		Expect(manifest.API.Prefix).To(Equal("api"))
		Expect(manifest.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
		Expect(manifest.Config[1].Default).To(Equal(json.Number("9007199254740993")))
		Expect(manifest.GrantPlan()).To(Equal(cassette.GrantPlan{
			Role: "cassette_summary", Schema: "summary", OwnSchema: true,
			Selects: []string{"tapes_v1.sessions", "tapes_v1.spans"}, Tables: []string{"summary"},
		}))
	})

	It("rejects unknown root and nested fields", func() {
		_, err := v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","cassette":{"name":"summary","version":"1","surprise":true}}`))
		Expect(err).To(MatchError(ContainSubstring("unknown field \"cassette.surprise\"")))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","unknown":true}`))
		Expect(err).To(MatchError(ContainSubstring("unknown field \"unknown\"")))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","config":[{"key":"model","type":"string","surprise":true}]}`))
		Expect(err).To(MatchError(ContainSubstring("unknown field \"config[0].surprise\"")))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","api":{"Health":"/not-strict"}}`))
		Expect(err).To(MatchError(ContainSubstring("unknown field \"api.Health\"")))
	})

	It("requires unambiguous, complete JSON and the exact kind", func() {
		_, err := v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","cassette":{"name":"one","name":"two"}}`))
		Expect(err).To(MatchError(ContainSubstring(`duplicate object key "name"`)))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1"} {}`))
		Expect(err).To(MatchError(ContainSubstring("trailing token")))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1"}`))
		Expect(err).To(MatchError(`expected kind "cassette/v1alpha1", got "cassette/v1"`))
	})

	It("retains canonicalization, digest stability, and redaction", func() {
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())
		before, err := manifest.MarshalCanonical()
		Expect(err).NotTo(HaveOccurred())
		var decoded v1alpha1.Manifest
		Expect(json.Unmarshal(before, &decoded)).To(Succeed())
		after, err := decoded.MarshalCanonical()
		Expect(err).NotTo(HaveOccurred())
		Expect(after).To(Equal(before))
		first, err := manifest.Digest()
		Expect(err).NotTo(HaveOccurred())
		second, err := decoded.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(second).To(Equal(first))
		manifest.Config = append(manifest.Config, v1alpha1.Setting{Key: "secret", Type: v1alpha1.SettingTypeString, Secret: true, Default: "value"})
		redacted := manifest.Redact()
		Expect(redacted.Config[len(redacted.Config)-1].Default).To(BeNil())
		Expect(manifest.Config[len(manifest.Config)-1].Default).To(Equal("value"))
	})

	It("returns every structured validation problem", func() {
		manifest := &v1alpha1.Manifest{
			Kind:     "wrong",
			Cassette: v1alpha1.Identity{Name: "public", Version: "1"},
			Depends:  v1alpha1.Depends{Core: "v9", Views: []string{"raw_turns", "raw_turns"}},
			API:      v1alpha1.APIAnchors{Health: "http://host/ping", OpenAPI: "/openapi?x=1", Prefix: "bad path"},
			Tables:   []v1alpha1.Table{{Name: "Bad.Name"}},
			Config: []v1alpha1.Setting{
				{Key: "same.key", Type: v1alpha1.SettingTypeInt, Default: "no", Min: new(int64(5)), Max: new(int64(1))},
				{Key: "same.key", Type: "float"},
			},
		}
		err := manifest.Validate([]cassette.ContractVersion{"v1"})
		var validation *cassette.ValidationError
		Expect(errors.As(err, &validation)).To(BeTrue())
		Expect(validation.Problems).To(HaveLen(15))
		Expect(validation.Problems).To(ContainElement(cassette.Problem{
			Field: "config[1].key", Message: "duplicates config[0].key",
		}))
	})

	It("rejects a homepage with only a port in its authority", func() {
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())
		manifest.Cassette.Homepage = "http://:8080"

		Expect(manifest.Validate([]cassette.ContractVersion{"v1"})).To(MatchError(ContainSubstring(
			"cassette.homepage: must be an absolute http or https URL",
		)))
	})

	It("rejects float integers that would wrap and attributes bounds correctly", func() {
		manifest := &v1alpha1.Manifest{
			Kind: v1alpha1.Kind, Cassette: v1alpha1.Identity{Name: "summary", Version: "1"},
			Depends: v1alpha1.Depends{Core: "v1"},
			API:     v1alpha1.APIAnchors{Health: "/ping", OpenAPI: "/openapi", Prefix: "api"},
			Config: []v1alpha1.Setting{
				{Key: "large", Type: v1alpha1.SettingTypeInt, Default: float64(math.MaxInt64)},
				{Key: "bounded", Type: v1alpha1.SettingTypeString, Max: new(int64(1))},
			},
		}
		err := manifest.Validate([]cassette.ContractVersion{"v1"})
		var validation *cassette.ValidationError
		Expect(errors.As(err, &validation)).To(BeTrue())
		Expect(validation.Problems).To(ContainElements(
			cassette.Problem{Field: "config[0].default", Message: "must be an integer"},
			cassette.Problem{Field: "config[1].max", Message: "is only valid for int settings"},
		))
	})

	It("preserves every optional declarative metadata field", func() {
		manifest, err := v1alpha1.Parse([]byte(`{
			"kind":"cassette/v1alpha1",
			"cassette":{"name":"summary","version":"1","license":"Apache-2.0","homepage":"https://example.com","image":"ghcr.io/example/summary:1","port":8080},
			"depends":{"core":"v1"},
			"api":{},
			"x-source-digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(manifest.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
		Expect(manifest.Cassette.License).To(Equal("Apache-2.0"))
		Expect(manifest.Cassette.Image).To(Equal("ghcr.io/example/summary:1"))
		Expect(manifest.Cassette.Port).To(Equal(8080))
		Expect(string(manifest.SourceDigest)).To(HavePrefix("sha256:"))
		Expect(string(mustCanonical(manifest))).To(ContainSubstring(`"x-source-digest":"sha256:aaaa`))
	})
})

var _ = Describe("the audience a cassette declares", func() {
	// withAudience splices an audience into the shared valid manifest, so these
	// specs differ from the baseline in exactly the field under test.
	withAudience := func(audience string) []byte {
		return []byte(`{
  "kind":"cassette/v1alpha1",
  "cassette":{"name":"summary","version":"0.3.1","audience":` + audience + `},
  "depends":{"core":"v1"},
  "api":{"health":"/ping","openapi":"/openapi"}
}`)
	}

	parsed := func(audience string) *v1alpha1.Manifest {
		manifest, err := v1alpha1.Parse(withAudience(audience))
		Expect(err).NotTo(HaveOccurred())

		return manifest
	}

	It("serves every client when it declares none", func() {
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())

		Expect(manifest.Cassette.Audience).To(BeEmpty())
		for _, client := range []string{"console", "paperctl", "tapesctl", "something-new"} {
			Expect(manifest.Cassette.ServesAudience(client)).To(BeTrue(),
				"an undeclared audience has to mean everyone, or every manifest written "+
					"before the field existed would vanish from every client")
		}
	})

	It("serves only the clients it names once it names any", func() {
		manifest := parsed(`["console","paperctl"]`)
		Expect(manifest.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())

		Expect(manifest.Cassette.ServesAudience("console")).To(BeTrue())
		Expect(manifest.Cassette.ServesAudience("paperctl")).To(BeTrue())
		Expect(manifest.Cassette.ServesAudience("tapesctl")).To(BeFalse())
	})

	It("accepts a client name no release of tapes has heard of", func() {
		manifest := parsed(`["some-future-client"]`)

		Expect(manifest.Validate([]cassette.ContractVersion{"v1"})).To(Succeed(),
			"membership is deliberately open: a cassette must be able to name a client "+
				"that shipped after the tapes it validates against")
	})

	It("rejects a malformed or repeated client name", func() {
		Expect(parsed(`["Console"]`).Validate([]cassette.ContractVersion{"v1"})).
			To(MatchError(ContainSubstring("cassette.audience[0]")))
		Expect(parsed(`[""]`).Validate([]cassette.ContractVersion{"v1"})).
			To(MatchError(ContainSubstring("cassette.audience[0]")))
		Expect(parsed(`["console","console"]`).Validate([]cassette.ContractVersion{"v1"})).
			To(MatchError(ContainSubstring("duplicates cassette.audience[0]")))
	})

	It("gives the same digest however the audience is ordered", func() {
		forward, err := parsed(`["console","paperctl"]`).Digest()
		Expect(err).NotTo(HaveOccurred())
		backward, err := parsed(`["paperctl","console"]`).Digest()
		Expect(err).NotTo(HaveOccurred())

		Expect(forward).To(Equal(backward))
	})

	It("keeps an undeclared audience out of the canonical form entirely", func() {
		// This is what makes the field safe to add to a schema already in use:
		// a manifest that says nothing about audience canonicalizes exactly as
		// it did before the field existed, so its digest — the identity core and
		// a registry compare on — does not move underneath it.
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())

		Expect(string(mustCanonical(manifest))).NotTo(ContainSubstring("audience"))
	})

	It("carries a declared audience into the canonical form", func() {
		Expect(string(mustCanonical(parsed(`["console"]`)))).To(ContainSubstring(`"audience":["console"]`))
	})
})

var _ = Describe("cassette names and prefix metadata", func() {
	It("derives every database name", func() {
		name, err := cassette.ParseName("summary")
		Expect(err).NotTo(HaveOccurred())
		Expect(name.SchemaName()).To(Equal("summary"))
		Expect(name.RoleName()).To(Equal("cassette_summary"))
	})

	DescribeTable("rejects invalid and reserved names",
		func(value string) { _, err := cassette.ParseName(value); Expect(err).To(HaveOccurred()) },
		Entry("uppercase", "Summary"),
		Entry("leading digit", "1summary"),
		Entry("trailing hyphen", "summary-"),
		Entry("reserved Postgres schema", "public"),
		Entry("reserved core prefix", "tapes"),
		Entry("Postgres prefix", "pg_cassette"),
	)

	DescribeTable("normalizes declared local prefixes",
		func(declared, normalized string) {
			manifest := &v1alpha1.Manifest{
				Kind: v1alpha1.Kind, Cassette: v1alpha1.Identity{Name: "summary", Version: "1"},
				Depends: v1alpha1.Depends{Core: "v1"},
				API:     v1alpha1.APIAnchors{Health: "/ping", OpenAPI: "/openapi", Prefix: declared},
			}
			Expect(manifest.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
			Expect(manifest.API.PrefixPath()).To(Equal(normalized))
		},
		Entry("bare", "api", "api"),
		Entry("leading slash", "/api", "api"),
		Entry("both slashes", "/api/", "api"),
		Entry("nested", "ext/v2", "ext/v2"),
		Entry("none", "/", ""),
	)
})

func mustCanonical(manifest *v1alpha1.Manifest) []byte {
	GinkgoHelper()
	canonical, err := manifest.MarshalCanonical()
	Expect(err).NotTo(HaveOccurred())
	return canonical
}

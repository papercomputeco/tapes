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

	It("resolves default and explicit environment variable names", func() {
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())
		derived := mustCanonical(manifest)
		manifest.Config[0].Env = "LLM_MODEL"
		Expect(mustCanonical(manifest)).To(Equal(derived))

		manifest.Config[1].Env = "SUMMARY_BATCH_SIZE"
		Expect(manifest.Config[0].EnvVar()).To(Equal("LLM_MODEL"))
		Expect(manifest.Config[1].EnvVar()).To(Equal("SUMMARY_BATCH_SIZE"))
		Expect(string(mustCanonical(manifest))).To(ContainSubstring(`"env":"SUMMARY_BATCH_SIZE"`))
	})

	It("validates explicit environment variable names and resolved uniqueness", func() {
		manifest, err := v1alpha1.Parse([]byte(validMetadata))
		Expect(err).NotTo(HaveOccurred())
		manifest.Config = []v1alpha1.Setting{
			{Key: "one", Env: "SHARED", Type: v1alpha1.SettingTypeString},
			{Key: "shared", Type: v1alpha1.SettingTypeString},
			{Key: "bad", Env: "NOT-PORTABLE", Type: v1alpha1.SettingTypeString},
		}

		err = manifest.Validate([]cassette.ContractVersion{"v1"})
		Expect(err).To(MatchError(And(
			ContainSubstring("config[1].env: resolves to the same environment variable as config[0]"),
			ContainSubstring("config[2].env: must be a portable environment variable name"),
		)))
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

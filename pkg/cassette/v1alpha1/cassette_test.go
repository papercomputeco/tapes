package v1alpha1_test

import (
	"encoding/json"
	"errors"
	"math"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/manifest"
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

// publishesMetadata is the shared JSON form of a manifest declaring the three
// dynamic-registration sections: a published view with a filter claim, an
// advertised entity, and a registry-change hook. The names are deliberately
// neutral (a "notes" cassette) — the mechanism under test is generic.
const publishesMetadata = `{
  "kind":"cassette/v1alpha1",
  "cassette":{"name":"notes","version":"0.1.0"},
  "depends":{"core":"v1","published":["other_v1.attachments"]},
  "api":{"health":"/ping","openapi":"/openapi"},
  "publishes":{
    "views":["notes_v1.attachments"],
    "filters":[{
      "param":"note",
      "surface":"sessions",
      "view":"notes_v1.attachments",
      "match":{"primitive_type":"session","value_column":"value"},
      "normalize":["trim","nfc","casefold"]
    }]
  },
  "entities":[{"type":"note","id_kind":"uuid","display_name":"Note",
    "relations":[{"to":"session","kind":"attached_to"}]}],
  "hooks":{"registry_changed":"/hooks/registry-changed"}
}`

// publishesTOML is the authored-TOML encoding of publishesMetadata. The two
// must produce one canonical digest, or an orchestrator and core would be
// looking at two documents that merely resemble each other.
const publishesTOML = `
kind = "cassette/v1alpha1"

[cassette]
name = "notes"
version = "0.1.0"

[depends]
core = "v1"
published = ["other_v1.attachments"]

[api]
health = "/ping"
openapi = "/openapi"

[publishes]
views = ["notes_v1.attachments"]

[[publishes.filters]]
param = "note"
surface = "sessions"
view = "notes_v1.attachments"
match = { primitive_type = "session", value_column = "value" }
normalize = ["trim", "nfc", "casefold"]

[[entities]]
type = "note"
id_kind = "uuid"
display_name = "Note"
relations = [{ to = "session", kind = "attached_to" }]

[hooks]
registry_changed = "/hooks/registry-changed"
`

var _ = Describe("the publishes section a cassette declares", func() {
	parsePublishes := func(data string) *v1alpha1.Manifest {
		GinkgoHelper()
		parsed, err := v1alpha1.Parse([]byte(data))
		Expect(err).NotTo(HaveOccurred())
		return parsed
	}

	It("parses and canonicalizes a publishes section into the manifest digest", func() {
		fromJSON := parsePublishes(publishesMetadata)
		Expect(fromJSON.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
		Expect(fromJSON.Publishes).NotTo(BeNil())
		Expect(fromJSON.Publishes.Views).To(Equal([]string{"notes_v1.attachments"}))
		Expect(fromJSON.Publishes.Filters).To(HaveLen(1))
		claim := fromJSON.Publishes.Filters[0]
		Expect(claim.Param).To(Equal("note"))
		Expect(claim.Surface).To(Equal("sessions"))
		Expect(claim.View).To(Equal("notes_v1.attachments"))
		Expect(claim.Match.PrimitiveType).To(Equal("session"))
		Expect(claim.Match.ValueColumn).To(Equal("value"))
		Expect(claim.Normalize).To(Equal([]string{"trim", "nfc", "casefold"}),
			"normalization verbs apply in declared order, so canonicalization must not sort them")
		Expect(fromJSON.Depends.Published).To(Equal([]string{"other_v1.attachments"}))

		// TOML and JSON are two encodings of one document, not two documents.
		fromTOML, err := manifest.ParseTOML([]byte(publishesTOML))
		Expect(err).NotTo(HaveOccurred())
		tomlDigest, err := fromTOML.Digest()
		Expect(err).NotTo(HaveOccurred())
		jsonDigest, err := fromJSON.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(tomlDigest).To(Equal(jsonDigest))

		// The canonical form round-trips through its own JSON.
		canonical := mustCanonical(fromJSON)
		var decoded v1alpha1.Manifest
		Expect(json.Unmarshal(canonical, &decoded)).To(Succeed())
		Expect(mustCanonical(&decoded)).To(Equal(canonical))

		// A claim change is an identity change: core admits claims by digest,
		// so a cassette that claims a different param is a different cassette.
		changed := parsePublishes(publishesMetadata)
		changed.Publishes.Filters[0].Param = "tag"
		changedDigest, err := changed.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(changedDigest).NotTo(Equal(jsonDigest))

		// And a manifest that declares none of this keeps the digest it had
		// before the sections existed.
		Expect(string(mustCanonical(parsePublishes(validMetadata)))).NotTo(
			ContainSubstring("publishes"))
	})

	It("derives the core-role published-view grants and consumer selects", func() {
		parsed := parsePublishes(publishesMetadata)
		plan := parsed.GrantPlan()
		Expect(plan.CoreSelects).To(Equal([]string{"notes_v1.attachments"}),
			"deployment tooling renders SELECT on the published view for core's read role")
		Expect(plan.Selects).To(ContainElement("other_v1.attachments"),
			"depends.published views are read by the cassette like contract views")
	})

	It("refuses malformed view names and params at admission", func() {
		mutate := func(change func(*v1alpha1.Manifest)) error {
			parsed := parsePublishes(publishesMetadata)
			change(parsed)
			return parsed.Validate([]cassette.ContractVersion{"v1"})
		}

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Views[0] = "Notes_v1.attachments"
			m.Publishes.Filters[0].View = "Notes_v1.attachments"
		})).To(MatchError(ContainSubstring("publishes.views[0]")), "uppercase view schema")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Views[0] = "attachments"
			m.Publishes.Filters[0].View = "attachments"
		})).To(MatchError(ContainSubstring("publishes.views[0]")), "unqualified view name")

		long := strings.Repeat("a", 64)
		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Views[0] = long + ".attachments"
			m.Publishes.Filters[0].View = long + ".attachments"
		})).To(MatchError(ContainSubstring("publishes.views[0]")), "overlong schema segment")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Views[0] = "tapes_v1.attachments"
			m.Publishes.Filters[0].View = "tapes_v1.attachments"
		})).To(MatchError(ContainSubstring("publishes.views[0]")),
			"a cassette must not publish into core's contract schema")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Param = "Bad-Param"
		})).To(MatchError(ContainSubstring("publishes.filters[0].param")), "bad param grammar")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Surface = "spanners"
		})).To(MatchError(ContainSubstring("publishes.filters[0].surface")), "unknown surface")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Normalize = []string{"trim", "lowercase"}
		})).To(MatchError(ContainSubstring("publishes.filters[0].normalize[1]")),
			"unknown normalize verb")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].View = "elsewhere_v1.attachments"
		})).To(MatchError(ContainSubstring("publishes.filters[0].view")),
			"a claim must join a view this manifest actually publishes")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters = append(m.Publishes.Filters, m.Publishes.Filters[0])
		})).To(MatchError(ContainSubstring("duplicates")),
			"one manifest cannot claim the same (param, surface) twice")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Match.PrimitiveType = "9bad"
		})).To(MatchError(ContainSubstring("publishes.filters[0].match.primitive_type")))

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Match.ValueColumn = ""
		})).To(MatchError(ContainSubstring("publishes.filters[0].match.value_column")))

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Match.ValueColumn = `value"; DROP TABLE sessions; --`
		})).To(MatchError(ContainSubstring("publishes.filters[0].match.value_column")),
			"a hostile column name dies at admission, before it can near an identifier position")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Match.ValueColumn = "Value"
		})).To(MatchError(ContainSubstring("publishes.filters[0].match.value_column")),
			"column grammar is lower-snake, the same as every published identifier")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Publishes.Filters[0].Match.ValueColumn = strings.Repeat("a", 64)
		})).To(MatchError(ContainSubstring("publishes.filters[0].match.value_column")),
			"overlong column segment")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Depends.Published[0] = "tapes_v1.sessions"
		})).To(MatchError(ContainSubstring("depends.published[0]")),
			"contract views are depends.views territory, not published views")

		Expect(mutate(func(m *v1alpha1.Manifest) {
			m.Hooks.RegistryChanged = "http://host/hook"
		})).To(MatchError(ContainSubstring("hooks.registry_changed")),
			"a hook is a path on the cassette's own listener, never a URL")
	})

	It("rejects unknown fields inside the new sections", func() {
		_, err := v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","publishes":{"surprise":true}}`))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "publishes.surprise"`)))
		_, err = v1alpha1.Parse([]byte(
			`{"kind":"cassette/v1alpha1","publishes":{"filters":[{"param":"x","surprise":true}]}}`))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "publishes.filters[0].surprise"`)))
		_, err = v1alpha1.Parse([]byte(
			`{"kind":"cassette/v1alpha1","publishes":{"filters":[{"match":{"surprise":true}}]}}`))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "publishes.filters[0].match.surprise"`)))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","entities":[{"surprise":true}]}`))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "entities[0].surprise"`)))
		_, err = v1alpha1.Parse([]byte(
			`{"kind":"cassette/v1alpha1","entities":[{"type":"note","relations":[{"surprise":true}]}]}`))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "entities[0].relations[0].surprise"`)))
		_, err = v1alpha1.Parse([]byte(`{"kind":"cassette/v1alpha1","hooks":{"surprise":true}}`))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "hooks.surprise"`)))
	})
})

var _ = Describe("the entities a cassette advertises", func() {
	It("parses and canonicalizes entity declarations into the manifest digest", func() {
		fromJSON, err := v1alpha1.Parse([]byte(publishesMetadata))
		Expect(err).NotTo(HaveOccurred())
		Expect(fromJSON.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
		Expect(fromJSON.Entities).To(HaveLen(1))
		Expect(fromJSON.Entities[0].Type).To(Equal("note"))
		Expect(fromJSON.Entities[0].IDKind).To(Equal("uuid"))
		Expect(fromJSON.Entities[0].DisplayName).To(Equal("Note"))
		Expect(fromJSON.Entities[0].Relations).To(Equal([]v1alpha1.EntityRelation{
			{To: "session", Kind: "attached_to"},
		}))

		fromTOML, err := manifest.ParseTOML([]byte(publishesTOML))
		Expect(err).NotTo(HaveOccurred())
		tomlDigest, err := fromTOML.Digest()
		Expect(err).NotTo(HaveOccurred())
		jsonDigest, err := fromJSON.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(tomlDigest).To(Equal(jsonDigest),
			"the TOML and JSON entity declarations must be one document by digest")

		// An entity change is digest-relevant: discovery consumers key their
		// registry refresh on it.
		changed, err := v1alpha1.Parse([]byte(publishesMetadata))
		Expect(err).NotTo(HaveOccurred())
		changed.Entities[0].Type = "annotation"
		changedDigest, err := changed.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(changedDigest).NotTo(Equal(jsonDigest))

		// Declaration order is not identity.
		reordered, err := v1alpha1.Parse([]byte(publishesMetadata))
		Expect(err).NotTo(HaveOccurred())
		reordered.Entities = append(reordered.Entities, v1alpha1.Entity{Type: "annotation", IDKind: "string"})
		forward, err := reordered.Digest()
		Expect(err).NotTo(HaveOccurred())
		reordered.Entities[0], reordered.Entities[1] = reordered.Entities[1], reordered.Entities[0]
		backward, err := reordered.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(backward).To(Equal(forward))

		// Malformed declarations refuse validation.
		invalid, err := v1alpha1.Parse([]byte(publishesMetadata))
		Expect(err).NotTo(HaveOccurred())
		invalid.Entities[0].Type = "Bad-Type"
		Expect(invalid.Validate([]cassette.ContractVersion{"v1"})).To(MatchError(
			ContainSubstring("entities[0].type")))

		invalid, err = v1alpha1.Parse([]byte(publishesMetadata))
		Expect(err).NotTo(HaveOccurred())
		invalid.Entities[0].IDKind = "UUID!"
		Expect(invalid.Validate([]cassette.ContractVersion{"v1"})).To(MatchError(
			ContainSubstring("entities[0].id_kind")))

		invalid, err = v1alpha1.Parse([]byte(publishesMetadata))
		Expect(err).NotTo(HaveOccurred())
		invalid.Entities = append(invalid.Entities, v1alpha1.Entity{Type: "note", IDKind: "uuid"})
		Expect(invalid.Validate([]cassette.ContractVersion{"v1"})).To(MatchError(
			ContainSubstring("duplicates")))
	})
})

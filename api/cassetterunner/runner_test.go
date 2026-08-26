package cassetterunner_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// claimDocument is a cassette OpenAPI document whose manifest exercises all
// three dynamic-registration sections: a published view, a filter claim on
// the sessions surface, an advertised entity, and a registry-change hook.
// The names are deliberately neutral — the mechanism under test is generic.
func claimDocument(name, param, entityType string) string {
	return fmt.Sprintf(`{
  "openapi": "3.1.0",
  "info": {"title": %[1]q, "version": "1.0.0"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": %[1]q, "version": "1.0.0"},
    "depends": {"core": "v1"},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"},
    "publishes": {
      "views": ["%[1]s_v1.attachments"],
      "filters": [{
        "param": %[2]q,
        "surface": "sessions",
        "view": "%[1]s_v1.attachments",
        "match": {"primitive_type": "session", "value_column": "value"},
        "normalize": ["trim", "nfc", "casefold"]
      }]
    },
    "entities": [{"type": %[3]q, "id_kind": "uuid", "display_name": %[3]q}],
    "hooks": {"registry_changed": "/hooks/registry-changed"}
  },
  "paths": {"/api/%[1]s/items": {"get": {"operationId": "%[1]s.items", "responses": {"200": {"description": "ok"}}}}}
}`, name, param, entityType)
}

// sessionsReserved derives the core-owned param set for the sessions surface
// from a real parser carrying a documented GET /v1/sessions — the same
// derivation core wires in production, so the spec proves the reserved set
// comes from a route table rather than a hand-list.
func sessionsReserved() func(string) []string {
	GinkgoHelper()
	parser := tapesoapi.NewParser()
	operation := &tapesoapi.Operation{
		OperationID: "listSessions",
		Parameters: []*tapesoapi.Parameter{
			{Name: "limit", In: tapesoapi.InQuery},
			{Name: "cursor", In: tapesoapi.InQuery},
			{Name: "sort", In: tapesoapi.InQuery},
		},
		Responses: map[string]*tapesoapi.Response{"200": {Description: "ok"}},
	}
	Expect(parser.AddOperation(http.MethodGet, "/v1/sessions", operation, tapesoapi.Provenance{
		Kind: tapesoapi.KindRoute, Name: "GET /v1/sessions",
	})).To(Succeed())

	return cassetterunner.ReservedParamsFromParser(parser, map[string]string{"sessions": "/v1/sessions"})
}

var _ = Describe("filter-param claims from admitted manifests", func() {
	It("refuses a document whose claim collides with a core-owned param", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "cursor", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{
			Registry:       registry,
			Contracts:      servedContracts(),
			ReservedParams: sessionsReserved(),
		})
		runtime.SetSources([]string{source.URL + "/openapi"})

		errs := runtime.Refresh(ctx)
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Error()).To(ContainSubstring(`"cursor"`),
			"the error must name the reserved param the claim collided with")
		Expect(errs[0].Error()).To(ContainSubstring("core-owned"))
		Expect(registry.Instances()).To(BeEmpty(),
			"the whole document is refused, never just the offending claim")
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty())
	})

	It("refuses a second cassette claiming an already-held param and keeps prior state on refresh", func(ctx SpecContext) {
		first := newMutableSource(claimDocument("notes", "note", "note"))
		second := newMutableSource(claimDocument("marks", "note", "mark"))
		defer first.Close()
		defer second.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{
			Registry:  registry,
			Contracts: servedContracts(),
		})
		runtime.SetSources([]string{first.URL + "/openapi", second.URL + "/openapi"})

		errs := runtime.Refresh(ctx)
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Error()).To(ContainSubstring("already held"))
		Expect(errs[0].Error()).To(ContainSubstring("notes"))

		claims := registry.ClaimsFor("sessions")
		Expect(claims).To(HaveLen(1), "first claim wins")
		Expect(claims[0].Cassette).To(Equal(cassette.Name("notes")))
		Expect(claims[0].Param).To(Equal("note"))
		Expect(claims[0].View).To(Equal("notes_v1.attachments"))
		Expect(claims[0].PrimitiveType).To(Equal("session"))
		Expect(claims[0].ValueColumn).To(Equal("value"))
		Expect(claims[0].Normalize).To(Equal([]string{"trim", "nfc", "casefold"}))

		// A violating refresh of the holder keeps its prior admitted claims,
		// exactly as it keeps the stale document.
		violating := strings.Replace(claimDocument("notes", "note", "note"),
			`"surface": "sessions"`, `"surface": "bogus"`, 1)
		first.document.Store(&violating)
		Expect(runtime.Refresh(ctx)).To(HaveLen(2), "both the stale holder and the loser report problems")

		held := registry.ClaimsFor("sessions")
		Expect(held).To(HaveLen(1), "a refused refresh must not drop the admitted claim")
		Expect(held[0].Cassette).To(Equal(cassette.Name("notes")))
		Expect(runtime.Status("notes")).To(Equal(tapesoapi.Stale))
	})

	It("drops claims when the holding cassette is withdrawn", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{
			Registry:  registry,
			Contracts: servedContracts(),
		})
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1))

		runtime.SetSources([]string{})

		Expect(registry.ClaimsFor("sessions")).To(BeEmpty(),
			"claims are derived from admitted state, so withdrawal is enough — no route changed")
		Expect(registry.Instances()).To(BeEmpty())
	})

	It("admits a publishes declaration without touching the database", func(ctx SpecContext) {
		// The runner holds no database handle at all: admission is manifest
		// arithmetic over in-memory state, so a publishes declaration must
		// admit in a world with no Postgres configured, and the grant it
		// implies must remain a declaration for deployment tooling.
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{
			Registry:       registry,
			Contracts:      servedContracts(),
			ReservedParams: sessionsReserved(),
		})
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1))

		instance, ok := registry.Get("notes")
		Expect(ok).To(BeTrue())
		Expect(instance.Manifest.GrantPlan().CoreSelects).To(Equal([]string{"notes_v1.attachments"}),
			"the core-role SELECT grant is derived, published, and never applied by tapes")
	})
})

var _ = Describe("entity aggregation into discovery", func() {
	newRuntime := func(registry *cassetterunner.Registry) *cassetterunner.Runner {
		return cassetterunner.NewRunner(cassetterunner.Config{
			Registry:  registry,
			Contracts: servedContracts(),
		})
	}

	It("aggregates core-native and cassette-declared entities into discovery", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := newRuntime(registry)

		before := registry.Entities()
		Expect(before).To(HaveLen(1), "core-native entities exist before any cassette does")
		Expect(before[0].Type).To(Equal("session"))
		Expect(before[0].IDKind).To(Equal("uuid"))
		Expect(before[0].Cassette).To(BeEmpty(), "core is not a cassette")

		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		after := registry.Entities()
		Expect(after).To(HaveLen(2))
		Expect(after[0].Type).To(Equal("session"))
		Expect(after[1].Type).To(Equal("note"))
		Expect(after[1].IDKind).To(Equal("uuid"))
		Expect(after[1].Cassette).To(Equal("notes"))
	})

	It("drops a withdrawn cassette's entities from discovery", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := newRuntime(registry)
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Entities()).To(HaveLen(2))

		runtime.SetSources([]string{})

		remaining := registry.Entities()
		Expect(remaining).To(HaveLen(1), "withdrawal removes the cassette's entities, mirroring claims")
		Expect(remaining[0].Type).To(Equal("session"))
	})

	It("replaces a cassette's entities when it re-registers with a changed manifest", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := newRuntime(registry)
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		changed := claimDocument("notes", "note", "annotation")
		source.document.Store(&changed)
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		entities := registry.Entities()
		Expect(entities).To(HaveLen(2), "the old declaration set is replaced, not appended to")
		Expect(entities[1].Type).To(Equal("annotation"))
		Expect(entities[1].Cassette).To(Equal("notes"))
	})
})

var _ = Describe("registry-change hooks", func() {
	It("notifies hook-declaring cassettes when the admitted registry changes", func(ctx SpecContext) {
		// One server plays the hook-declaring cassette: it serves its OpenAPI
		// document and records the POSTs core makes to its declared hook. The
		// hook deliberately answers 500 so the same spec pins best-effort:
		// every failure is survivable, none is fatal to admission.
		var hookCalls atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("/openapi", func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(claimDocument("notes", "note", "note")))
		})
		mux.HandleFunc("/hooks/registry-changed", func(writer http.ResponseWriter, request *http.Request) {
			Expect(request.Method).To(Equal(http.MethodPost))
			hookCalls.Add(1)
			writer.WriteHeader(http.StatusInternalServerError)
		})
		hooked := httptest.NewServer(mux)
		defer hooked.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{
			Registry:  registry,
			Contracts: servedContracts(),
		})
		runtime.SetSources([]string{hooked.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty(),
			"a failing hook must never fail admission")
		Expect(hookCalls.Load()).To(BeNumerically(">=", 1),
			"the cassette's own admission is an admitted-set change")

		// A steady-state refresh changes nothing and must not renotify.
		settled := hookCalls.Load()
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(hookCalls.Load()).To(Equal(settled),
			"an unchanged admitted set is not a registry change")

		// Admitting a second cassette changes the set again.
		other := newMutableSource(claimDocument("marks", "mark", "mark"))
		defer other.Close()
		runtime.SetSources([]string{hooked.URL + "/openapi", other.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(hookCalls.Load()).To(BeNumerically(">", settled))
		Expect(registry.Instances()).To(HaveLen(2))

		// Withdrawal is a change too.
		beforeWithdraw := hookCalls.Load()
		runtime.SetSources([]string{hooked.URL + "/openapi"})
		Expect(hookCalls.Load()).To(BeNumerically(">", beforeWithdraw))
	})
})

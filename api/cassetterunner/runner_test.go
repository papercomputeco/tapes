package cassetterunner_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
		Expect(before).To(HaveLen(3), "core-native entities exist before any cassette does")
		Expect(before[0].Type).To(Equal("session"))
		Expect(before[0].IDKind).To(Equal("uuid"))
		Expect(before[0].Cassette).To(BeEmpty(), "core is not a cassette")
		Expect(before[1].Type).To(Equal("trace"))
		Expect(before[1].IDKind).To(Equal("string"))
		Expect(before[2].Type).To(Equal("span"))
		Expect(before[2].IDKind).To(Equal("string"))

		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		after := registry.Entities()
		Expect(after).To(HaveLen(4))
		Expect(after[0].Type).To(Equal("session"))
		Expect(after[3].Type).To(Equal("note"))
		Expect(after[3].IDKind).To(Equal("uuid"))
		Expect(after[3].Cassette).To(Equal("notes"))
	})

	It("drops a withdrawn cassette's entities from discovery", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := newRuntime(registry)
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Entities()).To(HaveLen(4))

		runtime.SetSources([]string{})

		remaining := registry.Entities()
		Expect(remaining).To(HaveLen(3), "withdrawal removes the cassette's entities, mirroring claims")
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
		Expect(entities).To(HaveLen(4), "the old declaration set is replaced, not appended to")
		Expect(entities[3].Type).To(Equal("annotation"))
		Expect(entities[3].Cassette).To(Equal("notes"))
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

// claimProbe is a scriptable stand-in for the storage-side published-view
// probe: it records what it was asked to verify and answers with whatever
// error the spec has staged, so arming can be exercised without a database.
type claimProbe struct {
	mu    sync.Mutex
	err   error
	calls []string
}

func (probe *claimProbe) fn(_ context.Context, view, column string) error {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.calls = append(probe.calls, view+" "+column)

	return probe.err
}

func (probe *claimProbe) set(err error) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.err = err
}

func (probe *claimProbe) observed() []string {
	probe.mu.Lock()
	defer probe.mu.Unlock()

	return append([]string(nil), probe.calls...)
}

var _ = Describe("published-view arming for filter claims", func() {
	newProbedRuntime := func(registry *cassetterunner.Registry, probe *claimProbe) *cassetterunner.Runner {
		return cassetterunner.NewRunner(cassetterunner.Config{
			Registry:   registry,
			Contracts:  servedContracts(),
			ProbeClaim: probe.fn,
		})
	}

	It("leaves a failing-probe claim un-armed: unclaimed on the request path, reported to the operator", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		probe := &claimProbe{}
		probe.set(errors.New("relation does not exist"))
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty(),
			"an unreadable view is a claim problem, not a source failure: the document stays admitted")
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty(),
			"an un-armed claim must be invisible to the request path, exactly like an unclaimed param")
		Expect(probe.observed()).To(ContainElement("notes_v1.attachments value"),
			"the probe receives the claim-declared view and value column")

		rejections := registry.Rejections()
		Expect(rejections).To(HaveLen(1))
		Expect(rejections[0].Subject).To(Equal(`cassette notes: claim "note"`),
			"the rejection subject is claim-qualified and dedupe-stable across retries")
		Expect(rejections[0].Reason).To(ContainSubstring("notes_v1.attachments"))
		Expect(rejections[0].Reason).To(ContainSubstring("relation does not exist"))
	})

	It("arms a claim whose probe succeeds and files nothing", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		probe := &claimProbe{}
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		claims := registry.ClaimsFor("sessions")
		Expect(claims).To(HaveLen(1))
		Expect(claims[0].Param).To(Equal("note"))
		Expect(registry.Rejections()).To(BeEmpty())
	})

	It("re-probes on every refresh and arms a healed claim even when the document 304s", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		etag := `"v1"`
		source.etag.Store(&etag)
		probe := &claimProbe{}
		probe.set(errors.New("permission denied"))
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty())
		Expect(registry.Rejections()).To(HaveLen(1))

		// The grant lands; the document has not changed, so the next pass
		// revalidates to a 304 — arming must not depend on re-admission.
		probe.set(nil)
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(runtime.Status("notes")).To(Equal(tapesoapi.Fresh),
			"the second pass revalidated rather than re-admitting")
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1),
			"arming self-heals on the refresh cadence, with no document change required")
		Expect(registry.Rejections()).To(BeEmpty())
	})

	It("disarms an armed claim when its probe starts failing and notifies hooks on the transition only", func(ctx SpecContext) {
		var hookCalls atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("/openapi", func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(claimDocument("notes", "note", "note")))
		})
		mux.HandleFunc("/hooks/registry-changed", func(writer http.ResponseWriter, _ *http.Request) {
			hookCalls.Add(1)
			writer.WriteHeader(http.StatusNoContent)
		})
		hooked := httptest.NewServer(mux)
		defer hooked.Close()

		probe := &claimProbe{}
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{hooked.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1))
		settled := hookCalls.Load()

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(hookCalls.Load()).To(Equal(settled),
			"a claim that stays armed is not a registry change")

		probe.set(errors.New("view dropped"))
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty())
		Expect(hookCalls.Load()).To(BeNumerically(">", settled),
			"disarming changes the effective claim set as surely as withdrawal does")

		afterDisarm := hookCalls.Load()
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(hookCalls.Load()).To(Equal(afterDisarm),
			"a claim that stays un-armed is not a new change either")
	})

	It("keeps first-claim-wins ownership while the holder is un-armed", func(ctx SpecContext) {
		first := newMutableSource(claimDocument("notes", "note", "note"))
		second := newMutableSource(claimDocument("marks", "note", "mark"))
		defer first.Close()
		defer second.Close()
		probe := &claimProbe{}
		probe.set(errors.New("relation does not exist"))
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{first.URL + "/openapi", second.URL + "/openapi"})

		errs := runtime.Refresh(ctx)
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Error()).To(ContainSubstring("already held"),
			"arming gates execution, never ownership: an un-armed claim still holds its param")
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty())
	})

	It("requires a fresh probe after a withdrawn cassette is re-admitted", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		probe := &claimProbe{}
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1))

		runtime.SetSources([]string{})
		Expect(registry.Instances()).To(BeEmpty())

		// The world changed while the cassette was gone; the old verdict
		// must not leak into the re-admission.
		probe.set(errors.New("relation does not exist"))
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty(),
			"withdrawal drops arming state, so re-admission starts from an unproved claim")
	})

	It("retains an armed claim across a transient probe failure, without rejection or hook noise", func(ctx SpecContext) {
		var hookCalls atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("/openapi", func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(claimDocument("notes", "note", "note")))
		})
		mux.HandleFunc("/hooks/registry-changed", func(writer http.ResponseWriter, _ *http.Request) {
			hookCalls.Add(1)
			writer.WriteHeader(http.StatusNoContent)
		})
		hooked := httptest.NewServer(mux)
		defer hooked.Close()

		probe := &claimProbe{}
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{hooked.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1))
		settled := hookCalls.Load()

		// The store never answered: no verdict, so no state may move. A
		// disarm here would silently unfilter successful responses over a
		// blip — if the store were really down, the filtered queries
		// themselves would fail loudly anyway.
		probe.set(&cassetterunner.TransientProbeError{Err: errors.New("context deadline exceeded")})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1),
			"only a definitive verdict from the store may disarm an armed claim")
		Expect(registry.Rejections()).To(BeEmpty(),
			"an inconclusive probe files nothing")
		Expect(hookCalls.Load()).To(Equal(settled),
			"no state moved, so there is no registry change to announce")
	})

	It("keeps a definitive rejection unchanged across a transient probe failure", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		probe := &claimProbe{}
		probe.set(errors.New("relation does not exist"))
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty())
		Expect(registry.Rejections()).To(HaveLen(1))
		Expect(registry.Rejections()[0].Reason).To(ContainSubstring("relation does not exist"))

		probe.set(&cassetterunner.TransientProbeError{Err: errors.New("dial tcp: connection refused")})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty(),
			"arming always requires a definitive success")
		Expect(registry.Rejections()).To(HaveLen(1))
		Expect(registry.Rejections()[0].Reason).To(ContainSubstring("relation does not exist"),
			"the operator keeps the actionable verdict, not the blip that followed it")
		Expect(registry.Rejections()[0].Reason).NotTo(ContainSubstring("connection refused"))
	})

	It("never arms a claim on a transient probe failure", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		probe := &claimProbe{}
		probe.set(&cassetterunner.TransientProbeError{Err: errors.New("context deadline exceeded")})
		registry := cassetterunner.NewRegistry()
		runtime := newProbedRuntime(registry, probe)
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.ClaimsFor("sessions")).To(BeEmpty(),
			"a claim that was never proved readable stays un-armed until the store says yes")
		Expect(registry.Rejections()).To(BeEmpty(),
			"a blip is not a verdict, so nothing is filed against the claim")
	})

	It("arms every claim when core has no probe to consult", func(ctx SpecContext) {
		source := newMutableSource(claimDocument("notes", "note", "note"))
		defer source.Close()
		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{
			Registry:  registry,
			Contracts: servedContracts(),
		})
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.ClaimsFor("sessions")).To(HaveLen(1),
			"a core whose driver cannot probe keeps the pre-arming behavior: every admitted claim executes")
		Expect(registry.Rejections()).To(BeEmpty())
	})
})

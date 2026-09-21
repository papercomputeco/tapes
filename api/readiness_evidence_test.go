package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// evidenceDocument is a cassette OpenAPI document carrying settings with
// values — an ordinary default and a secret's default — so a spec can prove
// none of them reaches the evidence payload.
func evidenceDocument(name string) string {
	return fmt.Sprintf(`{
  "openapi": "3.1.0",
  "info": {"title": %q, "version": "1.0.0"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": %q, "version": "1.0.0"},
    "depends": {"core": "v1", "views": ["spans"]},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"},
    "tables": [{"name": "results"}],
    "config": [
      {"key": "llm.model", "type": "string", "default": "claude-decoy-model",
       "description": "decoy prose that must not travel"},
      {"key": "llm.api_key", "type": "string", "required": true, "secret": true}
    ]
  },
  "paths": {"/api/%s/results": {"get": {"operationId": "%s.results", "responses": {"200": {"description": "ok"}}}}}
}`, name, name, name, name)
}

const evidenceToken = "evidence-token-for-specs"

var _ = Describe("serving-instance readiness evidence", func() {
	var server *Server

	// serve stands up a cassette source answering document.
	serve := func(document string) *httptest.Server {
		source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(document))
		}))
		DeferCleanup(source.Close)

		return source
	}

	// evidence resolves the configured sources once and returns the payload
	// the handler would serve.
	evidence := func(ctx SpecContext, sources ...string) Evidence {
		server.SetCassetteSources(sources)
		server.RefreshCassetteSpecs(ctx)

		return server.buildEvidence(time.Now())
	}

	BeforeEach(func() {
		built, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		server = built
	})

	It("identifies the process that answered", func(ctx SpecContext) {
		document := evidence(ctx)

		Expect(document.Schema).To(Equal("tapes.evidence/v1"))
		Expect(document.Instance.InstanceID).NotTo(BeEmpty())
		Expect(document.Instance.StartedAt).NotTo(BeEmpty())
		Expect(document.CheckedAt).NotTo(BeEmpty())
		Expect(document.Configuration.ContractVersion).
			To(Equal(string(currentContractVersion(server.contracts))),
				"the evidence and the discovery document must not disagree about the surface")
	})

	// The deployment supplies pod identity; core cannot discover it without
	// becoming a Kubernetes client, which is the coupling it must not take on.
	It("publishes the pod identity the deployment supplied", func(ctx SpecContext) {
		GinkgoT().Setenv(EnvPodName, "acme-default-api-7d9c5f8b6-xk2qv")
		GinkgoT().Setenv(EnvPodUID, "3a91b0de-1f77-4a2b-9c55-1d1a1e0c9f42")
		GinkgoT().Setenv(EnvPodIP, "10.42.3.17")
		GinkgoT().Setenv(EnvNodeName, "ip-10-0-2-14")
		GinkgoT().Setenv(EnvReplicaSet, "acme-default-api-7d9c5f8b6")
		GinkgoT().Setenv(EnvImageDigest, "sha256:1111111111111111111111111111111111111111111111111111111111111111")

		built, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		instance := built.buildEvidence(time.Now()).Instance
		Expect(instance.PodName).To(Equal("acme-default-api-7d9c5f8b6-xk2qv"))
		Expect(instance.PodUID).To(Equal("3a91b0de-1f77-4a2b-9c55-1d1a1e0c9f42"))
		Expect(instance.PodIP).To(Equal("10.42.3.17"))
		Expect(instance.NodeName).To(Equal("ip-10-0-2-14"))
		Expect(instance.ReplicaSet).To(Equal("acme-default-api-7d9c5f8b6"))
		Expect(instance.ImageDigest).To(HavePrefix("sha256:"))
	})

	It("omits pod identity the deployment did not supply", func(ctx SpecContext) {
		encoded, err := json.Marshal(evidence(ctx).Instance)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).NotTo(ContainSubstring("pod_name"),
			"an absent field is honest; an empty one invites a reader to compare against it")
		Expect(string(encoded)).NotTo(ContainSubstring("image_digest"))
	})

	It("reports an admitted source with the identities core serves it under", func(ctx SpecContext) {
		source := serve(evidenceDocument("summary"))

		document := evidence(ctx, source.URL+"/openapi")

		Expect(document.Configuration.SourceCount).To(Equal(1))
		Expect(document.Configuration.SourceListDigest).To(HavePrefix("sha256:"))
		Expect(document.Configuration.LoadedAt).NotTo(BeEmpty())
		Expect(document.UnresolvedSources).To(BeEmpty())
		Expect(document.Cassettes).To(HaveLen(1))

		entry := document.Cassettes[0]
		Expect(entry.Name).To(Equal("summary"))
		Expect(entry.Source).To(Equal(source.URL + "/openapi"))
		Expect(entry.Admission).To(Equal(cassetterunner.AdmissionAdmitted))
		Expect(entry.ManifestDigest).To(HavePrefix("sha256:"))
		Expect(entry.OpenAPIDigest).To(HavePrefix("sha256:"))
		Expect(entry.OpenAPIStatus).To(Equal(tapesoapi.Fresh))
		Expect(entry.RoutePrefix).To(Equal("/v1/cassettes/summary"))
		Expect(entry.AdmittedAt).NotTo(BeEmpty())
		Expect(entry.Rejection).To(BeNil())
	})

	// The case replica counts alone call a success: the cassette's own pod is
	// healthy, and core refused the document it served.
	It("reports a fetched-and-refused document as rejected, with subject and reason", func(ctx SpecContext) {
		source := serve(`{"openapi":"3.1.0","paths":{}}`)

		document := evidence(ctx, source.URL+"/openapi")

		Expect(document.Cassettes).To(BeEmpty(),
			"a document refused on first sight never earned a cassette identity")
		Expect(document.UnresolvedSources).To(HaveLen(1))
		Expect(document.UnresolvedSources[0].Subject).To(Equal(source.URL + "/openapi"))
		Expect(document.UnresolvedSources[0].Admission).To(Equal(cassetterunner.AdmissionRejected),
			"the service is up and serving something core refused, which is not an unreachable source")
		Expect(document.UnresolvedSources[0].Reason).To(ContainSubstring("missing root extension"))
		Expect(document.UnresolvedSources[0].ObservedAt).NotTo(BeEmpty())
	})

	It("keeps a refused refresh under the cassette it still serves", func(ctx SpecContext) {
		good := evidenceDocument("summary")
		bad := `{"openapi":"3.1.0","paths":{}}`
		served := &good
		source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(*served))
		}))
		DeferCleanup(source.Close)

		Expect(evidence(ctx, source.URL+"/openapi").Cassettes[0].Admission).
			To(Equal(cassetterunner.AdmissionAdmitted))

		served = &bad
		server.RefreshCassetteSpecs(ctx)

		entry := server.buildEvidence(time.Now()).Cassettes[0]
		Expect(entry.Name).To(Equal("summary"))
		Expect(entry.Admission).To(Equal(cassetterunner.AdmissionRejected))
		Expect(entry.OpenAPIStatus).To(Equal(tapesoapi.Stale))
		Expect(entry.RoutePrefix).To(Equal("/v1/cassettes/summary"),
			"the route the cassette published is still mounted and still answering")
		Expect(entry.AdmittedAt).To(BeEmpty(),
			"a stale admission timestamp would read as a live admission")
		Expect(entry.Rejection).NotTo(BeNil())
		Expect(entry.Rejection.Subject).To(Equal(source.URL + "/openapi"))
		Expect(entry.Rejection.Reason).To(ContainSubstring("missing root extension"))
		Expect(entry.Rejection.ObservedAt).NotTo(BeEmpty())
	})

	It("reports a source it could not reach as unresolved", func(ctx SpecContext) {
		gone := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		gone.Close()

		document := evidence(ctx, gone.URL+"/openapi")

		Expect(document.Cassettes).To(BeEmpty())
		Expect(document.UnresolvedSources).To(HaveLen(1))
		Expect(document.UnresolvedSources[0].Subject).To(Equal(gone.URL + "/openapi"))
		Expect(document.UnresolvedSources[0].Admission).To(Equal(cassetterunner.AdmissionUnresolved))
		Expect(document.UnresolvedSources[0].Reason).NotTo(BeEmpty())
		Expect(document.UnresolvedSources[0].ObservedAt).NotTo(BeEmpty())
	})

	// The two entries call for opposite remediations — fix the document versus
	// fix the address — so they have to be told apart from the payload's
	// structure. Reason is prose and documented as never parsed, which makes
	// it the wrong thing to branch on.
	It("distinguishes a refused document from an unreachable source on the wire", func(ctx SpecContext) {
		refused := serve(`{"openapi":"3.1.0","paths":{}}`)
		gone := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		gone.Close()

		document := evidence(ctx, refused.URL+"/openapi", gone.URL+"/openapi")
		Expect(document.UnresolvedSources).To(HaveLen(2))

		states := map[string]cassetterunner.Admission{}
		for _, unresolved := range document.UnresolvedSources {
			states[unresolved.Subject] = unresolved.Admission
		}
		Expect(states[refused.URL+"/openapi"]).To(Equal(cassetterunner.AdmissionRejected))
		Expect(states[gone.URL+"/openapi"]).To(Equal(cassetterunner.AdmissionUnresolved))

		// The field has to survive serialization flattened beside the problem
		// it qualifies, or a reader of the JSON gains nothing from it.
		encoded, err := json.Marshal(document.UnresolvedSources)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).To(ContainSubstring(`"admission":"rejected"`))
		Expect(string(encoded)).To(ContainSubstring(`"admission":"unresolved"`))
		Expect(string(encoded)).To(ContainSubstring(`"subject":`),
			"the problem fields stay flat rather than nesting under a key")
		Expect(string(encoded)).NotTo(ContainSubstring("EvidenceProblem"))
	})

	// A source admitted once keeps its cassette identity, so a later refusal
	// belongs under that cassette and not in this list.
	It("accounts for a rejected first-pass source without disturbing the exact count", func(ctx SpecContext) {
		admitted := serve(evidenceDocument("summary"))
		refused := serve(`{"openapi":"3.1.0","paths":{}}`)

		document := evidence(ctx, admitted.URL+"/openapi", refused.URL+"/openapi")

		Expect(document.Cassettes).To(HaveLen(1))
		Expect(document.UnresolvedSources).To(HaveLen(1))
		Expect(document.UnresolvedSources[0].Admission).To(Equal(cassetterunner.AdmissionRejected))
		Expect(len(document.Cassettes)+len(document.UnresolvedSources)).
			To(Equal(document.Configuration.SourceCount),
				"carrying an admission state must not move an entry between the two lists")
	})

	// A reader that cannot tell a truncated answer from a complete one cannot
	// conclude anything from an absent cassette.
	It("accounts for every configured source exactly once", func(ctx SpecContext) {
		admitted := serve(evidenceDocument("summary"))
		refused := serve(`{"openapi":"3.1.0","paths":{}}`)

		document := evidence(ctx, admitted.URL+"/openapi", refused.URL+"/openapi")

		Expect(document.Configuration.SourceCount).To(Equal(2))
		Expect(len(document.Cassettes) + len(document.UnresolvedSources)).
			To(Equal(document.Configuration.SourceCount))
	})

	// The count and the entries are two halves of one claim. A reconfiguration
	// landing between the reads that produce them yields an answer that is new
	// configuration beside old sources, which reads to a convergence check as
	// a fleet that has taken a change it has not taken.
	It("accounts for every source even when the configuration changes under the reader", func() {
		first := serve(evidenceDocument("summary"))
		second := serve(evidenceDocument("reports"))

		catalogs := [][]string{
			{first.URL + "/openapi"},
			{first.URL + "/openapi", second.URL + "/openapi"},
			{},
		}

		var group sync.WaitGroup
		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for pass := range 3000 {
				server.SetCassetteSources(catalogs[pass%len(catalogs)])
			}
		}()

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range 3000 {
				document := server.buildEvidence(time.Now())
				Expect(len(document.Cassettes)+len(document.UnresolvedSources)).
					To(Equal(document.Configuration.SourceCount),
						"the configuration identity and the source entries must come from one snapshot")
			}
		}()

		group.Wait()
	})

	// The identity has to move on a reconfiguration that never restarts the
	// process, because that is the case nothing outside the process can see.
	It("moves the configuration digest when the sources change without a restart", func(ctx SpecContext) {
		first := serve(evidenceDocument("summary"))
		second := serve(evidenceDocument("reports"))

		before := evidence(ctx, first.URL+"/openapi").Configuration
		after := evidence(ctx, first.URL+"/openapi", second.URL+"/openapi").Configuration

		Expect(after.SourceListDigest).NotTo(Equal(before.SourceListDigest))
		Expect(after.SourceCount).To(Equal(2))
		Expect(before.SourceCount).To(Equal(1))
	})

	// The same guarantee discovery makes: settings are published as schema and
	// never as values, and a secret's declared default is withheld outright.
	// Evidence publishes no settings at all, so the assertion is stronger.
	It("carries no configuration value and no secret", func(ctx SpecContext) {
		source := serve(evidenceDocument("summary"))

		encoded, err := json.Marshal(evidence(ctx, source.URL+"/openapi"))
		Expect(err).NotTo(HaveOccurred())
		payload := string(encoded)

		Expect(payload).NotTo(ContainSubstring("claude-decoy-model"))
		Expect(payload).NotTo(ContainSubstring("decoy prose"))
		Expect(payload).NotTo(ContainSubstring("llm.model"))
		Expect(payload).NotTo(ContainSubstring("llm.api_key"))
		Expect(payload).NotTo(ContainSubstring(`"config"`))
		Expect(payload).NotTo(ContainSubstring(`"default"`))
		Expect(payload).NotTo(ContainSubstring(`"secret"`))
		Expect(payload).NotTo(ContainSubstring(`"tables"`))
	})

	// Core knows nothing about plans and must not learn: the identities here
	// map onto a commercial policy only in something that holds one.
	It("names no plan, tier, or entitlement", func(ctx SpecContext) {
		source := serve(evidenceDocument("summary"))

		encoded, err := json.Marshal(evidence(ctx, source.URL+"/openapi"))
		Expect(err).NotTo(HaveOccurred())
		for _, forbidden := range []string{"plan", "tier", "entitlement", "revision", "quota", "limit"} {
			Expect(strings.ToLower(string(encoded))).NotTo(ContainSubstring(forbidden))
		}
	})

	It("redacts a credential in a configured source URL", func(ctx SpecContext) {
		source := serve(evidenceDocument("summary"))
		credentialed := strings.Replace(source.URL, "http://", "http://operator:hunter2@", 1)

		encoded, err := json.Marshal(evidence(ctx, credentialed+"/openapi"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).NotTo(ContainSubstring("hunter2"))
		Expect(string(encoded)).To(ContainSubstring("redacted"))
	})
})

var _ = Describe("the internal listener", func() {
	var (
		server   *Server
		internal *InternalServer
	)

	request := func(header string) *http.Response {
		req := httptest.NewRequest(http.MethodGet, InternalEvidencePath, nil)
		if header != "" {
			req.Header.Set(fiber.HeaderAuthorization, header)
		}
		response, err := internal.app.Test(req, fiber.TestConfig{Timeout: 5 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(response.Body.Close)

		return response
	}

	BeforeEach(func() {
		built, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		server = built
		internal, err = server.NewInternalServer(InternalConfig{
			ListenAddr: DefaultInternalListen,
			Token:      evidenceToken,
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("is documented to run on :8092", func() {
		Expect(DefaultInternalListen).To(Equal(":8092"))
	})

	It("refuses to serve unauthenticated when no token is configured", func() {
		_, err := server.NewInternalServer(InternalConfig{ListenAddr: DefaultInternalListen})
		Expect(err).To(MatchError(And(
			ContainSubstring("TAPES_INTERNAL_TOKEN"),
			ContainSubstring("TAPES_INTERNAL_LISTEN"),
		)))
	})

	It("refuses to build a listener with no address", func() {
		_, err := server.NewInternalServer(InternalConfig{Token: evidenceToken})
		Expect(err).To(MatchError(ContainSubstring("no listen address")))
	})

	It("serves evidence to a caller presenting the token", func() {
		response := request("Bearer " + evidenceToken)
		Expect(response.StatusCode).To(Equal(http.StatusOK))

		var document Evidence
		Expect(json.NewDecoder(response.Body).Decode(&document)).To(Succeed())
		Expect(document.Schema).To(Equal(EvidenceSchema))
		Expect(response.Header.Get(fiber.HeaderCacheControl)).To(Equal("no-store"))
	})

	It("accepts the scheme in any case, as the HTTP grammar requires", func() {
		Expect(request("bearer " + evidenceToken).StatusCode).To(Equal(http.StatusOK))
	})

	DescribeTable("refuses anything else with a bare 401",
		func(header string) {
			response := request(header)
			Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
			Expect(response.Header.Get(fiber.HeaderWWWAuthenticate)).To(Equal("Bearer"))
			Expect(response.ContentLength).To(BeNumerically("<=", 0),
				"a body distinguishing the failures would make this an oracle")
		},
		Entry("with no Authorization header", ""),
		Entry("with the wrong token", "Bearer not-the-token"),
		Entry("with a token of the right length but wrong bytes", "Bearer evidence-token-for-spacs"),
		Entry("with an empty bearer credential", "Bearer "),
		Entry("with another scheme", "Basic ZXZpZGVuY2U6dG9rZW4="),
		Entry("with the bare token and no scheme", evidenceToken),
	)

	It("answers 401 before 404 for a path it does not serve", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
		response, err := internal.app.Test(req, fiber.TestConfig{Timeout: 5 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(response.Body.Close)
		Expect(response.StatusCode).To(Equal(http.StatusUnauthorized),
			"what is mounted here is not something an unauthenticated caller may enumerate")
	})

	It("serves nothing else to an authenticated caller either", func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
		req.Header.Set(fiber.HeaderAuthorization, "Bearer "+evidenceToken)
		response, err := internal.app.Test(req, fiber.TestConfig{Timeout: 5 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(response.Body.Close)
		Expect(response.StatusCode).To(Equal(http.StatusNotFound))
	})

	// The gateway rewrites a tenant path prefix onto the API listener's root,
	// so a route registered there is a route a tenant can call. This is the
	// assertion that keeps the two listeners from quietly merging.
	It("is the only listener that serves evidence", func() {
		req := httptest.NewRequest(http.MethodGet, InternalEvidencePath, nil)
		req.Header.Set(fiber.HeaderAuthorization, "Bearer "+evidenceToken)
		response, err := server.app.Test(req, fiber.TestConfig{Timeout: 5 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(response.Body.Close)
		Expect(response.StatusCode).To(Equal(http.StatusNotFound),
			"the documented API listener must not route the evidence path")
	})

	It("keeps the evidence path out of the published contract", func(ctx SpecContext) {
		compiled, err := CompileOpenAPI(ctx, nil)
		Expect(err).NotTo(HaveOccurred())
		encoded, err := json.Marshal(compiled)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(encoded)).NotTo(ContainSubstring(InternalEvidencePath))
		Expect(string(encoded)).NotTo(ContainSubstring("/internal/"))
	})
})

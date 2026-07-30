package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// stubSpecs is a spec cache stated directly, so the cassette surface can be
// exercised without a fetch loop full of real documents.
type stubSpecs struct {
	documents map[cassette.Name][]byte
	digests   map[cassette.Name]cassette.Digest
	statuses  map[cassette.Name]tapesoapi.Status
	merged    []byte
	err       error

	// base records the parser the aggregate was handed, so a spec can assert
	// which description of core it merged against.
	base *tapesoapi.Parser
}

func (source *stubSpecs) Status(name cassette.Name) tapesoapi.Status {
	if status, ok := source.statuses[name]; ok {
		return status
	}

	return tapesoapi.Missing
}

func (source *stubSpecs) Spec(name cassette.Name) ([]byte, cassette.Digest, bool) {
	document, ok := source.documents[name]
	if !ok {
		return nil, "", false
	}

	return document, source.digests[name], true
}

func (source *stubSpecs) Document(_ context.Context, base *tapesoapi.Parser) ([]byte, error) {
	source.base = base

	return source.merged, source.err
}

func (source *stubSpecs) Refresh(context.Context) []error { return nil }

var _ = Describe("The cassette surface", func() {
	var (
		reg      *cassetterunner.Registry
		source   *stubSpecs
		upstream *httptest.Server
		server   *Server
	)

	// newSurface builds an API server and installs reg's resolved state on it.
	// It reaches into the server's own registry rather than going through a
	// resolve loop, because these specs are about what the HTTP surface does
	// with a fleet, not about how the fleet came to be.
	newSurface := func(specs cassetterunner.SpecCache) *Server {
		built, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		for _, instance := range reg.Instances() {
			Expect(built.cassettes.Put(instance)).To(Succeed())
		}
		for _, rejection := range reg.Rejections() {
			built.cassettes.SetRejection(rejection.Subject, errors.New(rejection.Reason))
		}
		if specs != nil {
			built.cassetteSpecs = specs
		}

		return built
	}

	// do issues a request against the API server and returns the response.
	do := func(target *Server, request *http.Request) (*http.Response, []byte) {
		response, err := target.app.Test(request, 5000)
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Body.Close()).To(Succeed())

		return response, body
	}

	get := func(path string) (*http.Response, []byte) {
		return do(server, httptest.NewRequest(http.MethodGet, path, nil))
	}

	BeforeEach(func() {
		upstream = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(writer, `{"path": %q, "cassette": %q, "forwarded_host": %q}`,
				request.URL.Path, request.Header.Get("X-Tapes-Cassette"), request.Header.Get("X-Forwarded-Host"))
		}))
		DeferCleanup(upstream.Close)

		reg = cassetterunner.NewRegistry()
		Expect(reg.Put(&cassetterunner.Instance{
			Name:    "summary",
			URL:     upstream.URL,
			Anchors: cassette.Anchors{Health: "/ping", OpenAPI: "/openapi", Prefix: "api"},
		})).To(Succeed())

		source = &stubSpecs{
			documents: map[cassette.Name][]byte{"summary": []byte(`{"openapi":"3.1.0"}`)},
			digests:   map[cassette.Name]cassette.Digest{"summary": "sha256:abc"},
			statuses:  map[cassette.Name]tapesoapi.Status{"summary": tapesoapi.Fresh},
			merged:    []byte(`{"openapi":"3.1.0","paths":{}}`),
		}
		server = newSurface(source)
	})

	Describe("proxying", func() {
		It("swaps the public prefix for the one the cassette serves", func() {
			response, body := get("/v1/cassettes/summary/reports/7")
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["path"]).To(Equal("/api/summary/reports/7"),
				"the cassette serves its own declared prefix and never learns core's")
			Expect(decoded["cassette"]).To(Equal("summary"))
			Expect(decoded["forwarded_host"]).NotTo(BeEmpty(), "the cassette is told who the client asked")
		})

		It("forwards the bare prefix with no trailing segments", func() {
			response, body := get("/v1/cassettes/summary")
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["path"]).To(Equal("/api/summary"))
		})

		It("does not serve non-canonical cassette paths", func() {
			response, _ := get("/v1/summaries/reports/7")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		})

		It("preserves the query string", func() {
			_, body := get("/v1/cassettes/summary/reports?since=yesterday")

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["path"]).To(Equal("/api/summary/reports"))
		})

		It("returns a 502 that names the cassette when it does not answer", func() {
			upstream.Close()

			response, body := get("/v1/cassettes/summary/reports")
			Expect(response.StatusCode).To(Equal(http.StatusBadGateway))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["error"]).To(Equal("cassette_unavailable"),
				"a cassette being down is an expected state, not a core failure")
			Expect(decoded["message"]).To(ContainSubstring("summary"))
			Expect(decoded["message"]).To(ContainSubstring(upstream.URL))
		})

		It("still serves core's own endpoints", func() {
			response, body := get("/ping")
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			Expect(string(body)).To(ContainSubstring("pong"),
				"core is the thing that worked before any cassette existed")
		})

		It("404s a cassette name nothing is installed under", func() {
			response, body := get("/v1/cassettes/summaryzzz/reports")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["error"]).To(Equal("unknown_cassette"))
		})

		It("404s a path no cassette and no core route claims", func() {
			response, _ := get("/v1/nothing")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		})

		It("routes a cassette registered after the server was built", func() {
			second := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(http.StatusNoContent)
			}))
			DeferCleanup(second.Close)

			response, _ := get("/v1/cassettes/reports/daily")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))

			Expect(server.cassettes.Put(&cassetterunner.Instance{
				Name: "reports", URL: second.URL, Anchors: cassette.Anchors{OpenAPI: "/openapi", Prefix: "api"},
			})).To(Succeed())

			response, _ = get("/v1/cassettes/reports/daily")
			Expect(response.StatusCode).To(Equal(http.StatusNoContent),
				"the route table is read per request, so registration needs no restart")
		})

		It("forwards to a cassette that mounts directly under its own name", func() {
			bare := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(writer, `{"path": %q}`, request.URL.Path)
			}))
			DeferCleanup(bare.Close)

			Expect(server.cassettes.Put(&cassetterunner.Instance{
				Name: "plain", URL: bare.URL, Anchors: cassette.Anchors{OpenAPI: "/openapi", Prefix: ""},
			})).To(Succeed())

			_, body := get("/v1/cassettes/plain/thing")
			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["path"]).To(Equal("/plain/thing"))
		})
	})

	Describe("discovery", func() {
		It("publishes what is installed, and what failed", func() {
			server.cassettes.SetRejection("http://sidecar.invalid/openapi", errors.New("kind is required"))

			response, body := get("/v1/cassettes")
			Expect(response.StatusCode).To(Equal(http.StatusOK))

			var document Discovery
			Expect(json.Unmarshal(body, &document)).To(Succeed())
			Expect(document.ContractVersion).To(Equal("v1"))
			Expect(document.Cassettes).To(HaveLen(1))
			Expect(document.Cassettes[0].Name).To(Equal("summary"))
			Expect(document.Cassettes[0].RoutePrefix).To(Equal("/v1/cassettes/summary"))
			Expect(document.Cassettes[0].OpenAPIStatus).To(Equal(tapesoapi.Fresh))
			Expect(document.Problems).To(HaveLen(1))
			Expect(document.Problems[0].Subject).To(Equal("http://sidecar.invalid/openapi"))
		})

		It("reports every cassette missing before its spec is fetched", func() {
			bare := newSurface(nil)

			_, body := do(bare, httptest.NewRequest(http.MethodGet, "/v1/cassettes", nil))
			var document Discovery
			Expect(json.Unmarshal(body, &document)).To(Succeed())
			Expect(document.Cassettes[0].OpenAPIStatus).To(Equal(tapesoapi.Missing))
		})
	})

	Describe("per-cassette specs", func() {
		It("serves a document from core's cache, with the digest as its ETag", func() {
			response, body := get("/v1/cassettes/summary/openapi.json")
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			Expect(string(body)).To(MatchJSON(`{"openapi":"3.1.0"}`))
			Expect(response.Header.Get("ETag")).To(Equal(`"sha256:abc"`))
		})

		It("wins over the proxy, so a cassette cannot shadow it", func() {
			// The cassette in this suite answers every path with a JSON echo,
			// including /api/summary/openapi.json. Core's route is registered
			// first, so the cached document is what comes back.
			_, body := get("/v1/cassettes/summary/openapi.json")
			Expect(string(body)).To(MatchJSON(`{"openapi":"3.1.0"}`))
		})

		It("serves the cached document for a cassette that is currently down", func() {
			upstream.Close()
			source.statuses["summary"] = tapesoapi.Stale

			response, _ := get("/v1/cassettes/summary/openapi.json")
			Expect(response.StatusCode).To(Equal(http.StatusOK),
				"a client most needs the surface of a cassette exactly when it is down")
		})

		It("answers a matching conditional request with 304", func() {
			request := httptest.NewRequest(http.MethodGet, "/v1/cassettes/summary/openapi.json", nil)
			request.Header.Set("If-None-Match", `"sha256:abc"`)

			response, body := do(server, request)
			Expect(response.StatusCode).To(Equal(http.StatusNotModified))
			Expect(body).To(BeEmpty())
		})

		It("404s a cassette that is not installed here", func() {
			response, body := get("/v1/cassettes/absent/openapi.json")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["error"]).To(Equal("unknown_cassette"))
		})

		It("404s a name that is not a legal cassette name", func() {
			response, _ := get("/v1/cassettes/Summary/openapi.json")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		})

		It("503s an installed cassette whose spec has never been fetched", func() {
			delete(source.documents, "summary")

			response, body := get("/v1/cassettes/summary/openapi.json")
			Expect(response.StatusCode).To(Equal(http.StatusServiceUnavailable))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["error"]).To(Equal("spec_unavailable"))
		})
	})

	Describe("the aggregated spec", func() {
		It("serves the merged document only at its canonical path", func() {
			response, body := get("/openapi")
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			Expect(string(body)).To(MatchJSON(`{"openapi":"3.1.0","paths":{}}`))

			response, _ = get("/openapi.json")
			Expect(response.StatusCode).To(Equal(http.StatusNotFound))
		})

		It("reports a merge that failed rather than serving half a document", func() {
			source.err = errors.New("boom")

			response, body := get("/openapi")
			Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))

			var decoded map[string]string
			Expect(json.Unmarshal(body, &decoded)).To(Succeed())
			Expect(decoded["error"]).To(Equal("aggregate_failed"))
		})

		It("merges against the live parser, not a second description of core", func() {
			_, _ = get("/openapi")

			Expect(source.base).To(BeIdenticalTo(server.openapi),
				"core's half of the merge has to be the registrations themselves; "+
					"any other description of core is a copy that can disagree with the routes")
		})
	})

	Describe("server-owned cassette state", func() {
		It("retries unresolved startup sources even when periodic refresh is disabled", func(ctx SpecContext) {
			var ready atomic.Bool
			cassetteServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				if !ready.Load() {
					_, _ = writer.Write([]byte(`{"openapi":"3.1.0","paths":{}}`))
					return
				}
				_, _ = writer.Write([]byte(`{
					"openapi":"3.1.0",
					"x-tapes-cassette":{
						"kind":"cassette/v1alpha1",
						"cassette":{"name":"runtime","version":"1.0.0"},
						"depends":{"core":"v1"},
						"api":{"prefix_path":"api"}
					},
					"paths":{"/api/runtime/ping":{"get":{"responses":{"200":{"description":"pong"}}}}}
				}`))
			}))
			DeferCleanup(cassetteServer.Close)

			built, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())
			built.SetCassetteSources([]string{cassetteServer.URL + "/openapi"})
			built.StartCassetteSpecRefresh(ctx, 0)
			time.AfterFunc(100*time.Millisecond, func() { ready.Store(true) })
			Eventually(built.cassettes.Instances).WithTimeout(2 * time.Second).Should(HaveLen(1))
		})

		It("admits embedded metadata, routing, discovery, and specs through one runtime", func() {
			cassetteServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(`{
					"openapi":"3.1.0",
					"info":{"title":"Runtime","version":"1.0.0"},
					"x-tapes-cassette":{
						"kind":"cassette/v1alpha1",
						"cassette":{"name":"runtime","version":"1.0.0","display_name":"Runtime"},
						"depends":{"core":"v1"},
						"api":{"health":"/ping","openapi":"/openapi","prefix_path":"api"}
					},
					"paths":{"/api/runtime/ping":{"get":{"responses":{"200":{"description":"pong"}}}}}
				}`))
			}))
			DeferCleanup(cassetteServer.Close)

			built, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())
			built.SetCassetteSources([]string{cassetteServer.URL + "/openapi"})
			Expect(built.RefreshCassetteSpecs(context.Background())).To(BeEmpty())

			response, body := do(built,
				httptest.NewRequest(http.MethodGet, "/v1/cassettes/runtime/openapi.json", nil))
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			var spec map[string]any
			Expect(json.Unmarshal(body, &spec)).To(Succeed())
			Expect(spec["paths"]).To(HaveKey("/v1/cassettes/runtime/ping"))

			response, body = do(built, httptest.NewRequest(http.MethodGet, "/v1/cassettes", nil))
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			var discovery Discovery
			Expect(json.Unmarshal(body, &discovery)).To(Succeed())
			Expect(discovery.Cassettes).To(HaveLen(1))
			Expect(discovery.Cassettes[0].OpenAPIStatus).To(Equal(tapesoapi.Fresh))
		})
	})

	Describe("a core with no cassettes", func() {
		It("publishes an empty discovery document", func() {
			plain, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())

			response, body := do(plain, httptest.NewRequest(http.MethodGet, "/v1/cassettes", nil))
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			var document Discovery
			Expect(json.Unmarshal(body, &document)).To(Succeed())
			Expect(document.Cassettes).To(BeEmpty())
			Expect(document.Problems).To(BeEmpty())

			response, _ = do(plain, httptest.NewRequest(http.MethodGet, "/ping", nil))
			Expect(response.StatusCode).To(Equal(http.StatusOK))
		})
	})
})

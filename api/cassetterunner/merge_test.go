package cassetterunner_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/openapi"
)

// spec is a cassette's OpenAPI document, parameterised by the paths it claims
// so containment can be exercised without a second fixture.
//
// A cassette always describes the paths it serves on its own listener — the
// only surface it can honestly describe — so specs build documents with local
// paths and assert against public ones. The gap between the two arguments is
// the republication this package performs.
func spec(name string, paths ...string) string {
	quoted := make([]string, 0, len(paths))
	for _, path := range paths {
		quoted = append(quoted, `"`+path+`": {"get": {"operationId": "read", "responses": {`+
			`"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Row"}}}}}}}`)
	}

	return fmt.Sprintf(`{
  "openapi": "3.1.0",
  "info": {"title": %q, "version": "0.3.1"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": %q, "version": "0.3.1"},
    "depends": {"core": "v1"},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"}
  },
  "paths": {%s},
  "components": {"schemas": {"Row": {"type": "object", "x-vendor": {"$ref": "#/components/schemas/Row"}}}}
}`, name, name, strings.Join(quoted, ","))
}

// bareSpec is a document with only the manifest and the paths given, for the
// cases that assert on what republication does to a document rather than on
// the document itself.
func bareSpec(name, prefixPath, paths string) string {
	return fmt.Sprintf(`{
  "openapi": "3.1.0",
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": %q, "version": "0.3.1"},
    "depends": {"core": "v1"},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": %q}
  }%s
}`, name, prefixPath, paths)
}

// cassetteServer stands in for a running cassette, counting spec fetches so a
// conditional request can be told from a full one.
type cassetteServer struct {
	*httptest.Server
	document atomic.Pointer[string]
	etag     atomic.Pointer[string]
	status   atomic.Int32
	fetches  atomic.Int32
}

func newCassetteServer(document string) *cassetteServer {
	server := &cassetteServer{}
	server.document.Store(&document)
	server.status.Store(http.StatusOK)

	server.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		server.fetches.Add(1)

		if tag := server.etag.Load(); tag != nil {
			if request.Header.Get("If-None-Match") == *tag {
				writer.WriteHeader(http.StatusNotModified)

				return
			}
			writer.Header().Set("ETag", *tag)
		}
		if code := int(server.status.Load()); code != http.StatusOK {
			writer.WriteHeader(code)

			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(*server.document.Load()))
	}))

	return server
}

// resolve points a fresh runner at a cassette server.
func resolve(target *cassetteServer) *cassetterunner.Runner {
	runner := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
	runner.SetSources([]string{target.URL + "/openapi"})

	return runner
}

// decode reads a cached document back as a tree.
func decode(document []byte) map[string]any {
	var decoded map[string]any
	Expect(json.Unmarshal(document, &decoded)).To(Succeed())

	return decoded
}

var _ = Describe("Aggregator", func() {
	var (
		ctx        context.Context
		server     *cassetteServer
		aggregator *cassetterunner.Runner
	)

	BeforeEach(func() {
		ctx = context.Background()
		server = newCassetteServer(spec("summary", "/api/summary/reports"))
		DeferCleanup(server.Close)

		aggregator = resolve(server)
	})

	It("caches the document core serves rather than the one it fetched", func() {
		Expect(aggregator.Refresh(ctx)).To(BeEmpty())

		document, digest, ok := aggregator.Spec("summary")
		Expect(ok).To(BeTrue())
		Expect(document).To(MatchJSON(spec("summary", "/v1/cassettes/summary/reports")),
			"a client generating from this document has to be able to call what it describes")
		Expect(string(digest)).To(HavePrefix("sha256:"))
		Expect(aggregator.Status("summary")).To(Equal(openapi.Fresh))
	})

	It("digests the republished bytes, so moving the public surface moves the ETag", func() {
		Expect(aggregator.Refresh(ctx)).To(BeEmpty())
		document, digest, _ := aggregator.Spec("summary")

		sum := sha256.Sum256(document)
		Expect(string(digest)).To(Equal("sha256:"+hex.EncodeToString(sum[:])),
			"the digest is the ETag clients cache on, so it must cover the bytes they were handed")
	})

	It("drops the cassette's own servers array instead of republishing an unreachable origin", func() {
		document := `{
  "openapi": "3.1.0",
  "servers": [{"url": "http://172.17.0.4:8080"}],
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": "summary", "version": "0.3.1"},
    "depends": {"core": "v1"},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"}
  },
  "paths": {"/api/summary/reports": {"get": {"responses": {"200": {"description": "ok"}}}}}
}`
		server.document.Store(&document)
		Expect(aggregator.Refresh(ctx)).To(BeEmpty())

		cached, _, ok := aggregator.Spec("summary")
		Expect(ok).To(BeTrue())

		decoded := decode(cached)
		Expect(decoded).NotTo(HaveKey("servers"),
			"a document with no servers resolves against the origin it was served from, which is core")
		Expect(decoded["paths"]).To(HaveKey("/v1/cassettes/summary/reports"))
	})

	It("republishes a cassette that mounts directly under its own name", func() {
		document := bareSpec("summary", "/",
			`, "paths": {"/summary/reports": {"get": {"responses": {"200": {"description": "ok"}}}}}`)
		server.document.Store(&document)

		Expect(aggregator.Refresh(ctx)).To(BeEmpty())

		cached, _, ok := aggregator.Spec("summary")
		Expect(ok).To(BeTrue())
		Expect(decode(cached)["paths"]).To(HaveKey("/v1/cassettes/summary/reports"),
			"where a cassette serves its API does not change where core publishes it")
	})

	It("reports a cassette it has never reached as missing, and says why on the source", func() {
		_, _, ok := aggregator.Spec("summary")
		Expect(ok).To(BeFalse())
		Expect(aggregator.Status("summary")).To(Equal(openapi.Missing))

		server.status.Store(http.StatusInternalServerError)
		Expect(aggregator.Refresh(ctx)).To(HaveLen(1))

		Expect(aggregator.Status("summary")).To(Equal(openapi.Missing))
		rejections := aggregator.Registry().Rejections()
		Expect(rejections).To(HaveLen(1))
		Expect(rejections[0].Reason).To(ContainSubstring("500"),
			"a source that never resolved has no cassette name to file the problem under, only a URL")
	})

	It("keeps serving the last good document when the cassette goes away", func() {
		Expect(aggregator.Refresh(ctx)).To(BeEmpty())
		server.Close()

		Expect(aggregator.Refresh(ctx)).To(HaveLen(1))

		document, _, ok := aggregator.Spec("summary")
		Expect(ok).To(BeTrue(), "erasing a client's surface because a container restarted is the worse outcome")
		Expect(document).To(MatchJSON(spec("summary", "/v1/cassettes/summary/reports")))
		Expect(aggregator.Status("summary")).To(Equal(openapi.Stale))
		Expect(aggregator.Problem("summary")).NotTo(BeEmpty())
	})

	It("sends a conditional request once it has an ETag and treats 304 as fresh", func() {
		etag := `"v1"`
		server.etag.Store(&etag)

		Expect(aggregator.Refresh(ctx)).To(BeEmpty())
		_, first, _ := aggregator.Spec("summary")

		Expect(aggregator.Refresh(ctx)).To(BeEmpty())
		Expect(server.fetches.Load()).To(BeEquivalentTo(2))

		_, second, ok := aggregator.Spec("summary")
		Expect(ok).To(BeTrue())
		Expect(second).To(Equal(first), "a 304 must not disturb the cached digest clients are keyed on")
		Expect(aggregator.Status("summary")).To(Equal(openapi.Fresh))
	})

	It("recovers to fresh after the cassette comes back", func() {
		Expect(aggregator.Refresh(ctx)).To(BeEmpty())

		server.status.Store(http.StatusBadGateway)
		Expect(aggregator.Refresh(ctx)).To(HaveLen(1))
		Expect(aggregator.Status("summary")).To(Equal(openapi.Stale))

		server.status.Store(http.StatusOK)
		Expect(aggregator.Refresh(ctx)).To(BeEmpty())
		Expect(aggregator.Status("summary")).To(Equal(openapi.Fresh))
		Expect(aggregator.Problem("summary")).To(BeEmpty())
	})

	Describe("prefix containment", func() {
		It("refuses the whole document when one path escapes the cassette's prefix", func() {
			document := spec("summary", "/api/summary/reports", "/v1/sessions")
			server.document.Store(&document)

			errs := aggregator.Refresh(ctx)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0]).To(MatchError(ContainSubstring("outside /api/summary")))
			Expect(errs[0]).To(MatchError(ContainSubstring("the whole document is refused")))
			Expect(errs[0]).To(MatchError(ContainSubstring("/v1/sessions")))

			_, _, ok := aggregator.Spec("summary")
			Expect(ok).To(BeFalse(), "a spec that tried is not a spec to trust the rest of")
		})

		It("compares whole segments, so /api/sum cannot admit /api/summary", func() {
			document := spec("sum", "/api/summary/reports")
			server.document.Store(&document)

			errs := aggregator.Refresh(ctx)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0]).To(MatchError(ContainSubstring("outside /api/sum")))
		})

		It("refuses a document with no paths object at all", func() {
			document := bareSpec("summary", "api", "")
			server.document.Store(&document)

			errs := aggregator.Refresh(ctx)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0]).To(MatchError(ContainSubstring("no paths object")))
		})

		It("refuses a response that is not a JSON object", func() {
			document := `["not", "a", "spec"]`
			server.document.Store(&document)

			Expect(aggregator.Refresh(ctx)).To(HaveLen(1))
		})
	})

	Describe("Document", func() {
		mergeOf := func(runner *cassetterunner.Runner) map[string]any {
			encoded, err := runner.Document()
			Expect(err).NotTo(HaveOccurred())

			return decode(encoded)
		}

		It("describes an install with no cassettes without failing", func() {
			merged := mergeOf(cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()}))
			Expect(merged["openapi"]).To(Equal("3.1.0"))
			Expect(merged["paths"]).To(BeEmpty())
			Expect(merged).NotTo(HaveKey("components"))
		})

		It("merges every cached cassette, namespacing components and rewriting refs", func() {
			second := newCassetteServer(spec("reports", "/api/reports/daily"))
			DeferCleanup(second.Close)

			aggregator.SetSources([]string{server.URL + "/openapi", second.URL + "/openapi"})
			Expect(aggregator.Refresh(ctx)).To(BeEmpty())
			merged := mergeOf(aggregator)

			paths, ok := merged["paths"].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(paths).To(HaveKey("/v1/cassettes/summary/reports"))
			Expect(paths).To(HaveKey("/v1/cassettes/reports/daily"),
				"two cassettes that both mount under /api do not collide once each is republished under its own name")

			schemas := merged["components"].(map[string]any)["schemas"].(map[string]any)
			Expect(schemas).To(HaveKey("summary_Row"))
			Expect(schemas).To(HaveKey("reports_Row"),
				"OpenAPI's component space is flat, so two cassettes may both define a Row")

			operation := paths["/v1/cassettes/summary/reports"].(map[string]any)["get"].(map[string]any)
			reference := operation["responses"].(map[string]any)["200"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)["$ref"]
			Expect(reference).To(Equal("#/components/schemas/summary_Row"))

			Expect(operation["operationId"]).To(Equal("read"),
				"an operationId is a billing label and part of the contract, so a merge step must not rename it")
		})

		It("rewrites refs anywhere in the tree, including fields it has never heard of", func() {
			Expect(aggregator.Refresh(ctx)).To(BeEmpty())
			merged := mergeOf(aggregator)

			row := merged["components"].(map[string]any)["schemas"].(map[string]any)["summary_Row"].(map[string]any)
			Expect(row["x-vendor"].(map[string]any)["$ref"]).To(Equal("#/components/schemas/summary_Row"),
				"a vendor extension survives the round trip and is rewritten with everything else")
		})

		It("hyphenates a cassette name into a legal component namespace", func() {
			hyphenated := newCassetteServer(spec("hello-world", "/api/hello-world/hello"))
			DeferCleanup(hyphenated.Close)

			one := resolve(hyphenated)
			Expect(one.Refresh(ctx)).To(BeEmpty())

			schemas := mergeOf(one)["components"].(map[string]any)["schemas"].(map[string]any)
			Expect(schemas).To(HaveKey("hello_world_Row"))
		})

		It("leaves a reference into another document alone", func() {
			document := bareSpec("summary", "api",
				`, "paths": {"/api/summary/reports": {"get": {"responses": {"200": {"$ref": "https://example.com/spec.json#/x"}}}}}`)
			server.document.Store(&document)
			Expect(aggregator.Refresh(ctx)).To(BeEmpty())

			paths := mergeOf(aggregator)["paths"].(map[string]any)
			responses := paths["/v1/cassettes/summary/reports"].(map[string]any)["get"].(map[string]any)["responses"].(map[string]any)
			Expect(responses["200"].(map[string]any)["$ref"]).To(Equal("https://example.com/spec.json#/x"),
				"core does not resolve remote refs, and pretending to would be worse than leaving them visible")
		})

		It("omits a cassette that has never been fetched", func() {
			Expect(mergeOf(aggregator)["paths"]).To(BeEmpty())
		})
	})
})

var _ = Describe("Status", func() {
	It("reports a name it has never seen as missing", func() {
		runner := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		Expect(runner.Status(cassette.Name("absent"))).To(Equal(openapi.Missing))
	})
})

package cassetterunner_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/openapi"
)

func sourceDocument(name string) string {
	return fmt.Sprintf(`{
  "openapi": "3.1.0",
  "info": {"title": %q, "version": "1.0.0"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": %q, "version": "1.0.0", "display_name": %q},
    "depends": {"core": "v1", "views": ["spans"]},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"},
    "tables": [{"name": "results"}],
    "config": [{"key": "model", "type": "string", "default": "small"}]
  },
  "paths": {"/api/%s/results": {"get": {"operationId": "%s.results", "responses": {"200": {"description": "ok"}}}}}
}`, name, name, name, name, name)
}

type mutableSource struct {
	*httptest.Server
	document atomic.Pointer[string]
	etag     atomic.Pointer[string]
	status   atomic.Int32
	path     atomic.Pointer[string]
}

func newMutableSource(document string) *mutableSource {
	source := &mutableSource{}
	source.document.Store(&document)
	source.status.Store(http.StatusOK)
	source.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		path := request.URL.RequestURI()
		source.path.Store(&path)
		writer.Header().Set("Content-Type", "application/json")
		if status := int(source.status.Load()); status != http.StatusOK {
			writer.WriteHeader(status)
			return
		}
		if etag := source.etag.Load(); etag != nil {
			writer.Header().Set("ETag", *etag)
			if request.Header.Get("If-None-Match") == *etag {
				writer.WriteHeader(http.StatusNotModified)

				return
			}
		}
		_, _ = writer.Write([]byte(*source.document.Load()))
	}))
	return source
}

var _ = Describe("OpenAPI cassette sources", func() {
	It("registers from embedded versioned metadata and republishes the same document", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/contracts/openapi?format=json"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(*source.path.Load()).To(Equal("/contracts/openapi?format=json"))
		instances := registry.Instances()
		Expect(instances).To(HaveLen(1))
		Expect(instances[0].Name).To(Equal(cassette.Name("summary")))
		Expect(instances[0].Source).To(Equal(source.URL + "/contracts/openapi?format=json"))
		Expect(instances[0].URL).To(Equal(source.URL))
		Expect(string(instances[0].Digest)).To(HavePrefix("sha256:"))

		published, _, ok := runtime.Spec("summary")
		Expect(ok).To(BeTrue())
		Expect(published).To(MatchJSON(`{
  "openapi": "3.1.0",
  "info": {"title": "summary", "version": "1.0.0"},
  "x-tapes-cassette": {
    "kind": "cassette/v1alpha1",
    "cassette": {"name": "summary", "version": "1.0.0", "display_name": "summary"},
    "depends": {"core": "v1", "views": ["spans"]},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"},
    "tables": [{"name": "results"}],
    "config": [{"key": "model", "type": "string", "default": "small"}]
  },
  "paths": {"/v1/cassettes/summary/results": {"get": {"operationId": "summary.results", "responses": {"200": {"description": "ok"}}}}}
}`))
		Expect(runtime.Status("summary")).To(Equal(openapi.Fresh))
	})

	It("keeps unresolved sources retryable", func(ctx SpecContext) {
		source := newMutableSource(`{"openapi":"3.1.0","paths":{}}`)
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(BeEmpty())
		Expect(registry.Rejections()).To(HaveLen(1))

		valid := sourceDocument("summary")
		source.document.Store(&valid)
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.Rejections()).To(BeEmpty())
	})

	It("keeps the configured-order winner while preserving valid peers", func(ctx SpecContext) {
		first := newMutableSource(sourceDocument("summary"))
		duplicate := newMutableSource(sourceDocument("summary"))
		reports := newMutableSource(sourceDocument("reports"))
		defer first.Close()
		defer duplicate.Close()
		defer reports.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{first.URL + "/openapi", duplicate.URL + "/openapi", reports.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(HaveLen(2))
		Expect(registry.Instances()[0].Name).To(Equal(cassette.Name("reports")))
		Expect(registry.Instances()[1].Source).To(Equal(first.URL + "/openapi"))
		Expect(registry.Rejections()).To(ConsistOf(cassetterunner.Rejection{
			Subject: duplicate.URL + "/openapi",
			Reason:  fmt.Sprintf("cassette %q is already registered by earlier source %s", "summary", first.URL+"/openapi"),
		}))
	})

	It("withdraws a cassette when its source is removed", func(ctx SpecContext) {
		summary := newMutableSource(sourceDocument("summary"))
		reports := newMutableSource(sourceDocument("reports"))
		defer summary.Close()
		defer reports.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		reportsURL := reports.URL + "/openapi"
		runtime.SetSources([]string{summary.URL + "/openapi", reportsURL})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(2))
		summary.status.Store(http.StatusInternalServerError)
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Rejections()).To(HaveLen(1))

		runtime.SetSources([]string{reportsURL})

		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.Rejections()).To(BeEmpty())
		Expect(registry.Instances()[0].Name).To(Equal(cassette.Name("reports")))
		_, _, found := registry.Lookup("/v1/cassettes/summary/results")
		Expect(found).To(BeFalse())
		_, _, found = runtime.Spec("summary")
		Expect(found).To(BeFalse())
		Expect(runtime.Status("summary")).To(Equal(openapi.Missing))

		merged, err := runtime.Document()
		Expect(err).NotTo(HaveOccurred())
		document, err := openapi.Parse(merged)
		Expect(err).NotTo(HaveOccurred())
		paths, err := document.Paths()
		Expect(err).NotTo(HaveOccurred())
		Expect(paths).To(ConsistOf("/v1/cassettes/reports/results"))
	})

	It("does not withdraw a URL when only a repeated occurrence is removed", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		sourceURL := source.URL + "/openapi"
		runtime.SetSources([]string{sourceURL, sourceURL})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(1))

		runtime.SetSources([]string{sourceURL})
		Expect(registry.Instances()).To(HaveLen(1))
		_, _, found := runtime.Spec("summary")
		Expect(found).To(BeTrue())
	})

	It("lets a retained duplicate take ownership when the earlier source is removed", func(ctx SpecContext) {
		first := newMutableSource(sourceDocument("summary"))
		duplicate := newMutableSource(sourceDocument("summary"))
		defer first.Close()
		defer duplicate.Close()

		validator := `"summary-v1"`
		duplicate.etag.Store(&validator)
		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		firstURL := first.URL + "/openapi"
		duplicateURL := duplicate.URL + "/openapi"
		runtime.SetSources([]string{duplicateURL})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()[0].Source).To(Equal(duplicateURL))

		runtime.SetSources([]string{firstURL, duplicateURL})
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()[0].Source).To(Equal(firstURL))

		runtime.SetSources([]string{duplicateURL})
		Expect(registry.Instances()).To(BeEmpty())
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.Instances()[0].Source).To(Equal(duplicateURL))
		Expect(registry.Rejections()).To(BeEmpty())
	})

	It("lets a recovered earlier source reclaim configured priority", func(ctx SpecContext) {
		first := newMutableSource(`{"openapi":"3.1.0","paths":{}}`)
		later := newMutableSource(sourceDocument("summary"))
		defer first.Close()
		defer later.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{first.URL + "/openapi", later.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.Instances()[0].Source).To(Equal(later.URL + "/openapi"))

		recovered := sourceDocument("summary")
		first.document.Store(&recovered)
		Expect(runtime.Refresh(ctx)).To(HaveLen(1), "the later duplicate remains a reported rejection")
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.Instances()[0].Source).To(Equal(first.URL + "/openapi"))
		Expect(runtime.Status("summary")).To(Equal(openapi.Fresh), "a losing duplicate must not stale the winner")
	})

	It("rejects redirects so document admission and proxy origin cannot diverge", func(ctx SpecContext) {
		target := newMutableSource(sourceDocument("summary"))
		defer target.Close()
		redirect := httptest.NewServer(http.RedirectHandler(target.URL+"/openapi", http.StatusTemporaryRedirect))
		defer redirect.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{redirect.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(BeEmpty())
	})

	It("retains the last good instance and spec when refreshed metadata changes identity", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		before, beforeDigest, ok := runtime.Spec("summary")
		Expect(ok).To(BeTrue())

		changed := sourceDocument("renamed")
		source.document.Store(&changed)
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(HaveLen(1))
		Expect(registry.Instances()[0].Name).To(Equal(cassette.Name("summary")))
		after, afterDigest, ok := runtime.Spec("summary")
		Expect(ok).To(BeTrue())
		Expect(after).To(Equal(before))
		Expect(afterDigest).To(Equal(beforeDigest))
		Expect(runtime.Status("summary")).To(Equal(openapi.Stale))
	})

	DescribeTable("never publishes credentials from a rejected source URL",
		func(ctx SpecContext, source string) {
			registry := cassetterunner.NewRegistry()
			runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
			runtime.SetSources([]string{source})
			errs := runtime.Refresh(ctx)
			Expect(errs).To(HaveLen(1))
			Expect(errs[0].Error()).NotTo(ContainSubstring("secret"))
			Expect(registry.Rejections()).To(HaveLen(1))
			Expect(registry.Rejections()[0].Subject).NotTo(ContainSubstring("secret"))
		},
		Entry("parseable userinfo", "http://user:secret@example.invalid/openapi"),
		Entry("malformed userinfo", "http://user:secret%zz@example.invalid/openapi"),
	)

	It("honors cancellation while fetching", func() {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()
		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
	})

	// Admission is decided against the contract set the owning core injects,
	// not against anything this package knows. These cover both directions of
	// that: the same document that is admitted above is refused by a core
	// serving a different contract, and admitted by one serving several.
	It("refuses a cassette depending on a contract this core does not serve", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: []cassette.ContractVersion{"v2"}})
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(BeEmpty())
		Expect(registry.Rejections()).To(HaveLen(1))
		Expect(registry.Rejections()[0].Reason).To(ContainSubstring(`contract version "v1" is not supported`))
	})

	It("admits a cassette depending on any contract this core still serves", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: []cassette.ContractVersion{"v1", "v2"}})
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(registry.Instances()).To(HaveLen(1))
	})

	It("admits nothing when it was never told what its core serves", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry})
		runtime.SetSources([]string{source.URL + "/openapi"})

		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()).To(BeEmpty())
	})
})

package cassetterunner_test

import (
	"net/http"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

var _ = Describe("source admission evidence", func() {
	It("reports an admitted source with the identities core is serving it under", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence).To(HaveLen(1))
		Expect(evidence[0].Source).To(Equal(source.URL + "/openapi"))
		Expect(evidence[0].Name).To(Equal(cassette.Name("summary")))
		Expect(evidence[0].Admission).To(Equal(cassetterunner.AdmissionAdmitted))
		Expect(evidence[0].AdmittedAt).NotTo(BeZero())
		Expect(evidence[0].RoutePrefix).To(Equal("/v1/cassettes/summary"))
		Expect(string(evidence[0].ManifestDigest)).To(HavePrefix("sha256:"))
		Expect(string(evidence[0].OpenAPIDigest)).To(HavePrefix("sha256:"))
		Expect(evidence[0].OpenAPIStatus).To(Equal(tapesoapi.Fresh))
		Expect(evidence[0].Rejection).To(BeNil())
		Expect(evidence[0].ObservedAt).To(BeZero())
	})

	It("reports a source nobody has visited yet as unresolved", func() {
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{"http://cassette.invalid/openapi"})

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence).To(HaveLen(1))
		Expect(evidence[0].Admission).To(Equal(cassetterunner.AdmissionUnresolved),
			"the zero Admission must not read as a state no pass has proved")
		Expect(evidence[0].Name).To(BeEmpty())
		Expect(evidence[0].Rejection).To(BeNil())
	})

	// The distinction this draws is the one the registry cannot: a source core
	// could not read and a source core read and refused are both "no cassette",
	// and they call for opposite actions.
	It("separates a source it could not read from a document it refused", func(ctx SpecContext) {
		unreachable := newMutableSource(sourceDocument("reports"))
		unreachable.Close()
		refused := newMutableSource(`{"openapi":"3.1.0","paths":{}}`)
		defer refused.Close()

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{unreachable.URL + "/openapi", refused.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(HaveLen(2))

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence).To(HaveLen(2))

		Expect(evidence[0].Admission).To(Equal(cassetterunner.AdmissionUnresolved))
		Expect(evidence[0].Rejection).NotTo(BeNil())
		Expect(evidence[0].Rejection.Subject).To(Equal(unreachable.URL + "/openapi"))
		Expect(evidence[0].Rejection.Reason).To(ContainSubstring("connect"))
		Expect(evidence[0].ObservedAt).NotTo(BeZero())

		Expect(evidence[1].Admission).To(Equal(cassetterunner.AdmissionRejected))
		Expect(evidence[1].Rejection).NotTo(BeNil())
		Expect(evidence[1].Rejection.Subject).To(Equal(refused.URL + "/openapi"))
		Expect(evidence[1].Rejection.Reason).To(ContainSubstring("missing root extension"))
	})

	It("keeps the cassette identity when an admitted source is later refused", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		admittedAt := runtime.EvidenceSnapshot().Sources[0].AdmittedAt

		broken := `{"openapi":"3.1.0","paths":{}}`
		source.document.Store(&broken)
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence[0].Name).To(Equal(cassette.Name("summary")),
			"a refused refresh does not un-name the cassette the route still serves")
		Expect(evidence[0].Admission).To(Equal(cassetterunner.AdmissionRejected))
		Expect(evidence[0].OpenAPIStatus).To(Equal(tapesoapi.Stale))
		Expect(evidence[0].Rejection.Reason).To(ContainSubstring("missing root extension"))
		Expect(evidence[0].ObservedAt).NotTo(BeZero())
		Expect(evidence[0].AdmittedAt).To(Equal(admittedAt),
			"admittedAt is when the admission began, not when it was last checked")
	})

	// A timestamp that only moved when the reason changed would leave a reader
	// unable to tell a current rejection from one a later pass had cleared.
	It("restamps the observation on every failing pass", func(ctx SpecContext) {
		source := newMutableSource(`{"openapi":"3.1.0","paths":{}}`)
		defer source.Close()

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		first := runtime.EvidenceSnapshot().Sources[0].ObservedAt

		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(runtime.EvidenceSnapshot().Sources[0].ObservedAt).To(BeTemporally(">", first))
	})

	It("re-admits a source that recovers through revalidation", func(ctx SpecContext) {
		source := newMutableSource(sourceDocument("summary"))
		defer source.Close()
		etag := `"v1"`
		source.etag.Store(&etag)

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{source.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		// A transport failure refuses the source without disturbing the
		// cached document or the registry, so the next 304 is a recovery.
		source.status.Store(http.StatusInternalServerError)
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(runtime.EvidenceSnapshot().Sources[0].Admission).To(Equal(cassetterunner.AdmissionUnresolved))

		source.status.Store(http.StatusOK)
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence[0].Admission).To(Equal(cassetterunner.AdmissionAdmitted))
		Expect(evidence[0].Rejection).To(BeNil())
		Expect(evidence[0].ObservedAt).To(BeZero())
	})

	It("drops evidence for a source that is no longer configured", func(ctx SpecContext) {
		summary := newMutableSource(sourceDocument("summary"))
		reports := newMutableSource(sourceDocument("reports"))
		defer summary.Close()
		defer reports.Close()

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources([]string{summary.URL + "/openapi", reports.URL + "/openapi"})
		Expect(runtime.Refresh(ctx)).To(BeEmpty())
		Expect(runtime.EvidenceSnapshot().Sources).To(HaveLen(2))

		runtime.SetSources([]string{summary.URL + "/openapi"})

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence).To(HaveLen(1))
		Expect(evidence[0].Name).To(Equal(cassette.Name("summary")))
	})

	// A cassette name is not a unique key: two configured sources can claim
	// one, and only the earlier one publishes. Reading the loser's evidence by
	// name would hand it the winner's freshness and digests, which reads as
	// the rejected source serving the document core is actually serving.
	It("does not let a duplicate that lost priority report the winner's document", func(ctx SpecContext) {
		first := newMutableSource(`{"openapi":"3.1.0","paths":{}}`)
		later := newMutableSource(sourceDocument("summary"))
		defer first.Close()
		defer later.Close()

		registry := cassetterunner.NewRegistry()
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Registry: registry, Contracts: servedContracts()})
		firstURL := first.URL + "/openapi"
		laterURL := later.URL + "/openapi"
		runtime.SetSources([]string{firstURL, laterURL})
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()[0].Source).To(Equal(laterURL))

		// The earlier source starts serving the same cassette, so it takes
		// the publication and the later one becomes a rejected duplicate that
		// keeps the name it was admitted under.
		recovered := sourceDocument("summary")
		first.document.Store(&recovered)
		Expect(runtime.Refresh(ctx)).To(HaveLen(1))
		Expect(registry.Instances()[0].Source).To(Equal(firstURL))

		evidence := runtime.EvidenceSnapshot().Sources
		Expect(evidence).To(HaveLen(2))

		winner := evidence[0]
		Expect(winner.Source).To(Equal(firstURL))
		Expect(winner.Admission).To(Equal(cassetterunner.AdmissionAdmitted))
		Expect(winner.OpenAPIStatus).To(Equal(tapesoapi.Fresh))
		Expect(string(winner.ManifestDigest)).To(HavePrefix("sha256:"))
		Expect(string(winner.OpenAPIDigest)).To(HavePrefix("sha256:"))

		loser := evidence[1]
		Expect(loser.Source).To(Equal(laterURL))
		Expect(loser.Name).To(Equal(cassette.Name("summary")),
			"the loser keeps the name it was admitted under")
		Expect(loser.Admission).To(Equal(cassetterunner.AdmissionRejected))
		Expect(loser.OpenAPIStatus).To(Equal(tapesoapi.Missing),
			"a source publishing nothing must not inherit the winner's freshness")
		Expect(loser.OpenAPIDigest).To(BeEmpty(),
			"a digest here would credit the loser with the document core serves")
		Expect(loser.ManifestDigest).To(BeEmpty())
		Expect(loser.Rejection).NotTo(BeNil())
	})

	It("reports the identity of the source list its sources came from", func(ctx SpecContext) {
		summary := newMutableSource(sourceDocument("summary"))
		reports := newMutableSource(sourceDocument("reports"))
		defer summary.Close()
		defer reports.Close()

		catalog := []string{summary.URL + "/openapi", reports.URL + "/openapi"}
		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources(catalog)
		Expect(runtime.Refresh(ctx)).To(BeEmpty())

		snapshot := runtime.EvidenceSnapshot()
		Expect(snapshot.SourceList.Digest).To(Equal(cassetterunner.SourceListDigest(catalog)))
		Expect(snapshot.SourceList.Count).To(Equal(2))
		Expect(snapshot.SourceList.LoadedAt).NotTo(BeZero())
		Expect(snapshot.Sources).To(HaveLen(2))
	})

	// The snapshot is what a convergence check reads, so a reconfiguration
	// landing between its two halves would let a checker conclude that a
	// configuration took while it is looking at the sources of another one.
	It("never mixes one configuration's identity with another's sources", func() {
		first := newMutableSource(sourceDocument("summary"))
		second := newMutableSource(sourceDocument("reports"))
		defer first.Close()
		defer second.Close()

		catalogs := [][]string{
			{first.URL + "/openapi"},
			{first.URL + "/openapi", second.URL + "/openapi"},
			{second.URL + "/openapi"},
			{},
		}

		runtime := cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runtime.SetSources(catalogs[0])

		var group sync.WaitGroup
		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for pass := range 3000 {
				runtime.SetSources(catalogs[pass%len(catalogs)])
			}
		}()

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range 3000 {
				snapshot := runtime.EvidenceSnapshot()
				urls := make([]string, 0, len(snapshot.Sources))
				for _, source := range snapshot.Sources {
					urls = append(urls, source.Source)
				}
				// The exact accounting the evidence endpoint publishes, and
				// the stronger claim behind it: these *are* the sources the
				// reported digest was computed over.
				Expect(snapshot.SourceList.Count).To(Equal(len(urls)))
				Expect(snapshot.SourceList.Digest).To(Equal(cassetterunner.SourceListDigest(urls)))
			}
		}()

		group.Wait()
	})
})

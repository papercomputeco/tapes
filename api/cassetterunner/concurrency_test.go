package cassetterunner_test

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// The runner is refreshed on a ticker while request handlers read it, so every
// reader has to be safe against a pass that is publishing. These specs exist to
// give the race detector something to find: the rest of the suite is
// single-goroutine and would not catch a lock that was dropped.
var _ = Describe("concurrent access", func() {
	var (
		source *mutableSource
		runner *cassetterunner.Runner
	)

	BeforeEach(func() {
		source = newMutableSource(sourceDocument("summary"))
		DeferCleanup(source.Close)

		runner = cassetterunner.NewRunner(cassetterunner.Config{Contracts: servedContracts()})
		runner.SetSources([]string{source.URL + "/openapi"})
	})

	// hammer runs every reader repeatedly against a runner that is refreshing.
	hammer := func(ctx context.Context, passes int) {
		var group sync.WaitGroup

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range passes {
				runner.Refresh(ctx)
			}
		}()

		readers := []func(){
			func() { runner.Status("summary") },
			func() { runner.Spec("summary") },
			func() { runner.Problem("summary") },
			func() { _, _ = runner.Document(context.Background(), nil) },
			func() { runner.Registry().Instances() },
			func() { runner.Registry().Rejections() },
			func() { runner.Registry().Lookup("/v1/cassettes/summary/results") },
		}
		for _, read := range readers {
			group.Add(1)
			go func() {
				defer GinkgoRecover()
				defer group.Done()
				for range passes {
					read()
				}
			}()
		}

		group.Wait()
	}

	It("serves readers while a refresh publishes", func(ctx SpecContext) {
		hammer(ctx, 20)

		Expect(runner.Registry().Instances()).To(HaveLen(1))
		_, _, ok := runner.Spec("summary")
		Expect(ok).To(BeTrue())
	})

	It("serves readers while a refresh is failing and recovering", func(ctx SpecContext) {
		Expect(runner.Refresh(ctx)).To(BeEmpty())

		var group sync.WaitGroup
		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for pass := range 20 {
				if pass%2 == 0 {
					source.status.Store(500)
				} else {
					source.status.Store(200)
				}
			}
		}()

		hammer(ctx, 20)
		group.Wait()

		// Whatever the last pass did, the document a client was handed is still
		// there — a flapping cassette must never erase its own published surface.
		_, _, ok := runner.Spec("summary")
		Expect(ok).To(BeTrue())
	})

	It("never hands out a cassette that is registered but has no document", func(ctx SpecContext) {
		var group sync.WaitGroup

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range 50 {
				runner.Refresh(ctx)
			}
		}()

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range 50 {
				for _, instance := range runner.Registry().Instances() {
					_, _, ok := runner.Spec(instance.Name)
					Expect(ok).To(BeTrue(),
						"a reader that can name a cassette must be able to fetch the spec core admitted it on")
				}
			}
		}()

		group.Wait()
	})

	// A withdrawal touches three places — the source catalog, the registry, and
	// the spec cache — and evidence reads all three. If they are not captured
	// together, a reader can be handed an entry that says a source is admitted
	// and mounted while reporting no digests and a Missing status, which is a
	// state the deployment was never in. A convergence check reading that would
	// conclude something about a configuration that never existed.
	It("never reports a source as admitted without the document that admitted it", func(ctx SpecContext) {
		second := newMutableSource(sourceDocument("reports"))
		DeferCleanup(second.Close)

		both := []string{source.URL + "/openapi", second.URL + "/openapi"}
		first := []string{source.URL + "/openapi"}

		reconfigured := make(chan struct{})

		var group sync.WaitGroup
		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			defer close(reconfigured)
			for range 30 {
				// Admit both, then withdraw the second: the catalog entry the
				// reader may already have copied is admitted, and the registry
				// and cache entries backing it are about to disappear.
				runner.SetSources(both)
				runner.Refresh(ctx)
				runner.SetSources(first)
			}
		}()

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for {
				select {
				case <-reconfigured:
					return
				default:
				}

				for _, entry := range runner.EvidenceSnapshot().Sources {
					if entry.Admission != cassetterunner.AdmissionAdmitted {
						continue
					}
					Expect(entry.Name).NotTo(BeEmpty(),
						"an admitted source names the cassette it produced")
					Expect(entry.ManifestDigest).NotTo(BeEmpty(),
						"an admitted source owns the registry entry its manifest digest comes from")
					Expect(entry.OpenAPIDigest).NotTo(BeEmpty(),
						"an admitted source owns a cached document")
					Expect(entry.OpenAPIStatus).NotTo(Equal(tapesoapi.Missing),
						"a source whose document is gone is not admitted")
				}
			}
		}()

		group.Wait()

		// The withdrawn source is gone from the evidence entirely, not left
		// behind as an admitted entry pointing at nothing.
		evidence := runner.EvidenceSnapshot()
		Expect(evidence.Sources).To(HaveLen(1))
		Expect(evidence.Sources[0].Source).To(Equal(source.URL + "/openapi"))
	})

	It("survives sources being reconfigured under a reader", func(ctx SpecContext) {
		second := newMutableSource(sourceDocument("reports"))
		DeferCleanup(second.Close)

		catalogs := [][]string{
			{source.URL + "/openapi"},
			{source.URL + "/openapi", second.URL + "/openapi"},
			{second.URL + "/openapi"},
		}

		var group sync.WaitGroup
		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for pass := range 30 {
				runner.SetSources(catalogs[pass%len(catalogs)])
			}
		}()

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range 30 {
				runner.Refresh(ctx)
			}
		}()

		group.Add(1)
		go func() {
			defer GinkgoRecover()
			defer group.Done()
			for range 30 {
				runner.Status(cassette.Name("summary"))
				runner.Registry().Instances()
			}
		}()

		group.Wait()
	})
})

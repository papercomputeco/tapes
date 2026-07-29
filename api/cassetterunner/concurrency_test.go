package cassetterunner_test

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
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
			func() { _, _ = runner.Document() },
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

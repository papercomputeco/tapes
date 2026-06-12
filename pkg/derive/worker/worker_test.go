package worker_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/derive/worker"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// gaugeValue scrapes one gauge from the worker's registry. Mirrors the
// API server's metrics-test convention of asserting on typed Gather()
// output rather than regexing the HTTP exposition.
func gaugeValue(reg *prometheus.Registry, name string) float64 {
	mfs, err := reg.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

// counterValue scrapes one (label-less) counter from the registry.
func counterValue(reg *prometheus.Registry, name string) float64 {
	mfs, err := reg.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// fakeStore is an in-memory worker.Store. It models the queue's
// conditional-clear semantics exactly (clear only when DirtiedAt is
// unchanged) so the worker's race handling is testable without
// Postgres — the Postgres implementation of the same contract is
// covered by the storagetest conformance specs.
type fakeStore struct {
	mu sync.Mutex

	queue map[string]storage.DeriveQueueEntry // key: org|harness|session
	locks map[string]bool

	// rawSessions is what SweepDeriveDirty enqueues; each entry's
	// DirtiedAt doubles as the session's last raw activity for the
	// sweep's activeSince bound.
	rawSessions []storage.DeriveQueueEntry

	// listErr makes ListDeriveDirty fail (models the DB outage the
	// worker must back off on instead of hot-looping).
	listErr   error
	listCalls int

	// deriveErr makes RederiveSession fail.
	deriveErr error

	// report, when set, is what RederiveSession returns instead of the
	// default healthy report.
	report *derive.RederiveReport

	// onDerive runs inside RederiveSession (under no lock held by the
	// test) before the derive is recorded — used to model a raw turn
	// landing mid-derive, or to block so the test can cancel mid-
	// derive. It receives the context the worker ran the derive on.
	onDerive func(ctx context.Context)

	derives []string
	clears  []string
	sweeps  int
}

func key(org, harness, session string) string { return org + "|" + harness + "|" + session }

func newFakeStore() *fakeStore {
	return &fakeStore{
		queue: map[string]storage.DeriveQueueEntry{},
		locks: map[string]bool{},
	}
}

func (f *fakeStore) mark(e storage.DeriveQueueEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue[key(e.OrgID, e.HarnessID, e.HarnessSessionID)] = e
}

func (f *fakeStore) entry(org, harness, session string) (storage.DeriveQueueEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.queue[key(org, harness, session)]
	return e, ok
}

func (f *fakeStore) deriveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.derives)
}

func (f *fakeStore) sweepCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sweeps
}

func (f *fakeStore) listCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

func (f *fakeStore) setListErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listErr = err
}

func (f *fakeStore) lockCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.locks)
}

func (f *fakeStore) ListDeriveDirty(_ context.Context, dirtiedBefore, _ time.Time, limit int32) ([]storage.DeriveQueueEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []storage.DeriveQueueEntry
	for _, e := range f.queue {
		if !e.DirtiedAt.After(dirtiedBefore) {
			out = append(out, e)
		}
		if int32(len(out)) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeStore) DeriveQueueStats(_ context.Context) (storage.DeriveQueueStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stats := storage.DeriveQueueStats{Depth: int64(len(f.queue))}
	for _, e := range f.queue {
		if stats.OldestDirtiedAt.IsZero() || e.DirtiedAt.Before(stats.OldestDirtiedAt) {
			stats.OldestDirtiedAt = e.DirtiedAt
		}
	}
	return stats, nil
}

func (f *fakeStore) GetDeriveDirty(_ context.Context, org, harness, session string) (*storage.DeriveQueueEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.queue[key(org, harness, session)]
	if !ok {
		return nil, nil
	}
	cp := e
	return &cp, nil
}

func (f *fakeStore) ClearDeriveDirty(_ context.Context, e storage.DeriveQueueEntry) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(e.OrgID, e.HarnessID, e.HarnessSessionID)
	f.clears = append(f.clears, k)
	cur, ok := f.queue[k]
	if !ok || !cur.DirtiedAt.Equal(e.DirtiedAt) {
		return false, nil
	}
	delete(f.queue, k)
	return true, nil
}

func (f *fakeStore) SweepDeriveDirty(_ context.Context, activeSince time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeps++
	var enqueued int64
	for _, e := range f.rawSessions {
		if !activeSince.IsZero() && e.DirtiedAt.Before(activeSince) {
			continue
		}
		k := key(e.OrgID, e.HarnessID, e.HarnessSessionID)
		if _, ok := f.queue[k]; ok {
			continue
		}
		f.queue[k] = e
		enqueued++
	}
	return enqueued, nil
}

func (f *fakeStore) TryDeriveSessionLock(_ context.Context, org, harness, session string) (func(), bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(org, harness, session)
	if f.locks[k] {
		return nil, false, nil
	}
	f.locks[k] = true
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.locks, k)
	}, true, nil
}

func (f *fakeStore) RederiveSession(ctx context.Context, _, org, harness, session string) (*derive.RederiveReport, error) {
	f.mu.Lock()
	err := f.deriveErr
	hook := f.onDerive
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if hook != nil {
		hook(ctx)
	}
	// A real derive's store operations fail once the work context is
	// canceled (drain timeout); the fake honors the same contract.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.mu.Lock()
	f.derives = append(f.derives, key(org, harness, session))
	report := f.report
	f.mu.Unlock()
	if report != nil {
		return report, nil
	}
	return &derive.RederiveReport{RawTurns: 3, ParsedTurns: 3, Nodes: 7, Upserted: 7}, nil
}

var _ = Describe("Worker", func() {
	var (
		store  *fakeStore
		ctx    context.Context
		cancel context.CancelFunc
		done   chan struct{}
	)

	settled := func(org, harness, session string) storage.DeriveQueueEntry {
		return storage.DeriveQueueEntry{
			OrgID:            org,
			HarnessID:        harness,
			HarnessSessionID: session,
			DirtiedAt:        time.Now().Add(-time.Minute),
		}
	}

	startWorker := func(cfg worker.Config) {
		w := worker.NewWorker(cfg, store, slog.New(slog.DiscardHandler))
		done = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			Expect(w.Run(ctx)).To(Succeed())
		}()
	}

	fastConfig := func() worker.Config {
		return worker.Config{
			PollInterval:  5 * time.Millisecond,
			Debounce:      30 * time.Millisecond,
			SweepInterval: time.Hour,
		}
	}

	BeforeEach(func() {
		store = newFakeStore()
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // fresh per-spec context; BeforeEach is not a loop
	})

	AfterEach(func() {
		cancel()
		if done != nil {
			Eventually(done).Should(BeClosed())
		}
	})

	It("derives a settled dirty session and clears it", func() {
		store.mark(settled("org-a", "claude-code", "sess-1"))

		startWorker(fastConfig())

		Eventually(store.deriveCount).Should(Equal(1))
		Eventually(func() bool {
			_, ok := store.entry("org-a", "claude-code", "sess-1")
			return ok
		}).Should(BeFalse(), "queue entry should be cleared after a clean derive")

		// Idempotence at the loop level: nothing left to derive.
		Consistently(store.deriveCount, "50ms").Should(Equal(1))
	})

	It("debounces sessions dirtied more recently than the window", func() {
		store.mark(storage.DeriveQueueEntry{
			OrgID:            "org-a",
			HarnessID:        "claude-code",
			HarnessSessionID: "sess-busy",
			DirtiedAt:        time.Now(),
		})

		cfg := fastConfig()
		cfg.Debounce = time.Hour
		startWorker(cfg)

		Consistently(store.deriveCount, "60ms").Should(BeZero(),
			"a freshly dirtied session must wait out the debounce")
	})

	It("skips sessions whose advisory lock is held elsewhere", func() {
		store.mark(settled("org-a", "claude-code", "sess-locked"))
		store.locks[key("org-a", "claude-code", "sess-locked")] = true

		startWorker(fastConfig())

		Consistently(store.deriveCount, "60ms").Should(BeZero())

		// Releasing the lock lets the next poll pick it up.
		store.mu.Lock()
		delete(store.locks, key("org-a", "claude-code", "sess-locked"))
		store.mu.Unlock()
		Eventually(store.deriveCount).Should(Equal(1))
	})

	It("keeps a session queued when it is re-dirtied mid-derive", func() {
		store.mark(settled("org-a", "claude-code", "sess-race"))

		rederived := storage.DeriveQueueEntry{
			OrgID:            "org-a",
			HarnessID:        "claude-code",
			HarnessSessionID: "sess-race",
			DirtiedAt:        time.Now().Add(-time.Minute).Add(time.Second),
		}
		var once sync.Once
		store.onDerive = func(_ context.Context) {
			// A raw turn lands while the derive is running: dirtied_at
			// bumps, so the conditional clear must miss.
			once.Do(func() { store.mark(rederived) })
		}

		startWorker(fastConfig())

		// First derive completes but must NOT clear the bumped entry...
		Eventually(store.deriveCount).Should(BeNumerically(">=", 1))
		// ...so a later poll derives it again and only then clears it.
		Eventually(store.deriveCount).Should(BeNumerically(">=", 2))
		Eventually(func() bool {
			_, ok := store.entry("org-a", "claude-code", "sess-race")
			return ok
		}).Should(BeFalse())
	})

	It("retries a session whose derive failed", func() {
		store.mark(settled("org-a", "claude-code", "sess-err"))
		store.mu.Lock()
		store.deriveErr = context.DeadlineExceeded
		store.mu.Unlock()

		startWorker(fastConfig())

		Consistently(store.deriveCount, "60ms").Should(BeZero())
		_, stillQueued := store.entry("org-a", "claude-code", "sess-err")
		Expect(stillQueued).To(BeTrue(), "a failed derive must leave the entry queued")

		store.mu.Lock()
		store.deriveErr = nil
		store.mu.Unlock()
		Eventually(store.deriveCount).Should(Equal(1))
	})

	It("backs off polling while the queue is unreachable, then recovers", func() {
		store.setListErr(errors.New("connection refused"))
		store.mark(settled("org-a", "claude-code", "sess-outage"))

		metrics := worker.NewMetrics()
		cfg := fastConfig()
		cfg.PollInterval = time.Millisecond
		cfg.MaxPollBackoff = 250 * time.Millisecond
		cfg.Metrics = metrics
		startWorker(cfg)

		// Let failures accumulate. With exponential backoff the poll
		// count stays far below the no-backoff rate (~1 per ms here);
		// a generous bound keeps the spec timing-robust.
		time.Sleep(200 * time.Millisecond)
		Expect(store.listCount()).To(BeNumerically("<", 25),
			"polls must back off during an outage, not hot-loop")
		Expect(gaugeValue(metrics.Registry(), "tapes_derive_worker_consecutive_poll_failures")).
			To(BeNumerically(">=", 1), "the outage must be visible as a gauge")

		// The store comes back: the next backed-off poll succeeds, the
		// queued session derives, and the failure gauge resets.
		store.setListErr(nil)
		Eventually(store.deriveCount, "2s").Should(Equal(1))
		Eventually(func() float64 {
			return gaugeValue(metrics.Registry(), "tapes_derive_worker_consecutive_poll_failures")
		}, "2s").Should(BeZero())
	})

	It("publishes queue depth and derive lag gauges each poll", func() {
		// A long debounce keeps the entry queued so the gauges have a
		// stable non-zero queue to report.
		store.mark(storage.DeriveQueueEntry{
			OrgID:            "org-a",
			HarnessID:        "claude-code",
			HarnessSessionID: "sess-gauges",
			DirtiedAt:        time.Now().Add(-time.Minute),
		})

		metrics := worker.NewMetrics()
		cfg := fastConfig()
		cfg.Debounce = time.Hour
		cfg.Metrics = metrics
		startWorker(cfg)

		Eventually(func() float64 {
			return gaugeValue(metrics.Registry(), "tapes_derive_worker_queue_depth")
		}).Should(Equal(1.0))
		Eventually(func() float64 {
			return gaugeValue(metrics.Registry(), "tapes_derive_worker_derive_lag_seconds")
		}).Should(BeNumerically("~", 60, 5), "lag is now minus the oldest dirty mark")
	})

	It("finishes and clears the in-flight derive when canceled mid-derive", func() {
		store.mark(settled("org-a", "claude-code", "sess-drain"))

		started := make(chan struct{})
		finish := make(chan struct{})
		var startOnce sync.Once
		store.onDerive = func(_ context.Context) {
			startOnce.Do(func() { close(started) })
			<-finish
		}

		startWorker(fastConfig())
		Eventually(started).Should(BeClosed())

		// The shutdown signal lands mid-derive. The worker must wait
		// for the derive, not abort it.
		cancel()
		Consistently(done, "50ms").ShouldNot(BeClosed(),
			"the worker must drain the in-flight derive before exiting")

		close(finish)
		Eventually(done).Should(BeClosed())
		Expect(store.deriveCount()).To(Equal(1), "the in-flight derive must complete")
		_, stillQueued := store.entry("org-a", "claude-code", "sess-drain")
		Expect(stillQueued).To(BeFalse(), "the completed derive must still clear its entry")
		Expect(store.lockCount()).To(BeZero(), "the advisory lock must release on shutdown")
	})

	It("aborts a stuck derive at the drain timeout and still releases the lock", func() {
		store.mark(settled("org-a", "claude-code", "sess-stuck"))

		started := make(chan struct{})
		var startOnce sync.Once
		store.onDerive = func(ctx context.Context) {
			startOnce.Do(func() { close(started) })
			// A derive that never finishes on its own: only the drain
			// timeout's context cancellation can end it.
			<-ctx.Done()
		}

		cfg := fastConfig()
		cfg.DrainTimeout = 25 * time.Millisecond
		startWorker(cfg)
		Eventually(started).Should(BeClosed())

		cancel()
		Eventually(done, "1s").Should(BeClosed(),
			"the drain timeout must bound shutdown even with a stuck derive")
		Expect(store.deriveCount()).To(BeZero(), "the aborted derive must not count as completed")
		_, stillQueued := store.entry("org-a", "claude-code", "sess-stuck")
		Expect(stillQueued).To(BeTrue(), "an aborted derive must leave the session queued for the next worker")
		Expect(store.lockCount()).To(BeZero(), "the advisory lock must release even on an aborted derive")
	})

	It("surfaces prune, unknown-kind, and parse-failure signals as counters", func() {
		store.mark(settled("org-a", "claude-code", "sess-signals"))
		store.report = &derive.RederiveReport{
			// 6 raw turns: 3 parsed, 1 raw-only by design, 2 parse
			// failures (the report's sample list is capped, so the
			// counter must come from the arithmetic, not the samples).
			RawTurns:     6,
			ParsedTurns:  3,
			RawOnlyTurns: 1,
			CallKinds:    map[string]int{derive.KindMain: 2, derive.KindUnknown: 2},
			Nodes:        7,
			Upserted:     7,
			Pruned:       1,
		}

		metrics := worker.NewMetrics()
		cfg := fastConfig()
		cfg.Metrics = metrics
		startWorker(cfg)

		Eventually(store.deriveCount).Should(Equal(1))
		reg := metrics.Registry()
		Eventually(func() float64 {
			return counterValue(reg, "tapes_derive_worker_parse_failures_total")
		}).Should(Equal(2.0))
		Expect(counterValue(reg, "tapes_derive_worker_unknown_call_kinds_total")).To(Equal(2.0))
		Expect(counterValue(reg, "tapes_derive_worker_nodes_pruned_total")).To(Equal(1.0),
			"prune>0 on unchanged raw is the projection-bug alert signal")
	})

	It("runs a backstop sweep at startup that feeds the normal path", func() {
		store.rawSessions = []storage.DeriveQueueEntry{
			settled("org-a", "claude-code", "swept-1"),
			settled("org-a", "claude-code", "swept-2"),
		}

		startWorker(fastConfig())

		Eventually(store.sweepCount).Should(Equal(1))
		Eventually(store.deriveCount).Should(Equal(2))
	})

	It("bounds the backstop sweep to recently-active sessions", func() {
		stale := settled("org-a", "claude-code", "swept-stale")
		stale.DirtiedAt = time.Now().Add(-48 * time.Hour)
		store.rawSessions = []storage.DeriveQueueEntry{
			stale,
			settled("org-a", "claude-code", "swept-recent"),
		}

		// fastConfig leaves SweepWindow zero: the 24h default applies.
		startWorker(fastConfig())

		Eventually(store.deriveCount).Should(Equal(1))
		Consistently(store.deriveCount, "50ms").Should(Equal(1),
			"a session idle past the sweep window must not re-enqueue on restart")
	})

	It("sweeps all history when the sweep window is negative", func() {
		stale := settled("org-a", "claude-code", "swept-stale")
		stale.DirtiedAt = time.Now().Add(-48 * time.Hour)
		store.rawSessions = []storage.DeriveQueueEntry{
			stale,
			settled("org-a", "claude-code", "swept-recent"),
		}

		cfg := fastConfig()
		cfg.SweepWindow = -1
		startWorker(cfg)

		Eventually(store.deriveCount).Should(Equal(2),
			"a negative window is the full re-derive escape hatch")
	})
})

package embedworker_test

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/embedworker"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
)

// fakePass records how many times it ran and returns a scripted report
// or error. Safe for the worker goroutine and assertions to share.
type fakePass struct {
	mu     sync.Mutex
	runs   int
	report *spanembed.Report
	err    error
}

func (p *fakePass) Run(context.Context) (*spanembed.Report, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.runs++
	return p.report, p.err
}

func (p *fakePass) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runs
}

func (p *fakePass) set(report *spanembed.Report, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.report, p.err = report, err
}

var _ = Describe("Worker", func() {
	var (
		pass    *fakePass
		metrics *embedworker.Metrics
	)

	BeforeEach(func() {
		pass = &fakePass{report: &spanembed.Report{Embedded: 2, Failed: 0}}
		metrics = embedworker.NewMetrics()
	})

	// startWorker runs the worker until the returned cancel is called,
	// signalling its clean return on the done channel.
	startWorker := func(cfg embedworker.Config) (context.CancelFunc, chan struct{}) {
		cfg.Metrics = metrics
		w := embedworker.NewWorker(cfg, pass, logger.NewNoop())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			Expect(w.Run(ctx)).To(Succeed())
		}()
		return cancel, done
	}

	It("runs a pass immediately at startup", func() {
		cancel, done := startWorker(embedworker.Config{Interval: time.Hour})
		defer func() { cancel(); <-done }()

		Eventually(pass.count).Should(Equal(1))
		// With a long interval it should not run again on its own.
		Consistently(pass.count, "50ms").Should(Equal(1))
	})

	It("runs again on each interval tick", func() {
		cancel, done := startWorker(embedworker.Config{Interval: 10 * time.Millisecond})
		defer func() { cancel(); <-done }()

		Eventually(pass.count).Should(BeNumerically(">=", 3))
	})

	It("absorbs pass failures and keeps running", func() {
		pass.set(nil, errors.New("embedding backend down"))
		cancel, done := startWorker(embedworker.Config{Interval: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
		defer func() { cancel(); <-done }()

		// A failing pass must not crash the worker; it keeps retrying.
		Eventually(pass.count).Should(BeNumerically(">=", 2))
	})

	It("stops cleanly when its context is canceled", func() {
		cancel, done := startWorker(embedworker.Config{Interval: time.Hour})
		Eventually(pass.count).Should(Equal(1))

		cancel()
		Eventually(done).Should(BeClosed())
	})

	Describe("readiness", func() {
		It("reports ready when no ReadyFunc is configured", func() {
			w := embedworker.NewWorker(embedworker.Config{}, pass, logger.NewNoop())
			Expect(w.Ready(context.Background())).To(Succeed())
		})

		It("delegates to the configured ReadyFunc", func() {
			boom := errors.New("db unreachable")
			w := embedworker.NewWorker(embedworker.Config{
				Ready: func(context.Context) error { return boom },
			}, pass, logger.NewNoop())
			Expect(w.Ready(context.Background())).To(MatchError(boom))
		})
	})
})

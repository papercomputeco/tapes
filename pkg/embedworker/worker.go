// Package embedworker is the production span-embedding loop: it runs
// the bounded span-embedding pass (see pkg/spanembed) on its own
// interval, in its own process (`tapes serve embed-worker`) — split out
// of the derive worker so that embedding NEVER blocks derivation.
//
// Why a separate process: embedding calls an external provider (OpenAI,
// Ollama) whose latency and availability are outside Tapes' control. As
// an inline step of the derive loop it could not fail or hang without
// stalling the projection of raw turns into the read model. Here it has
// its own memory budget, its own failure domain, and its own replica
// count; derivation is untouched whatever the embedding backend does.
//
// The pass is idempotent and keyed by span identity, so running it on a
// timer — and running multiple replicas — only costs redundant reads. A
// per-span embed failure (the provider rejected the input or was
// unreachable for that span) is counted and logged but never aborts the
// pass; the span stays un-embedded and the next run retries it.
package embedworker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/papercomputeco/tapes/pkg/spanembed"
)

// Defaults for Config fields left zero.
const (
	DefaultInterval     = time.Minute
	DefaultMaxBackoff   = 5 * time.Minute
	DefaultDrainTimeout = 30 * time.Second
)

// Pass runs one bounded span-embedding pass and reports what it did.
// *spanembed.Pass satisfies it; tests substitute a fake.
type Pass interface {
	Run(ctx context.Context) (*spanembed.Report, error)
}

// ReadyFunc reports whether the worker's dependencies are reachable —
// the /readyz signal. Typically a database ping. Nil means always
// ready (the process is up).
type ReadyFunc func(ctx context.Context) error

// Config tunes the worker loop. Zero fields take the package defaults.
type Config struct {
	// Interval is how often a full embed pass runs. A pass also runs
	// once immediately at startup to clear the standing backlog.
	Interval time.Duration

	// MaxBackoff caps the jittered exponential backoff applied between
	// passes while the pass keeps failing on infrastructure (database or
	// embedding backend unreachable), so an outage costs one line per
	// retry at a widening cadence instead of hammering a dead backend
	// every Interval.
	MaxBackoff time.Duration

	// DrainTimeout bounds graceful shutdown: after the run context is
	// canceled, an in-flight pass may keep running this long before its
	// store/backend calls are aborted.
	DrainTimeout time.Duration

	// Ready optionally backs the readiness probe. Nil reports ready as
	// soon as the worker exists.
	Ready ReadyFunc

	// Metrics optionally injects a pre-built metrics surface so the
	// hosting command can mount /metrics before the store is reachable.
	// NewWorker builds a fresh one when nil.
	Metrics *Metrics
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = DefaultMaxBackoff
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	return c
}

// Worker owns the startup-pass / interval / drain loop.
type Worker struct {
	cfg     Config
	pass    Pass
	logger  *slog.Logger
	metrics *Metrics

	// Consecutive infrastructure-failure bookkeeping for backoff and the
	// single-line outage/recovery logs. Only the Run goroutine touches
	// these.
	consecutiveFailures int
	firstFailureAt      time.Time
}

// NewWorker creates an embed worker. logger must be non-nil.
func NewWorker(cfg Config, pass Pass, logger *slog.Logger) *Worker {
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics()
	}
	return &Worker{
		cfg:     cfg.withDefaults(),
		pass:    pass,
		logger:  logger,
		metrics: metrics,
	}
}

// Ready reports whether the worker can do useful work right now. With no
// ReadyFunc configured it reports ready once the worker exists.
func (w *Worker) Ready(ctx context.Context) error {
	if w.cfg.Ready == nil {
		return nil
	}
	return w.cfg.Ready(ctx)
}

// Metrics exposes the worker's Prometheus surface so the hosting command
// can mount a scrape endpoint.
func (w *Worker) Metrics() *Metrics { return w.metrics }

// Run blocks until ctx is canceled. One pass runs immediately at startup
// (clearing the standing backlog), then the interval timer takes over.
//
// Shutdown is graceful: canceling ctx stops scheduling new passes, but
// an in-flight pass keeps running on a detached context for up to
// DrainTimeout before its calls are aborted. Run then returns nil.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("embed worker starting",
		"interval", w.cfg.Interval,
		"drain_timeout", w.cfg.DrainTimeout,
	)

	// workCtx carries the pass's store/backend calls. It survives ctx
	// cancellation by up to DrainTimeout so an in-flight pass can drain.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	go func() {
		select {
		case <-workCtx.Done():
			// Run returned on its own; nothing to drain.
		case <-ctx.Done():
			w.logger.Info("embed worker draining", "drain_timeout", w.cfg.DrainTimeout)
			drain := time.NewTimer(w.cfg.DrainTimeout)
			defer drain.Stop()
			select {
			case <-workCtx.Done():
			case <-drain.C:
				w.logger.Warn("embed worker drain timeout, aborting in-flight pass")
				cancelWork()
			}
		}
	}()

	// A timer (not a ticker) so the interval can stretch into a backoff
	// while the backend is unreachable instead of hot-looping the failure.
	w.runPass(ctx, workCtx)
	timer := time.NewTimer(w.nextDelay())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("embed worker stopping")
			return nil
		case <-timer.C:
			w.runPass(ctx, workCtx)
			timer.Reset(w.nextDelay())
		}
	}
}

// runPass runs one embed pass, records its outcome, and absorbs every
// error: an unreachable backend must never crash the worker, and the
// pass's idempotence means the next run simply retries what stayed
// un-embedded.
func (w *Worker) runPass(ctx, workCtx context.Context) {
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	report, err := w.pass.Run(workCtx)
	if workCtx.Err() != nil {
		// Aborted by shutdown drain; not a real failure.
		return
	}
	if err != nil {
		w.passFailed(err)
		return
	}
	w.passSucceeded(report, time.Since(start))
}

// passSucceeded records a clean pass and closes out any outage window.
func (w *Worker) passSucceeded(report *spanembed.Report, dur time.Duration) {
	w.metrics.Passes.WithLabelValues(resultOK).Inc()
	w.metrics.PassDuration.Observe(dur.Seconds())
	w.metrics.LastSuccessTimestamp.Set(float64(time.Now().Unix()))
	if report != nil {
		w.metrics.SpansScanned.Add(float64(report.Scanned))
		w.metrics.SpansEmbedded.Add(float64(report.Embedded))
		w.metrics.SpansUpToDate.Add(float64(report.UpToDate))
		w.metrics.SpansEmpty.Add(float64(report.Empty))
		w.metrics.SpansPoisoned.Add(float64(report.Poisoned))
		w.metrics.SpansFailed.Add(float64(report.Failed))
		w.metrics.SpansChunked.Add(float64(report.Chunked))
		w.metrics.ChunkRows.Add(float64(report.ChunkRows))
		w.metrics.Oversize.Add(float64(report.Oversized))
		w.metrics.OrphansPruned.Add(float64(report.Pruned))
		for _, tokens := range report.OversizeTokens {
			w.metrics.OversizeTokens.Observe(float64(tokens))
		}
		for reason, n := range report.FailuresByReason {
			w.metrics.SpanFailures.WithLabelValues(reason).Add(float64(n))
		}
	}
	if w.consecutiveFailures > 0 {
		w.logger.Info("embed worker recovered",
			"failures", w.consecutiveFailures,
			"outage", time.Since(w.firstFailureAt).Round(time.Millisecond),
		)
		w.consecutiveFailures = 0
		w.metrics.ConsecutiveFailures.Set(0)
	}
}

// passFailed records one consecutive infrastructure failure. One WARN
// line per failure, error text inline, never a stack — an outage reads
// as a counter ticking up, not a wall of spam.
func (w *Worker) passFailed(err error) {
	if w.consecutiveFailures == 0 {
		w.firstFailureAt = time.Now()
	}
	w.consecutiveFailures++
	w.metrics.Passes.WithLabelValues(resultError).Inc()
	w.metrics.ConsecutiveFailures.Set(float64(w.consecutiveFailures))
	w.logger.Warn("embed worker pass failed",
		"consecutive_failures", w.consecutiveFailures,
		"retry_in", w.nextDelay().Round(time.Millisecond),
		"error", err.Error(),
	)
}

// nextDelay is the wait before the next pass: the plain interval when
// healthy, or a jittered, exponentially-backed-off delay (capped at
// MaxBackoff) while passes keep failing on infrastructure.
func (w *Worker) nextDelay() time.Duration {
	if w.consecutiveFailures == 0 {
		return w.cfg.Interval
	}
	return backoffDelay(w.cfg.Interval, w.cfg.MaxBackoff, w.consecutiveFailures)
}

// backoffDelay computes the delay before retry number failures+1: base
// doubled per consecutive failure, capped, then jittered to 50–100% so
// restarted replicas don't re-pass in lockstep.
func backoffDelay(base, maxDelay time.Duration, failures int) time.Duration {
	delay := base
	for i := 1; i < failures && delay < maxDelay; i++ {
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	half := delay / 2
	return half + rand.N(half+1) //nolint:gosec // retry jitter, not cryptographic material
}

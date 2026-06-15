// Package worker is the production derive loop: it polls the dirty-
// session queue ingest feeds (see storage.DeriveQueue), debounces
// bursts, and re-derives one session at a time under a per-session
// advisory lock. It runs as its own process (`tapes serve
// derive-worker`) — NEVER inside the API container: a full derive once
// OOMKilled a 256Mi API pod, so derivation gets its own memory budget
// and processes exactly one session at a time.
//
// Everything is at-least-once and leans on the deriver's idempotence
// (re-running an unchanged session prunes 0): a lost clear, a crashed
// derive, or a duplicate mark only costs a redundant derive.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Defaults for Config fields left zero.
const (
	DefaultPollInterval   = 5 * time.Second
	DefaultDebounce       = 20 * time.Second
	DefaultMaxDeriveLag   = 45 * time.Second
	DefaultSweepInterval  = time.Hour
	DefaultPageSize       = 50
	DefaultMaxPollBackoff = 30 * time.Second
	DefaultDrainTimeout   = 30 * time.Second
	DefaultSweepWindow    = 24 * time.Hour
)

// Store is the storage capability surface the worker drives. The
// Postgres driver satisfies it; tests substitute a fake.
type Store interface {
	// ListDeriveDirty returns dirty sessions that have settled OR
	// waited past the max-lag bound, oldest first.
	ListDeriveDirty(ctx context.Context, dirtiedBefore, firstDirtiedBefore time.Time, limit int32) ([]storage.DeriveQueueEntry, error)

	// GetDeriveDirty re-reads one queue entry (nil when clean).
	GetDeriveDirty(ctx context.Context, orgID, harnessID, harnessSessionID string) (*storage.DeriveQueueEntry, error)

	// ClearDeriveDirty removes the entry only if DirtiedAt is unchanged.
	ClearDeriveDirty(ctx context.Context, e storage.DeriveQueueEntry) (bool, error)

	// SweepDeriveDirty enqueues every raw-layer session active at or
	// after activeSince (zero time = everything).
	SweepDeriveDirty(ctx context.Context, activeSince time.Time) (int64, error)

	// DeriveQueueStats reports queue depth and the oldest dirty mark
	// (the worker's depth/lag gauges and readiness probe).
	DeriveQueueStats(ctx context.Context) (storage.DeriveQueueStats, error)

	// TryDeriveSessionLock takes the per-session advisory lock.
	// acquired=false (nil error) means another worker holds it.
	TryDeriveSessionLock(ctx context.Context, orgID, harnessID, harnessSessionID string) (release func(), acquired bool, err error)

	// RederiveSession re-derives and persists one harness session.
	RederiveSession(ctx context.Context, project, orgID, harnessID, harnessSessionID string) (*derive.RederiveReport, error)
}

// SpanEmbedRunner runs one bounded span-embedding pass (see
// pkg/spanembed). The worker treats it as opaque: failures are logged
// and retried on the next trigger, never fed into the poll backoff.
type SpanEmbedRunner interface {
	Run(ctx context.Context) error
}

// SpanEmbedFunc adapts a function to SpanEmbedRunner.
type SpanEmbedFunc func(ctx context.Context) error

// Run implements SpanEmbedRunner.
func (f SpanEmbedFunc) Run(ctx context.Context) error { return f(ctx) }

// Config tunes the worker loop. Zero fields take the package defaults.
type Config struct {
	// Project mirrors the ingest worker's configured project tag; it
	// does not participate in node hashes.
	Project string

	// PollInterval is how often the dirty queue is polled.
	PollInterval time.Duration

	// Debounce is how long a session's dirty mark must be quiet before
	// it derives — ingest bursts settle into one derive.
	Debounce time.Duration

	// SweepInterval is the slow backstop cadence: enqueue every session
	// present in the raw layer, catching lost marks. A sweep also runs
	// once at startup.
	SweepInterval time.Duration

	// MaxDeriveLag bounds debounce starvation: a continuously
	// streaming session re-marks on every capture and never settles,
	// so a mark that has waited this long derives anyway. Live views
	// see bounded lag instead of waiting for a quiet gap.
	MaxDeriveLag time.Duration

	// SweepWindow bounds the backstop sweep to sessions with raw
	// activity in the last window, so a worker restart in a large org
	// re-enqueues recent sessions instead of stampeding the queue with
	// all of history (default 24h). Negative disables the bound and
	// sweeps every session ever captured — the escape hatch for a full
	// re-derive after a deriver fix.
	SweepWindow time.Duration

	// PageSize bounds one poll's batch. Sessions are still derived
	// strictly one at a time.
	PageSize int32

	// MaxPollBackoff caps the jittered exponential backoff applied
	// between polls while the queue is unreachable (DB outage). The
	// first failure retries after roughly PollInterval; each further
	// consecutive failure doubles the delay up to this cap.
	MaxPollBackoff time.Duration

	// DrainTimeout bounds the graceful-shutdown window: after the run
	// context is canceled, the in-flight derive may keep running this
	// long before its store operations are aborted. Locks release
	// either way.
	DrainTimeout time.Duration

	// Metrics optionally injects a pre-built metrics surface so the
	// hosting command can mount /metrics (and serve health probes)
	// before the store is even reachable — e.g. while --wait-for-db
	// retries. NewWorker builds a fresh one when nil.
	Metrics *Metrics

	// SpanEmbed optionally embeds spans after derives (nil disables —
	// the default). The pass runs once at startup (catching the
	// backlog from before embedding was enabled) and after every poll
	// cycle that derived at least one session. It is idempotent and
	// keyed by span identity, so an extra run — or a concurrent run by
	// another replica — only costs redundant reads.
	SpanEmbed SpanEmbedRunner
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.Debounce <= 0 {
		c.Debounce = DefaultDebounce
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = DefaultSweepInterval
	}
	if c.SweepWindow == 0 {
		c.SweepWindow = DefaultSweepWindow
	}
	if c.PageSize <= 0 {
		c.PageSize = DefaultPageSize
	}
	if c.MaxPollBackoff <= 0 {
		c.MaxPollBackoff = DefaultMaxPollBackoff
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	return c
}

// Worker owns the poll/debounce/lock/derive/clear loop.
type Worker struct {
	cfg     Config
	store   Store
	logger  *slog.Logger
	metrics *Metrics

	// Consecutive poll-failure bookkeeping for backoff and the
	// single-line outage/recovery logs. Only the Run goroutine touches
	// these.
	consecutiveFailures int
	firstFailureAt      time.Time
}

// NewWorker creates a derive worker. logger must be non-nil.
func NewWorker(cfg Config, store Store, logger *slog.Logger) *Worker {
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics()
	}
	return &Worker{
		cfg:     cfg.withDefaults(),
		store:   store,
		logger:  logger,
		metrics: metrics,
	}
}

// Ready reports whether the worker can serve its purpose right now:
// the store answers and the dirty queue is pollable. This is the
// /readyz signal — it intentionally exercises the same query the poll
// loop depends on rather than a bare connection ping.
func (w *Worker) Ready(ctx context.Context) error {
	_, err := w.store.DeriveQueueStats(ctx)
	return err
}

// Metrics exposes the worker's Prometheus surface so the hosting
// command can mount a scrape endpoint.
func (w *Worker) Metrics() *Metrics { return w.metrics }

// Run blocks until ctx is canceled. One sweep runs immediately at
// startup (catching anything queued — or never queued — before this
// worker existed), then the poll and sweep tickers take over.
//
// Shutdown is graceful: canceling ctx stops polling, but the in-flight
// derive keeps running on a detached context for up to DrainTimeout —
// finishing and clearing rather than aborting mid-write. A derive
// still running at the deadline is aborted; either way the advisory
// lock releases (release runs on a fresh background context) and Run
// returns nil.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("derive worker starting",
		"poll_interval", w.cfg.PollInterval,
		"debounce", w.cfg.Debounce,
		"sweep_interval", w.cfg.SweepInterval,
		"drain_timeout", w.cfg.DrainTimeout,
	)

	// workCtx carries store operations. It survives ctx cancellation by
	// up to DrainTimeout so the in-flight derive can drain.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	go func() {
		select {
		case <-workCtx.Done():
			// Run returned on its own; nothing to drain.
		case <-ctx.Done():
			w.logger.Info("derive worker draining", "drain_timeout", w.cfg.DrainTimeout)
			drain := time.NewTimer(w.cfg.DrainTimeout)
			defer drain.Stop()
			select {
			case <-workCtx.Done():
			case <-drain.C:
				w.logger.Warn("derive worker drain timeout, aborting in-flight work")
				cancelWork()
			}
		}
	}()

	w.runSweep(ctx)
	// The startup embed pass picks up the backlog: spans derived
	// before embedding was enabled (or while this worker was down)
	// would otherwise wait for their session's next derive.
	w.runEmbed(ctx, workCtx)

	// Polling uses a timer, not a ticker, so the interval can stretch
	// into a backoff while the store is unreachable instead of hot-
	// looping the failure every PollInterval.
	poll := time.NewTimer(w.cfg.PollInterval)
	defer poll.Stop()
	sweep := time.NewTicker(w.cfg.SweepInterval)
	defer sweep.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("derive worker stopping")
			return nil
		case <-poll.C:
			derived, err := w.runPoll(ctx, workCtx)
			switch {
			case ctx.Err() != nil:
				// Shutting down; the next select arm exits.
				poll.Reset(w.cfg.PollInterval)
			case err != nil:
				poll.Reset(w.pollFailed(err))
			default:
				w.pollRecovered()
				if derived > 0 {
					w.runEmbed(ctx, workCtx)
				}
				poll.Reset(w.cfg.PollInterval)
			}
		case <-sweep.C:
			w.runSweep(ctx)
		}
	}
}

// pollFailed records one consecutive poll failure and returns the
// jittered, exponentially-backed-off delay until the next poll. One
// WARN line per failure — the error text inline, never a stack — so an
// outage reads as a counter ticking up, not a wall of spam.
func (w *Worker) pollFailed(err error) time.Duration {
	if w.consecutiveFailures == 0 {
		w.firstFailureAt = time.Now()
	}
	w.consecutiveFailures++
	w.metrics.PollErrors.Inc()
	w.metrics.ConsecutiveFailures.Set(float64(w.consecutiveFailures))

	delay := backoffDelay(w.cfg.PollInterval, w.cfg.MaxPollBackoff, w.consecutiveFailures)
	w.logger.Warn("derive worker poll failed",
		"consecutive_failures", w.consecutiveFailures,
		"retry_in", delay.Round(time.Millisecond),
		"error", err.Error(),
	)
	return delay
}

// pollRecovered closes out an outage window with a single INFO line.
func (w *Worker) pollRecovered() {
	if w.consecutiveFailures == 0 {
		return
	}
	w.logger.Info("derive worker reconnected",
		"failures", w.consecutiveFailures,
		"outage", time.Since(w.firstFailureAt).Round(time.Millisecond),
	)
	w.consecutiveFailures = 0
	w.metrics.ConsecutiveFailures.Set(0)
}

// backoffDelay computes the delay before retry number failures+1:
// base doubled per consecutive failure, capped, then jittered to
// 50–100% so restarted replicas don't repoll in lockstep.
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

// runPoll drains one page of settled dirty sessions, one at a time,
// and reports how many sessions it derived. The returned error is an
// infrastructure failure (queue unreachable); it aborts the rest of
// the page so an outage costs one line + backoff instead of one error
// per queued session.
//
// ctx is the run (shutdown) context, checked between sessions to stop
// taking new work; workCtx carries the store operations so an in-
// flight derive survives shutdown into the drain window.
func (w *Worker) runPoll(ctx, workCtx context.Context) (int, error) {
	if ctx.Err() != nil {
		return 0, nil //nolint:nilerr // shutdown is not a poll failure
	}
	stats, err := w.store.DeriveQueueStats(workCtx)
	if err != nil {
		return 0, err
	}
	w.metrics.QueueDepth.Set(float64(stats.Depth))
	lag := 0.0
	if stats.Depth > 0 && !stats.OldestDirtiedAt.IsZero() {
		lag = time.Since(stats.OldestDirtiedAt).Seconds()
	}
	w.metrics.DeriveLag.Set(lag)

	cutoff := time.Now().Add(-w.cfg.Debounce)
	lagCutoff := time.Now().Add(-w.maxDeriveLag())
	entries, err := w.store.ListDeriveDirty(workCtx, cutoff, lagCutoff, w.cfg.PageSize)
	if err != nil {
		return 0, err
	}
	derived := 0
	for _, e := range entries {
		if ctx.Err() != nil {
			return derived, nil //nolint:nilerr // shutdown is not a poll failure
		}
		ok, err := w.processEntry(workCtx, e, cutoff, lagCutoff)
		if err != nil {
			return derived, err
		}
		if ok {
			derived++
		}
	}
	return derived, nil
}

// processEntry derives one dirty session under its advisory lock.
//
// Clear semantics: the queue row is re-read UNDER the lock and the
// clear is conditional on that read's dirtied_at — a raw turn landing
// mid-derive bumps dirtied_at, the clear matches nothing, and the
// session is re-derived on a later poll. At-least-once, never lost.
//
// The returned error is an infrastructure failure (lock, queue
// re-read, or clear — all pure store plumbing): it aborts the page and
// feeds the poll backoff. A derive failure stays per-session — it may
// be specific to this session's data, so it must not stall the rest of
// the queue — and returns nil.
// maxDeriveLag resolves the configured bound, defaulting when unset.
func (w *Worker) maxDeriveLag() time.Duration {
	if w.cfg.MaxDeriveLag > 0 {
		return w.cfg.MaxDeriveLag
	}
	return DefaultMaxDeriveLag
}

func (w *Worker) processEntry(ctx context.Context, e storage.DeriveQueueEntry, cutoff, lagCutoff time.Time) (bool, error) {
	release, acquired, err := w.store.TryDeriveSessionLock(ctx, e.OrgID, e.HarnessID, e.HarnessSessionID)
	if err != nil {
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return false, fmt.Errorf("derive lock %s/%s/%s: %w", e.OrgID, e.HarnessID, e.HarnessSessionID, err)
	}
	if !acquired {
		// Another worker owns this session right now.
		w.metrics.Derives.WithLabelValues(resultLocked).Inc()
		return false, nil
	}
	defer release()

	// Re-read under the lock: a concurrent worker may have already
	// derived and cleared this session between our list and our lock.
	cur, err := w.store.GetDeriveDirty(ctx, e.OrgID, e.HarnessID, e.HarnessSessionID)
	if err != nil {
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return false, fmt.Errorf("derive queue re-read %s/%s/%s: %w", e.OrgID, e.HarnessID, e.HarnessSessionID, err)
	}
	// Derive when the session has SETTLED (no raw turn since the debounce
	// window) OR when it is LAG-BOUNDED (its first mark has waited past
	// the max-lag bound). A continuously-streaming session bumps
	// dirtied_at on every capture and never settles, so without the
	// first-mark bound it would be listed-then-skipped forever — the lag
	// bound is what forces it to derive. We skip only when it is neither:
	// cleared already, or still inside the debounce window with a
	// first-mark that has not yet aged out.
	settled := cur != nil && !cur.DirtiedAt.After(cutoff)
	lagBounded := cur != nil && !cur.FirstDirtiedAt.IsZero() && !cur.FirstDirtiedAt.After(lagCutoff)
	if cur == nil || (!settled && !lagBounded) {
		w.metrics.Derives.WithLabelValues(resultSkipped).Inc()
		return false, nil
	}

	start := time.Now()
	report, err := w.store.RederiveSession(ctx, w.cfg.Project, cur.OrgID, cur.HarnessID, cur.HarnessSessionID)
	if err != nil {
		// Leave the entry queued: the next poll retries it.
		w.logger.Error("derive session", "error", err,
			"org", cur.OrgID, "harness", cur.HarnessID, "session", cur.HarnessSessionID)
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return false, nil
	}

	cleared, err := w.store.ClearDeriveDirty(ctx, *cur)
	if err != nil {
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return false, fmt.Errorf("derive queue clear %s/%s/%s: %w", cur.OrgID, cur.HarnessID, cur.HarnessSessionID, err)
	}

	duration := time.Since(start)
	w.metrics.Derives.WithLabelValues(resultOK).Inc()
	w.metrics.NodesUpserted.Add(float64(report.Upserted))
	w.metrics.NodesPruned.Add(float64(report.Pruned))
	w.metrics.DeriveDuration.Observe(duration.Seconds())
	w.metrics.UnknownCalls.Add(float64(report.CallKinds[derive.KindUnknown]))
	// ParseFailures samples are capped in the report; the exact count
	// is everything that neither parsed nor was raw-only by design.
	w.metrics.ParseFailures.Add(float64(report.RawTurns - report.ParsedTurns - report.RawOnlyTurns))
	if !cleared {
		// Re-dirtied while we derived; the session stays queued.
		w.metrics.Requeued.Inc()
	}

	w.logger.Info("derived session",
		"org", cur.OrgID,
		"harness", cur.HarnessID,
		"session", cur.HarnessSessionID,
		"raw_turns", report.RawTurns,
		"nodes", report.Nodes,
		"upserted", report.Upserted,
		"pruned", report.Pruned,
		"duration", duration,
		"requeued", !cleared,
	)
	return true, nil
}

// runSweep enqueues every recently-active session present in the raw
// layer (bounded by SweepWindow; negative window sweeps everything).
// The sweep never writes nodes itself — every derive funnels through
// the locked per-session path, so a sweep can never race a session
// derive's prune.
func (w *Worker) runSweep(ctx context.Context) {
	var activeSince time.Time
	if w.cfg.SweepWindow > 0 {
		activeSince = time.Now().Add(-w.cfg.SweepWindow)
	}
	enqueued, err := w.store.SweepDeriveDirty(ctx, activeSince)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Error("derive sweep", "error", err)
		w.metrics.Sweeps.WithLabelValues(resultError).Inc()
		return
	}
	w.metrics.Sweeps.WithLabelValues(resultOK).Inc()
	w.metrics.SweepEnqueued.Add(float64(enqueued))
	w.logger.Info("derive sweep", "enqueued", enqueued)
}

// runEmbed runs the optional span-embedding pass. Failures are logged
// and absorbed: an unreachable embedding backend must never stall
// derivation, and the pass's idempotence means the next trigger
// simply retries the spans that stayed un-embedded.
//
// ctx is the run (shutdown) context — once shutdown begins, no new
// embed work starts; workCtx carries the actual store/backend calls.
func (w *Worker) runEmbed(ctx, workCtx context.Context) {
	if w.cfg.SpanEmbed == nil || ctx.Err() != nil {
		return
	}
	if err := w.cfg.SpanEmbed.Run(workCtx); err != nil && workCtx.Err() == nil {
		w.logger.Warn("span embed pass", "error", err.Error())
	}
}

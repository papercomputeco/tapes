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
	"log/slog"
	"time"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Defaults for Config fields left zero.
const (
	DefaultPollInterval  = 5 * time.Second
	DefaultDebounce      = 20 * time.Second
	DefaultSweepInterval = time.Hour
	DefaultPageSize      = 50
)

// Store is the storage capability surface the worker drives. The
// Postgres driver satisfies it; tests substitute a fake.
type Store interface {
	// ListDeriveDirty returns settled dirty sessions, oldest first.
	ListDeriveDirty(ctx context.Context, dirtiedBefore time.Time, limit int32) ([]storage.DeriveQueueEntry, error)

	// GetDeriveDirty re-reads one queue entry (nil when clean).
	GetDeriveDirty(ctx context.Context, orgID, harnessID, harnessSessionID string) (*storage.DeriveQueueEntry, error)

	// ClearDeriveDirty removes the entry only if DirtiedAt is unchanged.
	ClearDeriveDirty(ctx context.Context, e storage.DeriveQueueEntry) (bool, error)

	// SweepDeriveDirty enqueues every session in the raw layer.
	SweepDeriveDirty(ctx context.Context) (int64, error)

	// TryDeriveSessionLock takes the per-session advisory lock.
	// acquired=false (nil error) means another worker holds it.
	TryDeriveSessionLock(ctx context.Context, orgID, harnessID, harnessSessionID string) (release func(), acquired bool, err error)

	// RederiveSession re-derives and persists one harness session.
	RederiveSession(ctx context.Context, project, orgID, harnessID, harnessSessionID string) (*derive.RederiveReport, error)
}

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

	// PageSize bounds one poll's batch. Sessions are still derived
	// strictly one at a time.
	PageSize int32
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
	if c.PageSize <= 0 {
		c.PageSize = DefaultPageSize
	}
	return c
}

// Worker owns the poll/debounce/lock/derive/clear loop.
type Worker struct {
	cfg     Config
	store   Store
	logger  *slog.Logger
	metrics *Metrics
}

// NewWorker creates a derive worker. logger must be non-nil.
func NewWorker(cfg Config, store Store, logger *slog.Logger) *Worker {
	return &Worker{
		cfg:     cfg.withDefaults(),
		store:   store,
		logger:  logger,
		metrics: NewMetrics(),
	}
}

// Metrics exposes the worker's Prometheus surface so the hosting
// command can mount a scrape endpoint.
func (w *Worker) Metrics() *Metrics { return w.metrics }

// Run blocks until ctx is canceled. One sweep runs immediately at
// startup (catching anything queued — or never queued — before this
// worker existed), then the poll and sweep tickers take over.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("derive worker starting",
		"poll_interval", w.cfg.PollInterval,
		"debounce", w.cfg.Debounce,
		"sweep_interval", w.cfg.SweepInterval,
	)

	w.runSweep(ctx)

	poll := time.NewTicker(w.cfg.PollInterval)
	defer poll.Stop()
	sweep := time.NewTicker(w.cfg.SweepInterval)
	defer sweep.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("derive worker stopping")
			return nil
		case <-poll.C:
			w.runPoll(ctx)
		case <-sweep.C:
			w.runSweep(ctx)
		}
	}
}

// runPoll drains one page of settled dirty sessions, one at a time.
func (w *Worker) runPoll(ctx context.Context) {
	cutoff := time.Now().Add(-w.cfg.Debounce)
	entries, err := w.store.ListDeriveDirty(ctx, cutoff, w.cfg.PageSize)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Error("derive worker poll", "error", err)
		w.metrics.PollErrors.Inc()
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		w.processEntry(ctx, e, cutoff)
	}
}

// processEntry derives one dirty session under its advisory lock.
//
// Clear semantics: the queue row is re-read UNDER the lock and the
// clear is conditional on that read's dirtied_at — a raw turn landing
// mid-derive bumps dirtied_at, the clear matches nothing, and the
// session is re-derived on a later poll. At-least-once, never lost.
func (w *Worker) processEntry(ctx context.Context, e storage.DeriveQueueEntry, cutoff time.Time) {
	release, acquired, err := w.store.TryDeriveSessionLock(ctx, e.OrgID, e.HarnessID, e.HarnessSessionID)
	if err != nil {
		w.logger.Error("derive lock", "error", err,
			"org", e.OrgID, "harness", e.HarnessID, "session", e.HarnessSessionID)
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return
	}
	if !acquired {
		// Another worker owns this session right now.
		w.metrics.Derives.WithLabelValues(resultLocked).Inc()
		return
	}
	defer release()

	// Re-read under the lock: a concurrent worker may have already
	// derived and cleared this session between our list and our lock.
	cur, err := w.store.GetDeriveDirty(ctx, e.OrgID, e.HarnessID, e.HarnessSessionID)
	if err != nil {
		w.logger.Error("derive queue re-read", "error", err,
			"org", e.OrgID, "harness", e.HarnessID, "session", e.HarnessSessionID)
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return
	}
	if cur == nil || cur.DirtiedAt.After(cutoff) {
		// Cleared by someone else, or re-dirtied inside the debounce
		// window — either way it is not ours to derive this cycle.
		w.metrics.Derives.WithLabelValues(resultSkipped).Inc()
		return
	}

	start := time.Now()
	report, err := w.store.RederiveSession(ctx, w.cfg.Project, cur.OrgID, cur.HarnessID, cur.HarnessSessionID)
	if err != nil {
		// Leave the entry queued: the next poll retries it.
		w.logger.Error("derive session", "error", err,
			"org", cur.OrgID, "harness", cur.HarnessID, "session", cur.HarnessSessionID)
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return
	}

	cleared, err := w.store.ClearDeriveDirty(ctx, *cur)
	if err != nil {
		w.logger.Error("derive queue clear", "error", err,
			"org", cur.OrgID, "harness", cur.HarnessID, "session", cur.HarnessSessionID)
		w.metrics.Derives.WithLabelValues(resultError).Inc()
		return
	}

	duration := time.Since(start)
	w.metrics.Derives.WithLabelValues(resultOK).Inc()
	w.metrics.NodesUpserted.Add(float64(report.Upserted))
	w.metrics.NodesPruned.Add(float64(report.Pruned))
	w.metrics.DeriveDuration.Observe(duration.Seconds())
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
}

// runSweep enqueues every session present in the raw layer. The sweep
// never writes nodes itself — every derive funnels through the locked
// per-session path, so a sweep can never race a session derive's
// prune.
func (w *Worker) runSweep(ctx context.Context) {
	enqueued, err := w.store.SweepDeriveDirty(ctx)
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

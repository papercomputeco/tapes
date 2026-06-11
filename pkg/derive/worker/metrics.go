package worker

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Derive result labels. Mirrors the API server's pattern of a small,
// fixed label set so cardinality stays bounded.
const (
	resultOK      = "ok"
	resultError   = "error"
	resultLocked  = "locked"
	resultSkipped = "skipped"
)

// Metrics is the Prometheus surface for the derive worker. Each worker
// owns its own registry (same rationale as api.Metrics: tests scrape in
// isolation; the hosting command mounts Handler on its listener).
type Metrics struct {
	registry *prometheus.Registry

	// Derives counts per-session derive attempts by outcome:
	// ok / error / locked (another worker holds the session) /
	// skipped (cleared by a peer or re-dirtied inside the debounce).
	Derives *prometheus.CounterVec

	// Requeued counts successful derives whose conditional clear
	// matched nothing — the session was re-dirtied mid-derive and
	// stays queued.
	Requeued prometheus.Counter

	NodesUpserted  prometheus.Counter
	NodesPruned    prometheus.Counter
	DeriveDuration prometheus.Histogram

	Sweeps        *prometheus.CounterVec
	SweepEnqueued prometheus.Counter

	PollErrors prometheus.Counter

	// ConsecutiveFailures mirrors the worker's in-memory outage
	// counter: non-zero means the queue is currently unreachable and
	// polls are backing off. Alert on sustained non-zero.
	ConsecutiveFailures prometheus.Gauge

	// QueueDepth and DeriveLag are refreshed once per successful poll:
	// how many sessions are dirty, and how stale the oldest dirty mark
	// is (now - oldest dirtied_at, seconds; 0 when the queue is empty).
	QueueDepth prometheus.Gauge
	DeriveLag  prometheus.Gauge
}

// NewMetrics constructs the derive worker's counters on a fresh
// registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		Derives: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_derive_worker_derives_total",
				Help: "Per-session derive attempts by the derive worker, by outcome.",
			},
			[]string{"result"},
		),
		Requeued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_derive_worker_requeued_total",
			Help: "Derives whose session was re-dirtied mid-derive and stayed queued.",
		}),
		NodesUpserted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_derive_worker_nodes_upserted_total",
			Help: "Derived nodes upserted by the derive worker.",
		}),
		NodesPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_derive_worker_nodes_pruned_total",
			Help: "Stale derived nodes pruned by the derive worker (0 is the idempotence invariant).",
		}),
		DeriveDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tapes_derive_worker_derive_duration_seconds",
			Help:    "Wall time of one session derive (read + derive + persist).",
			Buckets: prometheus.DefBuckets,
		}),
		Sweeps: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_derive_worker_sweeps_total",
				Help: "Backstop sweeps run by the derive worker, by outcome.",
			},
			[]string{"result"},
		),
		SweepEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_derive_worker_sweep_enqueued_total",
			Help: "Sessions newly enqueued by backstop sweeps.",
		}),
		PollErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_derive_worker_poll_errors_total",
			Help: "Dirty-queue poll failures.",
		}),
		ConsecutiveFailures: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_derive_worker_consecutive_poll_failures",
			Help: "Consecutive dirty-queue poll failures; non-zero means the store is unreachable and polls are backing off.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_derive_worker_queue_depth",
			Help: "Dirty (queued) sessions awaiting derivation, sampled each poll.",
		}),
		DeriveLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_derive_worker_derive_lag_seconds",
			Help: "Age of the oldest dirty mark still queued (now - oldest dirtied_at); 0 when the queue is empty.",
		}),
	}
	reg.MustRegister(
		m.Derives, m.Requeued,
		m.NodesUpserted, m.NodesPruned, m.DeriveDuration,
		m.Sweeps, m.SweepEnqueued, m.PollErrors,
		m.ConsecutiveFailures, m.QueueDepth, m.DeriveLag,
	)
	return m
}

// Registry exposes the registry so tests can scrape it.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler returns the /metrics scrape handler for this worker's
// registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

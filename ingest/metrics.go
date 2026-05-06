package ingest

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics enumerates the Prometheus counters and histograms emitted by the
// ingest server. Metric names are fixed so dashboards and alerts reference
// stable identifiers.
type Metrics struct {
	writes       *prometheus.CounterVec
	dagSeconds   *prometheus.HistogramVec
	queueDepth   prometheus.Gauge
	bytesHistory *prometheus.HistogramVec

	registry *prometheus.Registry
}

// NewMetrics builds a fresh registry and registers the ingest metric set on
// it. Each Server owns its own registry so tests don't leak counters across
// suite runs (the default prometheus registry is global state).
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,

		writes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_ingest_writes_total",
				Help: "Total ingest write attempts by provider and outcome status.",
			},
			[]string{"provider", "status"},
		),
		dagSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_ingest_dag_write_seconds",
				Help:    "Latency of ingest-to-worker enqueue by provider.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"provider"},
		),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_ingest_worker_queue_depth",
			Help: "Best-effort snapshot of pending items in the worker queue, as observed on the ingest enqueue path. The underlying Pool is shared with the proxy enqueue paths, which do not currently update this gauge — so the value reflects ingest-side observations only.",
		}),
		bytesHistory: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_ingest_body_bytes",
				Help:    "Size of accepted ingest envelopes by provider.",
				Buckets: prometheus.ExponentialBucketsRange(256, 16*1024*1024, 12),
			},
			[]string{"provider"},
		),
	}
	reg.MustRegister(m.writes, m.dagSeconds, m.queueDepth, m.bytesHistory)
	return m
}

// Registry exposes the backing *prometheus.Registry so callers can mount a
// scrape handler or assert on the metric state in tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler returns an http.Handler that serves the Prometheus scrape endpoint
// backed by this Metrics' registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// Result enumerates the status-label values emitted on the writes
// counter. Closed enumeration keeps dashboards safe against label typos.
type Result string

const (
	ResultAccepted      Result = "accepted"
	ResultRejectEnv     Result = "reject_envelope"
	ResultRejectParse   Result = "reject_parse"
	ResultUnknownProv   Result = "unknown_provider"
	ResultQueueFull     Result = "queue_full"
	ResultDownstreamErr Result = "downstream_error"
)

// ObserveWrite increments the writes counter for a given provider/result.
// A zero-length provider label becomes "unknown" so scrapes don't drop rows.
func (m *Metrics) ObserveWrite(provider string, result Result, bodyBytes int) {
	if provider == "" {
		provider = "unknown"
	}
	m.writes.WithLabelValues(provider, string(result)).Inc()
	if result == ResultAccepted && bodyBytes > 0 {
		m.bytesHistory.WithLabelValues(provider).Observe(float64(bodyBytes))
	}
}

// ObserveDAGLatency records how long it took to enqueue a turn into the worker
// pool. Latency is a cheap proxy for back-pressure so we graph it even though
// enqueue is nominally O(1) — a slow enqueue hints at queue saturation.
func (m *Metrics) ObserveDAGLatency(provider string, seconds float64) {
	if provider == "" {
		provider = "unknown"
	}
	m.dagSeconds.WithLabelValues(provider).Observe(seconds)
}

// SetQueueDepth updates the worker queue depth gauge.
func (m *Metrics) SetQueueDepth(depth int) {
	m.queueDepth.Set(float64(depth))
}

package ingest

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// providerUnknown is the label value substituted for an empty provider.
// Prometheus drops a series with an empty label value, so a turn whose
// provider never got set would vanish from the counter entirely rather than
// show up as the anomaly it is.
const providerUnknown = "unknown"

// Metrics enumerates the Prometheus counters and histograms emitted by the
// ingest server. Metric names are fixed so dashboards and alerts reference
// stable identifiers.
type Metrics struct {
	writes        *prometheus.CounterVec
	dagSeconds    *prometheus.HistogramVec
	queueDepth    prometheus.Gauge
	bytesHistory  *prometheus.HistogramVec
	rawOnlyStamps *prometheus.CounterVec

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
				Buckets: prometheus.ExponentialBucketsRange(256, 64*1024*1024, 14),
			},
			[]string{"provider"},
		),
		rawOnlyStamps: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_ingest_rawonly_stamp_total",
				Help: "Capture-side fields restored onto a server-side raw-only reduction, by provider, field, and whether the envelope supplied the value or ingest fell back. A rising fallback rate means raw-only rows are landing without a capture-side duration or with an ingest-clock timestamp.",
			},
			[]string{"provider", "field", "source"},
		),
	}
	reg.MustRegister(m.writes, m.dagSeconds, m.queueDepth, m.bytesHistory, m.rawOnlyStamps)
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
	// ResultInternalErr covers a failure inside the handler itself (e.g. a
	// server-side marshal that should never fail) — a 500, distinct from a bad
	// payload or a downstream outage — so no handler exit is invisible to the
	// writes counter.
	ResultInternalErr Result = "internal_error"
	// ResultRejectOversize is a body-limit 413, recorded pre-parse by the
	// app-level error handler — always under provider "unknown".
	ResultRejectOversize Result = "reject_oversize"
)

// ObserveWrite increments the writes counter for a given provider/result.
// A zero-length provider label becomes "unknown" so scrapes don't drop rows.
func (m *Metrics) ObserveWrite(provider string, result Result, bodyBytes int) {
	if provider == "" {
		provider = providerUnknown
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
		provider = providerUnknown
	}
	m.dagSeconds.WithLabelValues(provider).Observe(seconds)
}

// StampField enumerates the capture-side fields ingest restores onto a
// server-side raw-only reduction. Closed enumeration keeps dashboards safe
// against label typos.
type StampField string

const (
	StampFieldDuration  StampField = "duration"
	StampFieldCreatedAt StampField = "created_at"
)

// StampSource names the meta field that supplied a stamped value, or the
// fallback taken when none did. Naming the field rather than just
// "envelope" is what lets a scrape answer which producers have been
// upgraded, per field, without correlating against deploy history.
//
// StampSourceFallback means the envelope carried nothing usable. What that
// leaves behind is field-specific: an unstamped duration for
// StampFieldDuration, and whatever the reducer produced — ingest's own clock
// for providers whose wire format carries no timestamp — for
// StampFieldCreatedAt.
//
// The fallback bucket is the point of this metric. Both fields degrade
// silently otherwise: a NULL duration and an ingest-time CreatedAt are
// indistinguishable downstream from a turn that genuinely had them.
type StampSource string

const (
	// StampSourceElapsed is meta.elapsed_seconds, for the duration.
	StampSourceElapsed StampSource = "elapsed_seconds"

	// StampSourceCapturedAt is meta.captured_at — the turn's completion
	// instant, exactly what CreatedAt denotes.
	StampSourceCapturedAt StampSource = "captured_at"

	// StampSourceTsRequest is meta.ts_request, the turn's request instant,
	// offset by elapsed_seconds when the envelope carries one.
	StampSourceTsRequest StampSource = "ts_request"

	// StampSourceFallback means no capture-side source was available.
	StampSourceFallback StampSource = "fallback"
)

// ObserveRawOnlyStamp records one capture-side field restored (or not) on a
// server-side raw-only reduction.
func (m *Metrics) ObserveRawOnlyStamp(provider string, field StampField, source StampSource) {
	if provider == "" {
		provider = providerUnknown
	}
	m.rawOnlyStamps.WithLabelValues(provider, string(field), string(source)).Inc()
}

// SetQueueDepth updates the worker queue depth gauge.
func (m *Metrics) SetQueueDepth(depth int) {
	m.queueDepth.Set(float64(depth))
}

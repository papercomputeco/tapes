package embedworker

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Pass result labels. A small, fixed label set keeps cardinality bounded
// (mirrors the derive worker's pattern).
const (
	resultOK    = "ok"
	resultError = "error"
)

// Metrics is the Prometheus surface for the embed worker. Each worker
// owns its own registry so tests scrape in isolation and the hosting
// command mounts Handler on its listener.
type Metrics struct {
	registry *prometheus.Registry

	// Passes counts embed passes by outcome: ok / error (an
	// infrastructure failure — candidate listing or prune — that aborted
	// the pass; per-span failures are not pass failures).
	Passes *prometheus.CounterVec

	// PassDuration is the wall time of one full pass.
	PassDuration prometheus.Histogram

	// Per-span outcomes accumulated across passes. Together they close the
	// identity scanned = embedded + upToDate + empty + poisoned + failed,
	// so a dashboard can both account for every candidate and SEE the
	// re-stream cost: the embed pass lists and renders every main llm
	// span each pass, so a high upToDate (or scanned) with embedded≈0 is
	// the wasteful-rescan signature, invisible if only embedded/failed
	// were exposed. Failed is a per-span embed/write error retried next
	// pass; UpToDate skipped because content+model already match; Empty
	// skipped because the delta rendered to no embeddable text; Poisoned
	// skipped because the span already failed deterministically under this
	// content and model.
	SpansScanned  prometheus.Counter
	SpansEmbedded prometheus.Counter
	SpansUpToDate prometheus.Counter
	SpansEmpty    prometheus.Counter
	SpansPoisoned prometheus.Counter
	SpansFailed   prometheus.Counter

	// SpansChunked counts spans whose text exceeded the model context
	// window and was embedded as several pieces; ChunkRows counts the
	// pieces written (so chunked spans contribute more than one row).
	SpansChunked prometheus.Counter
	ChunkRows    prometheus.Counter

	// Oversize counts spans the model rejected as too large; OversizeTokens
	// is the distribution of their reported/estimated token sizes — the
	// answer to "how big are the inputs we're chunking?".
	Oversize       prometheus.Counter
	OversizeTokens prometheus.Histogram

	// SpanFailures counts deterministic per-span failures by reason (e.g.
	// oversize, api_400) — the recorded, non-retried failures, a subset of
	// SpansFailed broken out for alerting on a specific cause.
	SpanFailures *prometheus.CounterVec

	// OrphansPruned counts embedding rows removed because their span no
	// longer exists as a main llm span.
	OrphansPruned prometheus.Counter

	// ConsecutiveFailures mirrors the worker's in-memory outage counter:
	// non-zero means passes are currently failing on infrastructure and
	// backing off. Alert on sustained non-zero.
	ConsecutiveFailures prometheus.Gauge

	// LastSuccessTimestamp is the Unix time of the last successful pass.
	// Alert on staleness (time() - metric > interval): it catches a
	// wedged-but-not-erroring worker, which ConsecutiveFailures cannot.
	LastSuccessTimestamp prometheus.Gauge
}

// NewMetrics constructs the embed worker's counters on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		Passes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_embed_worker_passes_total",
				Help: "Span embed passes run by the embed worker, by outcome.",
			},
			[]string{"result"},
		),
		PassDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tapes_embed_worker_pass_duration_seconds",
			Help:    "Wall time of one full span embed pass.",
			Buckets: prometheus.DefBuckets,
		}),
		SpansScanned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_scanned_total",
			Help: "Candidate main llm spans considered per pass (includes already-embedded spans re-scanned every pass).",
		}),
		SpansEmbedded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_embedded_total",
			Help: "Spans embedded (new or re-embedded after a content/model change).",
		}),
		SpansUpToDate: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_up_to_date_total",
			Help: "Spans skipped because their embedding already matches current content and model; high vs. scanned indicates wasteful re-scans.",
		}),
		SpansEmpty: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_empty_total",
			Help: "Spans skipped because their delta content rendered to no embeddable text.",
		}),
		SpansPoisoned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_poisoned_total",
			Help: "Spans skipped because they already failed deterministically under this content and model; not retried until content/model changes.",
		}),
		SpansFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_failed_total",
			Help: "Per-span embed/write failures this pass (transient retried next pass; deterministic also recorded and skipped thereafter).",
		}),
		SpansChunked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_spans_chunked_total",
			Help: "Spans whose text exceeded the model context window and was embedded as multiple pieces.",
		}),
		ChunkRows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_chunk_rows_total",
			Help: "Embedding rows written (one per piece); chunked spans contribute more than one.",
		}),
		Oversize: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_oversize_total",
			Help: "Spans the model rejected as exceeding its context window (whether the split then succeeded or exhausted the depth cap).",
		}),
		OversizeTokens: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "tapes_embed_worker_oversize_tokens",
			Help:    "Reported or estimated token count of oversized spans.",
			Buckets: []float64{8192, 10000, 12000, 16000, 24000, 32000, 48000, 64000, 96000, 131072},
		}),
		SpanFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_embed_worker_span_failures_total",
				Help: "Deterministic per-span embed failures recorded this pass, by reason.",
			},
			[]string{"reason"},
		),
		OrphansPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tapes_embed_worker_orphans_pruned_total",
			Help: "Orphaned embedding rows removed (their span was pruned or reclassified by a re-derive).",
		}),
		ConsecutiveFailures: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_embed_worker_consecutive_pass_failures",
			Help: "Consecutive embed-pass infrastructure failures; non-zero means the database or embedding backend is unreachable and passes are backing off.",
		}),
		LastSuccessTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_embed_worker_last_success_timestamp_seconds",
			Help: "Unix time of the last successful embed pass; alert when time() minus this exceeds the expected interval.",
		}),
	}
	reg.MustRegister(
		m.Passes, m.PassDuration,
		m.SpansScanned, m.SpansEmbedded, m.SpansUpToDate, m.SpansEmpty, m.SpansPoisoned, m.SpansFailed,
		m.SpansChunked, m.ChunkRows, m.Oversize, m.OversizeTokens, m.SpanFailures,
		m.OrphansPruned, m.ConsecutiveFailures, m.LastSuccessTimestamp,
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

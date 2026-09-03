package extproc

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the Prometheus surface for tapes-extproc. Each Processor owns
// its own registry so tests can scrape in isolation; the cmd-level wiring
// mounts /metrics on the existing HTTP mux.
// DefaultLargeTurnThreshold is the response-body size at which a turn is
// counted as "large". Chosen to sit above the expected p99 for normal
// agentic traffic (~1 MB) but below pathological tool-use turns. The
// threshold is observability, not a cap — turns above it are still
// captured and dispatched as usual.
//
// It is deliberately NOT capture policy, and that was a decision rather than
// an omission — see fixtures/drop-reason/README.md, which records it alongside
// the drop-reason taxonomy because it is the same kind of question. It gates a
// counter and nothing else: two builds that disagree about it capture, dispatch
// and store byte-identical turns, and differ only in one deployment's sizing
// dashboard. That is also why it is settable per deployment. A threshold that
// dropped or truncated a turn would have to be contract; this one is an
// observability knob wearing a policy's shape.
const DefaultLargeTurnThreshold = 4 * 1024 * 1024

// bodySizeBuckets is the single bucket scheme shared by every body-size
// histogram (tapes_extproc_body_bytes, tapes_extproc_body_bytes_by_outcome,
// tapes_extproc_request_content_length_bytes): 256 B → 64 MiB across 14
// buckets. 64 MiB tops the 32 MB Anthropic Messages contract plus
// gRPC/framing headroom; defining it once keeps the histograms comparable
// bucket-for-bucket in PromQL. Do not fork per-histogram copies.
var bodySizeBuckets = prometheus.ExponentialBucketsRange(256, 64*1024*1024, 14)

// Recurring label values. These are wire-visible: they appear verbatim in the
// Prometheus label set and in the dispatched envelope, so every dashboard,
// alert and downstream consumer reads these exact strings. Naming them keeps
// the two dozen sites that emit them from drifting one character apart.
const (
	// labelUnknown is the value every normalizer falls back to. It means
	// "we looked and could not tell", which is deliberately distinct from a
	// label being absent.
	labelUnknown = "unknown"
	// labelOther collapses recognized-but-unenumerated values so provider
	// and endpoint cardinality stays bounded.
	labelOther = "other"
	// labelAnthropic, labelOpenAI and labelOllama are the enumerated
	// providers — the set normalizeProvider passes through rather than
	// collapsing into labelOther.
	labelAnthropic = "anthropic"
	labelOpenAI    = "openai"
	labelOllama    = "ollama"
	// labelTrue and labelFalse are the stream label's two decided values;
	// the label is a tri-state (true / false / unknown), not a boolean.
	labelTrue  = "true"
	labelFalse = "false"
	// labelIdentity is the no-encoding content-encoding value. Empty is
	// normalized to it, so "absent" and "identity" share one label.
	labelIdentity = "identity"
	// labelGzip is the one content-encoding named outside the enumeration,
	// by the pre-render loop.
	labelGzip = "gzip"
)

type Metrics struct {
	registry *prometheus.Registry

	captured           *prometheus.CounterVec
	terminal           *prometheus.CounterVec
	largeTurns         *prometheus.CounterVec
	dropped            *prometheus.CounterVec
	reducerEmpty       *prometheus.CounterVec
	responseDecoded    *prometheus.CounterVec
	responseSalvaged   *prometheus.CounterVec
	rawAttached        *prometheus.CounterVec
	rawSkipped         *prometheus.CounterVec
	rawFallback        *prometheus.CounterVec
	sseChunks          *prometheus.HistogramVec
	turnDuration       *prometheus.HistogramVec
	terminalDuration   *prometheus.HistogramVec
	bodyBytes          *prometheus.HistogramVec
	bodyBytesByOutcome *prometheus.HistogramVec
	dispatchSeconds    *prometheus.HistogramVec
	inflight           prometheus.Gauge

	requestContentLength        *prometheus.HistogramVec
	requestContentLengthUnknown *prometheus.CounterVec

	buildInfo *prometheus.GaugeVec

	largeTurnThreshold int
}

// NewMetrics constructs a fresh registry populated with the full extproc set.
// AllDropReasons label rows are pre-created so /metrics renders every
// possible reason even before the first drop of that kind.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		captured: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_turns_captured_total",
				Help: "Completed turns successfully dispatched to tapes-ingest.",
			},
			[]string{"provider"},
		),
		terminal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_turns_terminal_total",
				Help: "Terminal extproc turn outcomes by bounded request/upstream dimensions. Raw request IDs, paths, and model names are intentionally excluded from labels and emitted only in structured logs.",
			},
			[]string{"provider", "outcome", "reason", "endpoint", "stream", "model_family", "upstream_status_class"},
		),
		largeTurns: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_turns_large_total",
				Help: "Turns whose response body exceeded the large-turn threshold. These are captured and dispatched normally — the counter is observability for cluster sizing, not a rejection signal.",
			},
			[]string{"provider"},
		),
		dropped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_turns_dropped_total",
				Help: "Turns dropped before landing in tapes, by provider and reason.",
			},
			[]string{"provider", "reason"},
		),
		reducerEmpty: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_reducer_empty_total",
				Help: "Turns whose reducer output would fail tapes ingest's validateReducedResponse — empty Content, missing Role, or a block with no Type. Labelled by upstream content-type and HTTP status so a dashboard can see whether the empty cases concentrate on a particular upstream shape (e.g. JSON error envelopes vs. truncated event-streams).",
			},
			[]string{"provider", "content_type", "upstream_status"},
		),
		responseDecoded: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_response_decoded_total",
				Help: "Decode attempts on the upstream response body before it reaches the reducer. Labelled by the upstream's Content-Encoding header (\"\" / \"identity\" / \"gzip\" / unknown) and the outcome (\"ok\" or \"error\"). Spikes in gzip@ok track how much of the traffic is being decompressed; any nonzero gzip@error or <unknown>@error count is an operator-actionable bug.",
			},
			[]string{"encoding", "result"},
		),
		responseSalvaged: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_response_decode_salvaged_total",
				Help: "Response decode attempts that recovered decoded bytes from a truncated compressed body. message_stop_seen indicates whether the decoded SSE body included Anthropic's terminal message_stop event.",
			},
			[]string{"provider", "encoding", "message_stop_seen"},
		),
		rawAttached: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_raw_response_attached_total",
				Help: "Turns dispatched carrying verbatim upstream response bytes. shape=dual keeps the adapter's reduction alongside the bytes; shape=raw_only leaves the reduction to ingest.",
			},
			[]string{"provider", "shape"},
		),
		rawSkipped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_raw_response_skipped_total",
				Help: "Turns where the configured mode wanted verbatim bytes but they were withheld. These rows land fidelity:reduced, so a rising rate means the raw lane is quietly not covering all traffic.",
			},
			[]string{"provider", "reason"},
		),
		rawFallback: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_raw_response_fallback_total",
				Help: "Raw-only turns that kept the adapter's reduction because ingest could not have produced one from the bytes (encoding it cannot decode, or content salvaged from a truncated body).",
			},
			[]string{"provider", "reason"},
		),
		sseChunks: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_extproc_sse_chunks_per_turn",
				Help:    "Number of SSE chunks per streamed turn.",
				Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
			},
			[]string{"provider"},
		),
		turnDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "tapes_extproc_turn_duration_seconds",
				Help: "End-to-end duration of a captured turn, RequestHeaders to dispatch.",
				// DefBuckets caps at 10s, but turns regularly run to the 60s
				// ext_proc messageTimeout and beyond. Keep the DefBuckets
				// boundaries (so existing dashboards stay valid) and extend the
				// tail to 300s. See PCC-577.
				Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90, 120, 180, 300},
			},
			[]string{"provider"},
		),
		terminalDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "tapes_extproc_turn_terminal_duration_seconds",
				Help: "Header-to-terminal-outcome duration by provider, outcome, and reason.",
				// Widened past DefBuckets' 10s ceiling: terminal outcomes
				// include the 60s ext_proc messageTimeout 504s, which DefBuckets
				// silently piled into +Inf. Same boundaries + 300s tail as
				// turn_duration_seconds. See PCC-577.
				Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90, 120, 180, 300},
			},
			[]string{"provider", "outcome", "reason"},
		),
		bodyBytes: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_extproc_body_bytes",
				Help:    "Accumulated body size at EndOfStream, by provider and side.",
				Buckets: bodySizeBuckets,
			},
			[]string{"provider", "side"},
		),
		bodyBytesByOutcome: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_extproc_body_bytes_by_outcome",
				Help:    "Accumulated request/response body size by terminal outcome. Use this to compare failed and captured turn sizes without relying on success-only body_bytes.",
				Buckets: bodySizeBuckets,
			},
			[]string{"provider", "side", "outcome", "reason"},
		),
		dispatchSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_extproc_dispatch_latency_seconds",
				Help:    "Latency of the HTTP POST to tapes-ingest.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"provider"},
		),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_extproc_inflight_dispatches",
			Help: "Number of dispatches currently awaiting completion.",
		}),
		requestContentLength: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_extproc_request_content_length_bytes",
				Help:    "Request Content-Length as observed at the request-headers phase, before any body message can be rejected at the gRPC recv boundary — so >4 MiB requests that never survive the body phase are still sized here. Absent or unparseable Content-Length is never observed (not even as 0); those requests are counted in tapes_extproc_request_content_length_unknown_total instead.",
				Buckets: bodySizeBuckets,
			},
			[]string{"provider"},
		),
		requestContentLengthUnknown: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_extproc_request_content_length_unknown_total",
				Help: "Requests whose Content-Length header was absent or unparseable at the request-headers phase (e.g. chunked/streaming clients). These requests make no observation in tapes_extproc_request_content_length_bytes, so this counter measures exactly the population that histogram cannot see.",
			},
			[]string{"provider"},
		),
		buildInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "tapes_extproc_build_info",
				Help: "Build metadata for the running binary. Value is always 1 so other metrics can be joined onto version and commit with a group_left on this series.",
			},
			[]string{"version", "commit"},
		),
	}
	reg.MustRegister(m.captured, m.terminal, m.largeTurns, m.dropped, m.reducerEmpty, m.responseDecoded, m.responseSalvaged, m.rawAttached, m.rawSkipped, m.rawFallback, m.sseChunks, m.turnDuration, m.terminalDuration, m.bodyBytes, m.bodyBytesByOutcome, m.dispatchSeconds, m.inflight, m.requestContentLength, m.requestContentLengthUnknown, m.buildInfo)
	m.largeTurnThreshold = DefaultLargeTurnThreshold

	// Pre-create a zero row for every known drop reason × every provider we
	// might route through. Keeps dashboards / alerts stable on cold starts
	// without waiting for a first drop of each kind. reducer_empty is left
	// unseeded because its content_type / upstream_status label space is
	// open-ended and would just create a wall of zero rows.
	preRenderProviders := []string{labelAnthropic, labelOpenAI, labelOllama, labelUnknown}
	for _, p := range preRenderProviders {
		for _, r := range AllDropReasons() {
			m.dropped.WithLabelValues(p, string(r)).Add(0)
		}
		for _, enc := range []string{labelGzip, "x-gzip"} {
			for _, seen := range []string{labelFalse, labelTrue} {
				m.responseSalvaged.WithLabelValues(p, enc, seen).Add(0)
			}
		}
	}

	return m
}

// LargeTurnThreshold returns the byte threshold above which a turn is
// counted in tapes_extproc_turns_large_total. Configurable via
// SetLargeTurnThreshold; defaults to DefaultLargeTurnThreshold.
func (m *Metrics) LargeTurnThreshold() int { return m.largeTurnThreshold }

// SetLargeTurnThreshold overrides the "large turn" byte threshold. Useful
// for tests and for per-environment tuning once real p99 data is in.
func (m *Metrics) SetLargeTurnThreshold(bytes int) {
	if bytes > 0 {
		m.largeTurnThreshold = bytes
	}
}

// ObserveTurnSize increments tapes_extproc_turns_large_total when the
// response body exceeded the threshold. Called unconditionally on every
// dispatched turn; the threshold check lives here so the call site stays
// one line.
func (m *Metrics) ObserveTurnSize(provider string, respBytes int) {
	if provider == "" {
		provider = labelUnknown
	}
	if respBytes > m.largeTurnThreshold {
		m.largeTurns.WithLabelValues(provider).Inc()
	}
}

// Registry exposes the *prometheus.Registry so callers can mount the scrape
// handler or assert on metric state.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler returns an http.Handler serving Prometheus text exposition.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

// ObserveAccepted increments captured_total for a provider.
func (m *Metrics) ObserveAccepted(provider string) {
	if provider == "" {
		provider = labelUnknown
	}
	m.captured.WithLabelValues(provider).Inc()
}

// ObserveDrop increments dropped_total for a provider / reason.
func (m *Metrics) ObserveDrop(provider string, reason DropReason) {
	if provider == "" {
		provider = labelUnknown
	}
	m.dropped.WithLabelValues(provider, string(reason)).Inc()
}

// ObserveTerminal records the bounded terminal outcome dimensions that let
// operators correlate drops with endpoint, stream mode, model family, and
// upstream status class without promoting raw paths/models/request IDs to
// labels.
func (m *Metrics) ObserveTerminal(provider, outcome, reason string, ctx OutcomeContext) {
	provider = normalizeProvider(provider)
	outcome = normalizeOutcome(outcome)
	if reason == "" {
		reason = outcome
	}
	m.terminal.WithLabelValues(
		provider,
		outcome,
		normalizeReason(reason),
		normalizeEndpointLabel(ctx.Endpoint),
		normalizeStreamLabel(ctx.Stream),
		normalizeModelFamily(ctx.ModelFamily),
		normalizeStatusClass(ctx.UpstreamStatusClass, ctx.UpstreamStatus),
	).Inc()
}

// ObserveTerminalDuration records header-to-terminal latency for accepted and
// dropped turns. Zero or negative durations are ignored so partially-populated
// contexts don't create misleading near-zero rows.
func (m *Metrics) ObserveTerminalDuration(provider, outcome, reason string, seconds float64) {
	if seconds <= 0 {
		return
	}
	provider = normalizeProvider(provider)
	outcome = normalizeOutcome(outcome)
	if reason == "" {
		reason = outcome
	}
	m.terminalDuration.WithLabelValues(provider, outcome, normalizeReason(reason)).Observe(seconds)
}

// ObserveBodyBytesByOutcome records body-size distributions for both accepted
// and dropped turns. This complements tapes_extproc_body_bytes, which
// historically measured only successful captured turns.
func (m *Metrics) ObserveBodyBytesByOutcome(provider, side, outcome, reason string, bytes int) {
	if bytes < 0 {
		bytes = 0
	}
	provider = normalizeProvider(provider)
	outcome = normalizeOutcome(outcome)
	if reason == "" {
		reason = outcome
	}
	if side != "request" && side != "response" {
		side = labelUnknown
	}
	m.bodyBytesByOutcome.WithLabelValues(provider, side, outcome, normalizeReason(reason)).Observe(float64(bytes))
}

// ObserveResponseDecoded records one decode attempt on the upstream
// response body. encoding is the raw Content-Encoding header value
// (normalized to lowercase, empty/labelIdentity mapped to labelIdentity).
// result is "ok" or "error". The metric is purely observational —
// success doesn't mean the reducer was happy with the decoded bytes,
// just that decode itself succeeded. Pair with reducer_empty_total
// when investigating which path is dominant.
func (m *Metrics) ObserveResponseDecoded(encoding, result string) {
	enc := normalizeContentEncoding(encoding)
	if result == "" {
		result = labelUnknown
	}
	m.responseDecoded.WithLabelValues(enc, result).Inc()
}

// ObserveResponseDecodeSalvaged records a successful decode from a truncated
// compressed response body. It is separate from response_decoded_total so
// operators can tell normal gzip@ok traffic from Option-A salvage recoveries.
func (m *Metrics) ObserveResponseDecodeSalvaged(provider, encoding string, messageStopSeen bool) {
	provider = normalizeProvider(provider)
	enc := normalizeContentEncoding(encoding)
	m.responseSalvaged.WithLabelValues(provider, enc, strconv.FormatBool(messageStopSeen)).Inc()
}

// normalizeContentEncoding folds the open header-value space into a
// small label set: identity (incl. empty), gzip (incl. x-gzip),
// br/deflate/etc. kept as-is, plus an labelUnknown bucket for everything
// else so cardinality stays bounded across a long tail of one-off
// upstream choices.
func normalizeContentEncoding(ce string) string {
	ce = strings.ToLower(strings.TrimSpace(ce))
	if ce == "" {
		return labelIdentity
	}
	// Multi-encoding values like "gzip, br" get a compact synthetic
	// label so the count of layered cases is still observable without
	// exploding the label space.
	if strings.Contains(ce, ",") {
		return "stacked"
	}
	switch ce {
	case labelIdentity, labelGzip, "x-gzip", "br", "deflate", "zstd":
		return ce
	default:
		return labelUnknown
	}
}

// ObserveReducerEmpty increments the counter that tracks reducer outputs
// that would fail tapes ingest's validator. Labels surface the upstream
// shape (content_type, HTTP status) so an operator can tell whether the
// empty cases concentrate on, e.g., text/event-stream@200 (real reducer
// bug) vs. application/json@5xx (upstream error envelope captured as if
// it were a turn). The content-type label is normalized to its bare MIME
// type to keep cardinality bounded.
func (m *Metrics) ObserveReducerEmpty(provider, contentType string, upstreamStatus int) {
	if provider == "" {
		provider = labelUnknown
	}
	ct := normalizeContentType(contentType)
	status := labelUnknown
	if upstreamStatus > 0 {
		status = strconv.Itoa(upstreamStatus)
	}
	m.reducerEmpty.WithLabelValues(provider, ct, status).Inc()
}

// normalizeContentType drops the charset / boundary parameters and
// lowercases the MIME type so the metric label stays bounded across
// minor header-format variations (e.g. "text/event-stream; charset=utf-8"
// vs. "text/event-stream").
func normalizeContentType(ct string) string {
	if ct == "" {
		return labelUnknown
	}
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}

func normalizeProvider(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	switch provider {
	case labelAnthropic, labelOpenAI, labelOllama:
		return provider
	case "":
		return labelUnknown
	default:
		return labelOther
	}
}

func normalizeOutcome(outcome string) string {
	outcome = strings.ToLower(strings.TrimSpace(outcome))
	switch outcome {
	case "accepted", "dropped":
		return outcome
	default:
		return labelUnknown
	}
}

func normalizeReason(reason string) string {
	reason = strings.TrimSpace(strings.ToLower(reason))
	if reason == "" {
		return labelUnknown
	}
	for _, r := range AllDropReasons() {
		if reason == string(r) {
			return reason
		}
	}
	if reason == "accepted" || reason == labelUnknown {
		return reason
	}
	return labelOther
}

func normalizeEndpointLabel(endpoint string) string {
	endpoint = strings.TrimSpace(strings.ToLower(endpoint))
	switch endpoint {
	case "messages", "messages_count_tokens", "chat_completions", "responses", "ollama_chat", labelOther:
		return endpoint
	default:
		return labelUnknown
	}
}

func normalizeStreamLabel(stream string) string {
	stream = strings.TrimSpace(strings.ToLower(stream))
	switch stream {
	case labelTrue, labelFalse, labelUnknown:
		return stream
	default:
		return labelUnknown
	}
}

func normalizeModelFamily(modelFamily string) string {
	modelFamily = strings.TrimSpace(strings.ToLower(modelFamily))
	// Keep this allowlist intentionally bounded for Prometheus cardinality.
	// Add new first-party model families here as they become routable.
	switch modelFamily {
	case "claude-fable-5-1", "claude-fable-5", "claude-opus-5", "claude-opus-4", "claude-sonnet-5", "claude-sonnet-4", "claude-haiku-4-6", "claude-haiku-4-5", "claude-3-7-sonnet", "claude-3-5-sonnet", "gpt-6-astra", "gpt-5", labelUnknown, labelOther:
		return modelFamily
	case "":
		return labelUnknown
	default:
		return labelOther
	}
}

func normalizeStatusClass(statusClass string, status int) string {
	statusClass = strings.TrimSpace(strings.ToLower(statusClass))
	switch statusClass {
	case "1xx", "2xx", "3xx", "4xx", "5xx", labelUnknown:
		return statusClass
	}
	if status <= 0 {
		return labelUnknown
	}
	switch {
	case status < 200:
		return "1xx"
	case status < 300:
		return "2xx"
	case status < 400:
		return "3xx"
	case status < 500:
		return "4xx"
	case status < 600:
		return "5xx"
	default:
		return labelUnknown
	}
}

// ObserveSSEChunks records how many SSE frames a streamed turn produced.
func (m *Metrics) ObserveSSEChunks(provider string, n int) {
	if provider == "" {
		provider = labelUnknown
	}
	m.sseChunks.WithLabelValues(provider).Observe(float64(n))
}

// ObserveTurnDuration records the header-to-dispatch wall time.
func (m *Metrics) ObserveTurnDuration(provider string, seconds float64) {
	if provider == "" {
		provider = labelUnknown
	}
	m.turnDuration.WithLabelValues(provider).Observe(seconds)
}

// ObserveBodyBytes records accumulated body size by side ("request" | "response").
func (m *Metrics) ObserveBodyBytes(provider, side string, bytes int) {
	if provider == "" {
		provider = labelUnknown
	}
	m.bodyBytes.WithLabelValues(provider, side).Observe(float64(bytes))
}

// ObserveRequestContentLength records a parsed request Content-Length at the
// request-headers phase. Callers must only pass successfully parsed values;
// absent/unparseable headers go through ObserveRequestContentLengthUnknown
// instead so the histogram's low buckets are never corrupted by zero stand-ins.
func (m *Metrics) ObserveRequestContentLength(provider string, bytes int64) {
	m.requestContentLength.WithLabelValues(normalizeProvider(provider)).Observe(float64(bytes))
}

// ObserveRequestContentLengthUnknown counts a request whose Content-Length was
// absent or unparseable at the request-headers phase. Increments only the
// unknown counter — never the content-length histogram.
func (m *Metrics) ObserveRequestContentLengthUnknown(provider string) {
	m.requestContentLengthUnknown.WithLabelValues(normalizeProvider(provider)).Inc()
}

// ObserveDispatchLatency records the HTTP POST roundtrip to ingest.
func (m *Metrics) ObserveDispatchLatency(provider string, seconds float64) {
	if provider == "" {
		provider = labelUnknown
	}
	m.dispatchSeconds.WithLabelValues(provider).Observe(seconds)
}

// SetBuildInfo publishes the build metadata series. The value is always
// exactly 1 (Prometheus build_info convention — enables group_left joins).
func (m *Metrics) SetBuildInfo(version, commit string) {
	m.buildInfo.WithLabelValues(version, commit).Set(1)
}

// SetInflight updates the gauge of currently-dispatching turns.
func (m *Metrics) SetInflight(n int) { m.inflight.Set(float64(n)) }

// AsObserver adapts Metrics to the Dispatcher.Observer interface so the
// extproc dispatcher can emit terminal outcomes without importing promhttp.
func (m *Metrics) AsObserver() Observer { return metricsObserver{m: m} }

type metricsObserver struct{ m *Metrics }

func (o metricsObserver) OnAccepted(provider string, _ string) { o.m.ObserveAccepted(provider) }
func (o metricsObserver) OnDrop(provider string, reason DropReason, _ string) {
	o.m.ObserveDrop(provider, reason)
}

func (o metricsObserver) OnAcceptedContext(provider string, _ string, ctx OutcomeContext) {
	o.m.ObserveTerminal(provider, "accepted", "accepted", ctx)
	o.m.ObserveBodyBytesByOutcome(provider, "request", "accepted", "accepted", ctx.RequestBytes)
	o.m.ObserveBodyBytesByOutcome(provider, "response", "accepted", "accepted", ctx.ResponseBytes)
	o.m.ObserveTurnDuration(provider, ctx.ElapsedSeconds)
	o.m.ObserveTerminalDuration(provider, "accepted", "accepted", ctx.ElapsedSeconds)
}

func (o metricsObserver) OnDropContext(provider string, reason DropReason, _ string, ctx OutcomeContext) {
	o.m.ObserveTerminal(provider, "dropped", string(reason), ctx)
	o.m.ObserveBodyBytesByOutcome(provider, "request", "dropped", string(reason), ctx.RequestBytes)
	o.m.ObserveBodyBytesByOutcome(provider, "response", "dropped", string(reason), ctx.ResponseBytes)
	o.m.ObserveTerminalDuration(provider, "dropped", string(reason), ctx.ElapsedSeconds)
}

func (o metricsObserver) OnDispatchLatency(provider string, _ string, seconds float64) {
	o.m.ObserveDispatchLatency(provider, seconds)
}
func (o metricsObserver) OnInflight(n int) { o.m.SetInflight(n) }

func (o metricsObserver) OnRawResponseAttached(provider, shape string) {
	o.m.ObserveRawResponseAttached(provider, shape)
}

func (o metricsObserver) OnRawResponseSkipped(provider, reason string) {
	o.m.ObserveRawResponseSkipped(provider, reason)
}

func (o metricsObserver) OnRawResponseFallback(provider, reason string) {
	o.m.ObserveRawResponseFallback(provider, reason)
}

// ObserveRawResponseAttached counts a turn dispatched with verbatim bytes.
func (m *Metrics) ObserveRawResponseAttached(provider, shape string) {
	m.rawAttached.WithLabelValues(normalizeProvider(provider), shape).Inc()
}

// ObserveRawResponseSkipped counts a turn whose verbatim bytes were withheld.
func (m *Metrics) ObserveRawResponseSkipped(provider, reason string) {
	m.rawSkipped.WithLabelValues(normalizeProvider(provider), reason).Inc()
}

// ObserveRawResponseFallback counts a raw-only turn that kept its reduction.
func (m *Metrics) ObserveRawResponseFallback(provider, reason string) {
	m.rawFallback.WithLabelValues(normalizeProvider(provider), reason).Inc()
}

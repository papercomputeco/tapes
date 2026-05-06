package api

import (
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the Prometheus surface for the Tapes API server. Each Server
// owns its own registry so tests can scrape in isolation; the production
// path mounts /metrics on the Fiber app via NewServer.
type Metrics struct {
	registry *prometheus.Registry

	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inflight prometheus.Gauge

	// scrapeHandler is the cached /metrics Fiber handler. promhttp.HandlerFor
	// with a non-nil Registry calls MustRegister(...) on the registry to
	// instrument the scrape handler itself (promhttp_metric_handler_*),
	// so building a fresh handler twice against the same Metrics would
	// panic on the second call. Build once at construction; Handler()
	// just returns this field.
	scrapeHandler fiber.Handler
}

// NewMetrics constructs the Tapes API server's RED metrics. Labels stay
// templated (`route`) rather than per-URL so :hash path params don't blow up
// cardinality.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		requests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tapes_apiserver_requests_total",
				Help: "Total HTTP requests handled by the Tapes API server, by route template, method, and status.",
			},
			[]string{"route", "method", "status"},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tapes_apiserver_request_duration_seconds",
				Help:    "Latency of requests handled by the Tapes API server.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"route", "method"},
		),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_apiserver_inflight_requests",
			Help: "In-flight requests currently being handled by the Tapes API server.",
		}),
	}
	reg.MustRegister(m.requests, m.duration, m.inflight)
	m.scrapeHandler = adaptor.HTTPHandler(promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	return m
}

// Registry exposes the *prometheus.Registry so tests can scrape against the
// same registry the middleware writes to.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Middleware returns a Fiber handler that records request count + duration
// per (route template, method, status). Templates like /v1/sessions/:hash
// stay as the label value so the :hash path param never expands cardinality.
//
// Register this OUTSIDE recover.New() (i.e. via app.Use before recover) —
// see resolveStatus for why.
func (m *Metrics) Middleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		m.inflight.Inc()
		defer m.inflight.Dec()

		start := time.Now()
		err := c.Next()

		// Route().Path is the registered template (e.g. /v1/sessions/:hash);
		// we fall back to a sentinel when Fiber's router never advanced past
		// the middleware's own "/" entry, so unknown URLs don't each become
		// their own series. Fiber v2 keeps c.route pointing at the last
		// matched route — which for an unhandled request is this very
		// middleware (path "/") — rather than nilling it, so we can't rely
		// on an empty path. Detecting an unmatched request via the
		// framework-emitted 404 fiber.Error is the contract that survives
		// across Fiber versions: a real handler returning fiber.ErrNotFound
		// is rare and harmless to bucket with router 404s here.
		route := c.Route().Path
		if route == "" || isRouterNotFound(err) {
			route = "unmatched"
		}

		m.requests.WithLabelValues(route, c.Method(), strconv.Itoa(resolveStatus(c, err))).Inc()
		m.duration.WithLabelValues(route, c.Method()).Observe(time.Since(start).Seconds())

		return err
	}
}

// resolveStatus returns the HTTP status the client will see for this
// request. Two reasons we can't just read c.Response().StatusCode():
//
//  1. When a downstream middleware (recover.New, an auth check, the
//     route handler) returns a non-nil error, Fiber routes it through
//     the registered ErrorHandler — but only AFTER all middleware has
//     unwound. By the time this middleware records metrics, the status
//     is still its pre-error default (typically 200).
//
//  2. recover.New translates a panic into a generic error rather than
//     calling c.Status(500), so even with the metrics middleware
//     wrapping recover, the response status hasn't been touched yet.
//
// Mirror what Fiber's default ErrorHandler will do: a *fiber.Error
// carries the code; everything else is treated as 500. If err is nil,
// trust whatever the handler set. Use errors.As so a wrapped fiber.Error
// (e.g. fmt.Errorf("...: %w", fiber.ErrBadRequest)) is still recognized
// — Fiber's own ErrorHandler does the same, so the metrics row should
// agree with what the client actually sees.
func resolveStatus(c *fiber.Ctx, err error) int {
	if err == nil {
		return c.Response().StatusCode()
	}
	var e *fiber.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return fiber.StatusInternalServerError
}

// isRouterNotFound reports whether err is the synthetic 404 the Fiber router
// itself emits when no registered route matches the request. We use this to
// detect "unmatched" so the cardinality of the requests counter stays bounded
// by the number of registered routes, not by attacker-controlled URLs.
//
// A naive "any 404 *fiber.Error is unmatched" would silently misclassify
// any handler that returns fiber.ErrNotFound (or wraps it) — those would
// drop their route dimension and show up as `route="unmatched"`. Distinguish
// the two via errors.Is: handler-returned fiber.ErrNotFound (and wraps of
// it) match the package singleton; the router emits a freshly-allocated
// *fiber.Error with message "Cannot METHOD /path" which does not. So we
// treat err as router-emitted iff it's a 404 fiber.Error that does NOT
// match the singleton — handler-emitted fiber.ErrNotFound buckets on its
// registered route template instead of collapsing to unmatched.
func isRouterNotFound(err error) bool {
	var e *fiber.Error
	if !errors.As(err, &e) || e.Code != fiber.StatusNotFound {
		return false
	}
	return !errors.Is(err, fiber.ErrNotFound)
}

// Handler returns a Fiber handler that serves Prometheus text exposition
// from this Metrics instance's registry. Mount it at /metrics with no
// auth. The handler is built once at NewMetrics time and cached — see the
// scrapeHandler field comment for why.
func (m *Metrics) Handler() fiber.Handler {
	return m.scrapeHandler
}

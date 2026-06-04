package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// newTestApp constructs a fresh *Metrics and a bare Fiber app wired with
// the metrics middleware in the production order (metrics outside
// recover.New). Use it for specs that exercise middleware behavior in
// isolation — registering ad-hoc routes against the returned app — and
// don't need the full Server's v1 routes, swagger, or MCP surface. Specs
// that need real handlers (`/ping`, `/v1/sessions/:hash`) should keep
// using NewServer via the suite-level BeforeEach.
func newTestApp() (*Metrics, *fiber.App) {
	m := NewMetrics()
	app := fiber.New()
	app.Use(m.Middleware())
	app.Use(recover.New())
	return m, app
}

// counterValue returns the value of tapes_apiserver_requests_total for the
// given (route, method, status) labelset, or 0 if no such row exists. We
// scrape the registry directly via Gather() rather than the HTTP exposition
// so tests assert against typed values, not regexes over text.
func counterValue(reg *prometheus.Registry, route, method, status string) float64 {
	mfs, err := reg.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, mf := range mfs {
		if mf.GetName() != "tapes_apiserver_requests_total" {
			continue
		}
		for _, m := range mf.Metric {
			if labelsMatch(m, map[string]string{"route": route, "method": method, "status": status}) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// countSeriesWithRoute returns the number of distinct counter rows whose
// route label equals the given value. Used to verify cardinality contracts
// (e.g. that unmatched 404s collapse to a single sentinel row).
func countSeriesWithRoute(reg *prometheus.Registry, route string) int {
	mfs, err := reg.Gather()
	Expect(err).NotTo(HaveOccurred())
	count := 0
	for _, mf := range mfs {
		if mf.GetName() != "tapes_apiserver_requests_total" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "route" && lp.GetValue() == route {
					count++
					break
				}
			}
		}
	}
	return count
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, lp := range m.Label {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

var _ = Describe("API server Prometheus metrics", func() {
	var server *Server

	BeforeEach(func() {
		logger := tapeslogger.NewNoop()
		driver := inmemory.NewDriver()

		var err error
		server, err = NewServer(Config{ListenAddr: ":0"}, driver, logger)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("/metrics endpoint", func() {
		It("is scrape-able without auth and exposes the tapes_apiserver_* surface", func() {
			// Touch /ping first so the requests counter and duration
			// histogram each have an observed labelset. Without an
			// observation prometheus omits the metric family entirely
			// (only the always-present gauge would otherwise show up),
			// which would make an empty scrape look misleadingly bare.
			ping, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/ping", nil)
			Expect(err).NotTo(HaveOccurred())
			_, err = server.app.Test(ping)
			Expect(err).NotTo(HaveOccurred())

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
			Expect(err).NotTo(HaveOccurred())

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			text := string(body)

			// All three RED metrics should be present now that the
			// counter and histogram each have at least one observation.
			Expect(text).To(ContainSubstring("tapes_apiserver_requests_total"))
			Expect(text).To(ContainSubstring("tapes_apiserver_request_duration_seconds"))
			Expect(text).To(ContainSubstring("tapes_apiserver_inflight_requests"))
		})
	})

	Describe("RED counter labels", func() {
		It("uses the templated route, not the materialized URL, for parameterised paths", func() {
			// /v1/stems/:hash matches even though the hash slot is empty
			// or fake — we only care that the registered template is what
			// lands in the `route` label. We hit it twice with different
			// :hash values and assert they collapse to a single series.
			for _, hash := range []string{"abc123", "def456"} {
				req, err := http.NewRequestWithContext(
					context.Background(), http.MethodGet, "/v1/stems/"+hash, nil)
				Expect(err).NotTo(HaveOccurred())
				_, err = server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
			}

			// Both requests should hit the same templated row. The actual
			// status the handler returns for an unknown hash is 404, but
			// the precise code is not what this test asserts — only that
			// the row exists and counts both hits.
			total := 0.0
			for _, status := range []string{"200", "404", "500"} {
				total += counterValue(server.metrics.Registry(),
					"/v1/stems/:hash", http.MethodGet, status)
			}
			Expect(total).To(BeNumerically(">=", 2.0),
				"both /v1/stems/<hash> hits should land on the templated row")

			// And no row should carry the materialized URL as the route.
			Expect(countSeriesWithRoute(server.metrics.Registry(),
				"/v1/stems/abc123")).To(Equal(0))
			Expect(countSeriesWithRoute(server.metrics.Registry(),
				"/v1/stems/def456")).To(Equal(0))
		})

		It("collapses unmatched 404s to a single 'unmatched' sentinel row", func() {
			// Three different never-registered URLs. Without the sentinel,
			// each materialized path would become its own series — that
			// would let an attacker inflate cardinality with random URLs.
			for _, path := range []string{"/nope/one", "/nope/two", "/totally/different"} {
				req, err := http.NewRequestWithContext(
					context.Background(), http.MethodGet, path, nil)
				Expect(err).NotTo(HaveOccurred())
				_, err = server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
			}

			// Only one row should exist for the sentinel, regardless of how
			// many distinct unmatched URLs we threw at it.
			Expect(countSeriesWithRoute(server.metrics.Registry(),
				"unmatched")).To(Equal(1))
			Expect(counterValue(server.metrics.Registry(),
				"unmatched", http.MethodGet, "404")).To(Equal(3.0))
		})
	})

	Describe("status derivation under error conditions", func() {
		It("counts a panic as 500 (recover.New() runs inside the metrics middleware)", func() {
			// The metrics middleware wraps recover.New, so a panic must
			// surface back through the metrics middleware as an error,
			// get mapped to 500, and land in the requests_total counter.
			// Use a bare test app so the route registration here does not
			// pollute the suite-level Server with /_test/* paths.
			m, app := newTestApp()
			app.Get("/_test/panic", func(c *fiber.Ctx) error {
				panic("boom")
			})

			req, err := http.NewRequestWithContext(
				context.Background(), http.MethodGet, "/_test/panic", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			Expect(counterValue(m.Registry(),
				"/_test/panic", http.MethodGet, "500")).To(Equal(1.0))
		})

		It("counts a wrapped *fiber.Error with the wrapped status code", func() {
			// errors.As inside resolveStatus should recover the inner
			// fiber.Error from a fmt.Errorf("...: %w", ...) wrap, matching
			// what Fiber's own ErrorHandler does — so the metrics row
			// agrees with the actual response status the client sees.
			m, app := newTestApp()
			app.Get("/_test/wrapped", func(c *fiber.Ctx) error {
				return fmt.Errorf("validation failed: %w", fiber.ErrBadRequest)
			})

			req, err := http.NewRequestWithContext(
				context.Background(), http.MethodGet, "/_test/wrapped", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))

			Expect(counterValue(m.Registry(),
				"/_test/wrapped", http.MethodGet, "400")).To(Equal(1.0))
		})

		It("keeps unmatched-route attribution intact even when a literal '/' route is registered", func() {
			// Defensive contract test, intentionally fragile.
			//
			// Today the unmatched bucket relies on isRouterNotFound: an
			// unmatched URL leaves c.Route().Path pointing at the
			// metrics-middleware mount path (which is "/"), and we
			// reassign route to "unmatched" only because the router
			// emits a 404 *fiber.Error. If a future change either
			//
			//   (a) removes the isRouterNotFound branch in Middleware,
			//       or
			//   (b) registers a real handler at literal "/" while
			//       leaving the unmatched detection in place,
			//
			// real "/" traffic and unmatched-router-fallthrough traffic
			// could both label-encode as route="/" and the unmatched
			// sentinel would silently disappear. This spec exercises
			// case (b): a real "/" handler IS registered, and the
			// assertion is that unmatched URLs still land on
			// route="unmatched", NOT on route="/" with status="404".
			//
			// If this test ever fails, the contract is broken — read
			// the comment in api/metrics.go around isRouterNotFound and
			// decide whether to widen the unmatched detection or accept
			// the new behavior. Don't paper over the assertion.
			m, app := newTestApp()
			app.Get("/", func(c *fiber.Ctx) error { return c.SendString("home") })

			// Hit the real "/" handler — should land on route="/" 200.
			homeReq, err := http.NewRequestWithContext(
				context.Background(), http.MethodGet, "/", nil)
			Expect(err).NotTo(HaveOccurred())
			homeResp, err := app.Test(homeReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(homeResp.StatusCode).To(Equal(http.StatusOK))

			// Hit a path that doesn't match anything — should land on
			// route="unmatched" 404, NOT on route="/" 404.
			missReq, err := http.NewRequestWithContext(
				context.Background(), http.MethodGet, "/totally/unknown", nil)
			Expect(err).NotTo(HaveOccurred())
			missResp, err := app.Test(missReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(missResp.StatusCode).To(Equal(http.StatusNotFound))

			Expect(counterValue(m.Registry(), "/", http.MethodGet, "200")).
				To(Equal(1.0), "real / traffic must count as route=/ status=200")
			Expect(counterValue(m.Registry(), "unmatched", http.MethodGet, "404")).
				To(Equal(1.0), "unmatched paths must still bucket as unmatched even with a real / route registered")
			Expect(counterValue(m.Registry(), "/", http.MethodGet, "404")).
				To(Equal(0.0), "unmatched 404s must NOT leak into the / route bucket")
		})

		It("buckets a handler-emitted fiber.ErrNotFound on the registered route template, not unmatched", func() {
			// isRouterNotFound must distinguish the framework's catchall
			// 404 (fresh *fiber.Error with message "Cannot METHOD /path")
			// from a handler returning the package singleton fiber.ErrNotFound.
			// The first should bucket as unmatched; the second should keep
			// its route template label so the route dimension survives.
			m, app := newTestApp()
			app.Get("/_test/notfound", func(c *fiber.Ctx) error {
				return fiber.ErrNotFound
			})

			req, err := http.NewRequestWithContext(
				context.Background(), http.MethodGet, "/_test/notfound", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNotFound))

			Expect(counterValue(m.Registry(), "/_test/notfound", http.MethodGet, "404")).
				To(Equal(1.0), "handler-emitted fiber.ErrNotFound must bucket on its registered route, not unmatched")
			Expect(counterValue(m.Registry(), "unmatched", http.MethodGet, "404")).
				To(Equal(0.0), "handler-returned fiber.ErrNotFound must not collapse into the unmatched sentinel")
		})

		It("buckets a wrapped fiber.ErrNotFound on the registered route template", func() {
			// Same contract as the previous spec, exercised through an
			// errors.Wrap chain. errors.Is unwraps until it finds the
			// singleton, so wrapped not-founds are also identified as
			// handler-emitted and keep their route dimension.
			m, app := newTestApp()
			app.Get("/_test/wrapped-notfound", func(c *fiber.Ctx) error {
				return fmt.Errorf("session lookup: %w", fiber.ErrNotFound)
			})

			req, err := http.NewRequestWithContext(
				context.Background(), http.MethodGet, "/_test/wrapped-notfound", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNotFound))

			Expect(counterValue(m.Registry(), "/_test/wrapped-notfound", http.MethodGet, "404")).
				To(Equal(1.0), "wrapped fiber.ErrNotFound must bucket on its registered route, not unmatched")
		})
	})

	Describe("Handler() lifecycle", func() {
		It("can be called repeatedly without panicking (regression: promhttp.HandlerFor double-MustRegister)", func() {
			// promhttp.HandlerFor with a non-nil Registry calls
			// MustRegister(...) on the registry to instrument the scrape
			// handler itself. Pre-fix this method built a fresh handler
			// per call, so a second call would panic. The fix caches the
			// handler at NewMetrics time; this spec guards the behavior.
			m := NewMetrics()
			Expect(func() { _ = m.Handler() }).NotTo(Panic())
			Expect(func() { _ = m.Handler() }).NotTo(Panic())
			Expect(func() { _ = m.Handler() }).NotTo(Panic())
		})
	})

	Describe("/ping happy path", func() {
		It("increments the templated /ping row with status 200", func() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/ping", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			// Sanity: the duration histogram also got an observation. We
			// don't assert on bucket boundaries — Prometheus' DefBuckets
			// can shift and the test would become flaky — only that the
			// histogram for this (route, method) was touched.
			body, err := metricsScrape(server)
			Expect(err).NotTo(HaveOccurred())
			Expect(body).To(ContainSubstring(`tapes_apiserver_request_duration_seconds_count{method="GET",route="/ping"}`))

			Expect(counterValue(server.metrics.Registry(),
				"/ping", http.MethodGet, "200")).To(Equal(1.0))
		})
	})
})

// metricsScrape pulls the /metrics body once. Centralised so the assertions
// above don't each have to repeat the request boilerplate.
func metricsScrape(s *Server) (string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	if err != nil {
		return "", err
	}
	resp, err := s.app.Test(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	// Trim trailing newline noise so callers can ContainSubstring cleanly.
	return strings.TrimRight(string(b), "\n"), nil
}

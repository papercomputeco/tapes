package api

import (
	"context"
	"encoding/json"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// parkedSpanModel is the driver behind the guard specs. Every span read
// parks in GetSpanRecord until the spec releases it or the request context
// ends, and reports which of the two happened; the sessions list answers
// empty at once. The embedded nil interfaces satisfy the capability
// checks, so a handler reaching for anything else nil-panics here.
type parkedSpanModel struct {
	storage.Driver
	sessionsReader
	storage.SpanModelReader

	release  chan struct{}
	observed chan error
}

func newParkedSpanModel() *parkedSpanModel {
	return &parkedSpanModel{
		Driver:   inmemory.NewDriver(),
		release:  make(chan struct{}),
		observed: make(chan error, 16),
	}
}

func (d *parkedSpanModel) GetSpanRecord(ctx context.Context, _, traceID, spanID string) (*storage.SpanRecord, error) {
	select {
	case <-d.release:
	case <-ctx.Done():
	}
	d.observed <- ctx.Err()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &storage.SpanRecord{TraceID: traceID, SpanID: spanID, Kind: "tool", Name: "Read", Status: "completed"}, nil
}

func (d *parkedSpanModel) ListSessionRecords(context.Context, string, storage.SessionListOpts) ([]storage.SessionRecord, error) {
	return nil, nil
}

const parkedSpanPath = "/v1/traces/trc-0001/spans/span-0001"

func newGuardedServer(driver storage.Driver, config Config) *Server {
	config.ListenAddr = ":0"
	server, err := NewServer(config, driver, tapeslogger.NewNoop())
	Expect(err).NotTo(HaveOccurred())
	return server
}

// reply is what one request came back with, its body read and closed.
type reply struct {
	status int
	header http.Header
	body   []byte
}

// get issues one request through app.Test with a timeout long enough that
// a hang shows up as a failed spec rather than a passing slow one.
func get(server *Server, path string) reply {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	response, err := server.app.Test(request, fiber.TestConfig{Timeout: 5 * time.Second})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	body, err := io.ReadAll(response.Body)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, response.Body.Close()).To(Succeed())
	return reply{status: response.StatusCode, header: response.Header, body: body}
}

// payloadInflight reads tapes_apiserver_payload_reads_inflight off the
// server's registry, the same way a scrape would.
func payloadInflight(server *Server) float64 {
	families, err := server.metrics.Registry().Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, family := range families {
		if family.GetName() == "tapes_apiserver_payload_reads_inflight" {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	Fail("tapes_apiserver_payload_reads_inflight is not registered")
	return 0
}

// holdSlots parks n payload reads in the driver and waits until every one
// of them holds a slot. The returned channel yields their status codes once
// the driver is released.
func holdSlots(server *Server, n int) <-chan int {
	results := make(chan int, n)
	for range n {
		go func() {
			defer GinkgoRecover()
			results <- get(server, parkedSpanPath).status
		}()
	}
	Eventually(func() float64 { return payloadInflight(server) }).Should(Equal(float64(n)),
		"every parked read holds a slot")
	return results
}

var _ = Describe("read guards", func() {
	It("cancels storage at the read deadline", func() {
		driver := newParkedSpanModel() // never released: only the deadline can end the read
		server := newGuardedServer(driver, Config{ReadDeadline: 50 * time.Millisecond})

		started := time.Now()
		got := get(server, parkedSpanPath)
		Expect(time.Since(started)).To(BeNumerically("<", time.Second), "the read must not hang")
		Expect(got.status).To(Equal(http.StatusInternalServerError), string(got.body))
		Expect(driver.observed).To(Receive(MatchError(context.DeadlineExceeded)),
			"storage saw the deadline, not a client hang-up or a plain cancel")
	})

	It("sheds the fifth concurrent payload read with 503 and Retry-After", func() {
		driver := newParkedSpanModel()
		server := newGuardedServer(driver, Config{PayloadConcurrency: 4})
		held := holdSlots(server, 4)

		got := get(server, parkedSpanPath)
		Expect(got.status).To(Equal(http.StatusServiceUnavailable), string(got.body))
		Expect(got.header.Get(fiber.HeaderRetryAfter)).To(Equal("1"))
		Expect(string(got.body)).To(MatchJSON(`{"error":"too many concurrent payload reads"}`))
		Expect(payloadInflight(server)).To(Equal(4.0), "a shed read holds nothing")

		close(driver.release)
		for range 4 {
			Eventually(held).Should(Receive(Equal(http.StatusOK)))
		}
		Eventually(func() float64 { return payloadInflight(server) }).Should(BeZero())

		got = get(server, parkedSpanPath)
		Expect(got.status).To(Equal(http.StatusOK), string(got.body))
	})

	It("does not cap non-payload routes", func() {
		driver := newParkedSpanModel()
		server := newGuardedServer(driver, Config{PayloadConcurrency: 4})
		held := holdSlots(server, 4)
		DeferCleanup(func() {
			close(driver.release)
			for range 4 {
				Eventually(held).Should(Receive())
			}
		})

		got := get(server, "/v1/sessions")
		Expect(got.status).To(Equal(http.StatusOK), string(got.body))

		got = get(server, "/v1/traces?session_id=0f0e0d0c-0b0a-4908-8706-050403020100")
		Expect(got.status).NotTo(Equal(http.StatusServiceUnavailable), string(got.body))
	})

	It("does not cap preview reads", func() {
		// Stored previews make a preview read payload-free, so the cap has
		// nothing to bound there: with every slot held, preview reads of
		// both payload routes still go through.
		driver := newParkedSpanModel()
		server := newGuardedServer(driver, Config{PayloadConcurrency: 4})
		held := holdSlots(server, 4)
		DeferCleanup(func() {
			close(driver.release)
			for range 4 {
				Eventually(held).Should(Receive())
			}
		})

		got := get(server, "/v1/sessions/0f0e0d0c-0b0a-4908-8706-050403020100/traces?payload=preview")
		Expect(got.status).NotTo(Equal(http.StatusServiceUnavailable), string(got.body))

		got = get(server, "/v1/traces/trc_1?payload=preview")
		Expect(got.status).NotTo(Equal(http.StatusServiceUnavailable), string(got.body))
	})

	It("caps a preview-flagged span drill-in", func() {
		// The span route serves the full payload whatever the query says,
		// so the preview exemption must not reach it.
		driver := newParkedSpanModel()
		server := newGuardedServer(driver, Config{PayloadConcurrency: 4})
		held := holdSlots(server, 4)
		DeferCleanup(func() {
			close(driver.release)
			for range 4 {
				Eventually(held).Should(Receive())
			}
		})

		got := get(server, parkedSpanPath+"?payload=preview")
		Expect(got.status).To(Equal(http.StatusServiceUnavailable), string(got.body))
	})

	It("does not apply the deadline to admin jobs", func() {
		server := newGuardedServer(newParkedSpanModel(), Config{ReadDeadline: time.Second})
		deadlines := map[string]bool{}
		probe := func(c fiber.Ctx) error {
			_, deadlines[strings.Clone(c.Path())] = c.Context().Deadline()
			return c.SendStatus(http.StatusNoContent)
		}
		server.app.Get(adminRoutePrefix+"probe", probe)
		server.app.Get("/v1/admin-probe", probe)

		Expect(get(server, adminRoutePrefix+"probe").status).To(Equal(http.StatusNoContent))
		Expect(get(server, "/v1/admin-probe").status).To(Equal(http.StatusNoContent))
		Expect(deadlines).To(Equal(map[string]bool{
			adminRoutePrefix + "probe": false,
			"/v1/admin-probe":          true,
		}), "only the admin mount is exempt; a sibling path is not")
	})

	It("keeps the request logger in the deadline context", func() {
		server := newGuardedServer(newParkedSpanModel(), Config{ReadDeadline: time.Second})

		var (
			requestID   string
			log         *slog.Logger
			deadline    time.Time
			hasDeadline bool
		)
		server.app.Get("/guard-probe", func(c fiber.Ctx) error {
			requestID = tapeslogger.RequestIDFromContext(c.Context())
			log = tapeslogger.RequestLoggerFromContext(c.Context())
			deadline, hasDeadline = c.Context().Deadline()
			return c.SendStatus(http.StatusNoContent)
		})

		got := get(server, "/guard-probe")
		Expect(got.status).To(Equal(http.StatusNoContent))
		Expect(requestID).To(Equal(got.header.Get(requestIDHeader)))
		Expect(log).NotTo(BeNil())
		Expect(hasDeadline).To(BeTrue(), "the deadline is on the same context as the logger")
		Expect(deadline).To(BeTemporally("~", time.Now().Add(time.Second), 500*time.Millisecond))
	})

	It("does not apply the deadline to the MCP route", func() {
		server := newGuardedServer(newParkedSpanModel(), Config{ReadDeadline: time.Second})

		// The MCP mount itself is a registered All route, so the probe sits
		// beneath it, where the exemption must still hold; a sibling route
		// off the mount shows the deadline is otherwise in force.
		deadlines := map[string]bool{}
		probe := func(c fiber.Ctx) error {
			// c.Path() views the request buffer, which fasthttp recycles;
			// a map key must be a copy (see methodLabel in metrics.go).
			_, deadlines[strings.Clone(c.Path())] = c.Context().Deadline()
			return c.SendStatus(http.StatusNoContent)
		}
		server.app.Get(mcpRoutePath+"/probe", probe)
		server.app.Get("/v1/mcp-probe", probe)

		Expect(get(server, mcpRoutePath+"/probe").status).To(Equal(http.StatusNoContent))
		Expect(get(server, "/v1/mcp-probe").status).To(Equal(http.StatusNoContent))
		Expect(deadlines).To(Equal(map[string]bool{
			mcpRoutePath + "/probe": false,
			"/v1/mcp-probe":         true,
		}), "only the MCP mount is exempt; a sibling path is not")

		// The mount's own path, exactly, through the same middleware.
		app := fiber.New()
		app.Use(newReadGuards(time.Second, 0, prometheus.NewRegistry()).deadlineMiddleware())
		app.All(mcpRoutePath, probe)
		request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, mcpRoutePath, nil)
		response, err := app.Test(request)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Body.Close()).To(Succeed())
		Expect(deadlines).To(HaveKeyWithValue(mcpRoutePath, false))
	})

	It("releases the slot when a streamed response ends", func() {
		driver := newPagedSpanModel(5, 4)
		// The gauge is sampled from inside the reader, mid-stream, which is
		// the only moment a streamed read provably holds its slot.
		var during atomic.Int64
		var server *Server
		spans := driver.spans
		driver.iterate = func(_ context.Context, traceID string, _ storage.SpanCursor) iter.Seq2[storage.SpanRecord, error] {
			return func(yield func(storage.SpanRecord, error) bool) {
				during.Store(int64(payloadInflight(server)))
				for _, sp := range spans[traceID] {
					if !yield(sp, nil) {
						return
					}
				}
			}
		}
		server = newGuardedServer(driver, Config{ReadDeadline: 5 * time.Second, PayloadConcurrency: 4})

		want, err := json.Marshal(driver.reference(PayloadFull))
		Expect(err).NotTo(HaveOccurred())
		got := get(server, "/v1/sessions/"+pagedSessionID+"/traces")
		Expect(got.status).To(Equal(http.StatusOK), string(got.body))
		Expect(string(got.body)).To(Equal(string(want)),
			"the handler returning must not cancel the stream's context: the page must land whole")
		Expect(during.Load()).To(Equal(int64(1)), "the stream held its slot while it was written")
		Eventually(func() float64 { return payloadInflight(server) }).Should(BeZero(),
			"the slot is given back once the stream has been written")

		// The standalone trace page streams the same way under the other
		// prefix, and gives its slot back the same way.
		during.Store(0)
		got = get(server, "/v1/traces/"+driver.turns[0].TraceID)
		Expect(got.status).To(Equal(http.StatusOK), string(got.body))
		Expect(json.Valid(got.body)).To(BeTrue(), string(got.body))
		Expect(during.Load()).To(Equal(int64(1)), "the trace page held its slot while it was written")
		Eventually(func() float64 { return payloadInflight(server) }).Should(BeZero())
	})

	It("cuts a stream that outlives the deadline and frees its slot", func() {
		driver := newPagedSpanModel(1, 0)
		driver.turns[0].SpanCount = 1 << 30
		// An endless trace in the composite, which pages by trace and so
		// never closes on a span count: only the deadline can end this
		// stream.
		exited := make(chan struct{})
		driver.iterate = func(ctx context.Context, traceID string, _ storage.SpanCursor) iter.Seq2[storage.SpanRecord, error] {
			return func(yield func(storage.SpanRecord, error) bool) {
				defer close(exited)
				for i := 0; ; i++ {
					if err := ctx.Err(); err != nil {
						yield(storage.SpanRecord{}, err)
						return
					}
					if !yield(fixtureSpan(traceID, i, fixtureEpoch, strings.Repeat("x", 1024)), nil) {
						return
					}
				}
			}
		}
		server := newGuardedServer(driver, Config{ReadDeadline: 100 * time.Millisecond, PayloadConcurrency: 4})
		handler := server.app.Handler()

		baseline := runtime.NumGoroutine()
		request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+pagedSessionID+"/traces", nil)
		response, done := streamRequest(handler, request)
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		Expect(payloadInflight(server)).To(Equal(1.0), "the stream holds its slot while it runs")

		// Draining is what a client does; the body must end on its own.
		started := time.Now()
		body, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(time.Since(started)).To(BeNumerically("<", 5*time.Second))
		done()

		Eventually(exited).Should(BeClosed(), "the reader was released")
		Expect(json.Valid(slices.Clip(body))).To(BeFalse(),
			"a cut stream is a truncated document, not a complete one")
		Eventually(func() float64 { return payloadInflight(server) }).Should(BeZero(),
			"the slot is given back once the cut stream has been written")
		Eventually(runtime.NumGoroutine, 5*time.Second).Should(BeNumerically("<=", baseline),
			"neither the writer goroutine nor a deadline timer outlives the request")
	})
})

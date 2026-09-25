package api

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Read guards: the deadline every request runs under and the concurrency
// cap on the routes that carry span payloads.
//
// Neither existed before. The fasthttp request context cancels only on
// server shutdown, so a read the gateway had already answered with a 504
// kept running to completion while the client's retry started another; and
// nothing bounded how many payload reads one replica would run at once, so
// a burst of session opens was a burst of multi-megabyte encodes competing
// for the same heap. The deadline turns an abandoned read into a cancelled
// pgx query; the cap turns the burst into a fast 503 the client retries a
// second later, against a replica that is still standing.
//
// What makes both guards awkward is the streamed response. The composite
// and trace pages commit their status and then hand the body to a writer
// goroutine that runs after the handler has returned — the handler abandons
// its Fiber context (c.Abandon) and the goroutine keeps only the user
// context. For such a response the guard cannot release when the handler
// returns: cancelling the deadline there would cut every stream at its
// first span, and freeing the slot there would cap nothing. The release has
// to wait for the stream's end, and no handler here reports that.
//
// fasthttp does. A request local (c.Locals, which is a fasthttp user value)
// that implements io.Closer is closed by fasthttp once it has finished
// writing the response — after the body stream has been drained to its EOF
// or the write to the client has failed, and before the connection serves
// its next request. Each guard therefore stores what it holds as a lease
// under a request local: a handler that returned normally gives the lease
// back on the spot, and an abandoned one leaves it to fasthttp, which gives
// it back when the stream is done. The lease is idempotent, so the two
// paths never double-release.
//
// The deadline's timer needs neither path to fire: context.WithTimeout
// cancels the context on its own when the deadline elapses, and a stream
// that outlives it sees ctx.Done in its next storage read or ctx check,
// closes short, and reaches the release that way (see the page writers'
// error handling: a cut stream is a truncated document, not an error
// response). The cancel the lease holds is what stops that timer when the
// stream ends first, so no timer outlives its request.

// payloadRoutePrefixes are the routes whose responses can carry span
// payloads, as Fiber prefix (Use) patterns; on them only full-payload
// reads take a slot. `/v1/traces/:trace_id` covers
// the span drill-in beneath it by prefix, which is why the list has two
// entries for three routes — and why the span route is never capped twice.
// `/v1/traces` itself (the summary list) is payload-free and outside it.
var payloadRoutePrefixes = []string{
	"/v1/sessions/:id/traces",
	"/v1/traces/:trace_id",
}

// mcpRoutePath is the streamable MCP transport's mount (openapi_routes.go
// registers it with the same literal). It is the one route the deadline
// leaves alone: the MCP server runs cassette tool calls under its own
// 30-second timeout, and a 20-second read deadline underneath it would cut
// long tool calls short of the limit they were promised. The cassette proxy
// needs no exemption — it never reads c.Context().
const mcpRoutePath = "/v1/mcp"

// adminRoutePrefix covers the operator jobs (POST /v1/admin/...: the demo
// seed, the whole-corpus derive run, attribution repair). They are writes
// that run for as long as the corpus takes, not reads a client is waiting
// on, so the read deadline leaves them to their own bounds.
const adminRoutePrefix = "/v1/admin/"

// spanRouteSegment marks the span drill-in beneath the trace page. That
// route has no preview mode — it always serves one span's full payload —
// so a payload=preview flag on it changes nothing and must not skip the
// cap.
const spanRouteSegment = "/spans/"

// deadlineExempt reports whether the request is outside the deadline's
// remit: the MCP mount and anything beneath it.
func deadlineExempt(c fiber.Ctx) bool {
	path := c.Path()
	return path == mcpRoutePath || strings.HasPrefix(path, mcpRoutePath+"/") ||
		strings.HasPrefix(path, adminRoutePrefix)
}

// readGuards owns the two guards' state: the deadline, the payload slots,
// and the gauge that reports how many payload reads are in flight.
type readGuards struct {
	deadline time.Duration
	// slots is the payload semaphore; nil when the cap is disabled. A read
	// holds one slot for as long as its response is being produced.
	slots    chan struct{}
	inflight prometheus.Gauge
}

// newReadGuards builds the guards and registers their gauge. A deadline of
// zero disables the deadline; a concurrency of zero disables the cap. The
// gauge is registered and maintained either way, so its absence can never
// be mistaken for an idle replica.
func newReadGuards(deadline time.Duration, concurrency int, reg prometheus.Registerer) *readGuards {
	g := &readGuards{
		deadline: deadline,
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tapes_apiserver_payload_reads_inflight",
			Help: "Payload-bearing reads (session traces, trace pages, span drill-ins) currently in flight on this replica.",
		}),
	}
	if concurrency > 0 {
		g.slots = make(chan struct{}, concurrency)
	}
	reg.MustRegister(g.inflight)
	return g
}

// lease is what one request holds against a guard, and the one-shot that
// gives it back. It is stored as a request local so fasthttp closes it once
// the response — body stream included — has been written; a handler that
// returned without abandoning its context gives it back itself, earlier.
type lease struct {
	once sync.Once
	give func()
}

// Close is the io.Closer fasthttp calls when it removes the request's
// locals. It never fails; the error is the interface's, not ours.
func (l *lease) Close() error {
	l.giveBack()
	return nil
}

func (l *lease) giveBack() { l.once.Do(l.give) }

// fasthttp only closes a request local it can assert to io.Closer, so the
// release path for an abandoned stream exists only while this holds.
var _ io.Closer = (*lease)(nil)

// deadlineLeaseKey and payloadLeaseKey are the request-local keys the two
// leases live under. Unexported types: nothing else can read or replace them.
type (
	deadlineLeaseKey struct{}
	payloadLeaseKey  struct{}
)

// deadlineMiddleware installs the read deadline on the request's user
// context. It must run behind requestIDMiddleware: that middleware seeds
// the user context with the request logger, and this one derives from it,
// so a handler's c.Context() carries both the logger and the deadline. The
// MCP route and the admin jobs pass through untouched (see deadlineExempt).
func (g *readGuards) deadlineMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		if deadlineExempt(c) {
			return c.Next()
		}
		ctx, cancel := context.WithTimeout(c.Context(), g.deadline)
		c.SetContext(ctx)
		held := &lease{give: cancel}
		c.Locals(deadlineLeaseKey{}, held)

		err := c.Next()

		// An abandoned context means a writer goroutine still holds ctx;
		// cancelling here would cut its stream at the first span. fasthttp
		// closes the lease when that stream ends, and the timer cancels
		// on its own if the stream outlives the deadline first.
		if !c.IsAbandoned() {
			held.giveBack()
		}
		return err
	}
}

// payloadMiddleware takes a payload slot for the request, or sheds it. The
// acquire never blocks: a replica at its cap answers 503 with Retry-After
// at once rather than queueing reads behind the ones already hogging the
// heap, which is the failure the cap exists to prevent. The in-flight gauge
// tracks payload reads whether or not the cap is enabled.
func (g *readGuards) payloadMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		// A preview read selects the stored preview columns and never the
		// payload ones, so it is not the work the cap exists to bound: it
		// passes uncounted, and the console's batched preview scans never
		// contend with the one full read an expanded turn needs. The span
		// drill-in has no preview mode, so the flag buys it nothing.
		if payloadModeFromQuery(c.Query("payload")) == PayloadPreview &&
			!strings.Contains(c.Path(), spanRouteSegment) {
			return c.Next()
		}
		if g.slots != nil {
			select {
			case g.slots <- struct{}{}:
			default:
				c.Set(fiber.HeaderRetryAfter, "1")
				return c.Status(fiber.StatusServiceUnavailable).
					JSON(llm.ErrorResponse{Error: "too many concurrent payload reads"})
			}
		}
		g.inflight.Inc()
		held := &lease{give: func() {
			g.inflight.Dec()
			if g.slots != nil {
				<-g.slots
			}
		}}
		c.Locals(payloadLeaseKey{}, held)

		err := c.Next()

		// Same rule as the deadline: a streamed response holds its slot
		// until fasthttp has written the stream and closes the lease.
		if !c.IsAbandoned() {
			held.giveBack()
		}
		return err
	}
}

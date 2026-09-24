package api

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/valyala/fasthttp"

	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// pagedSpanModel is the driver behind these specs: one session, its turn
// headers and links held whole (as the handler loads them), and spans
// served one trace at a time. The embedded nil interfaces satisfy the
// capability checks; only the methods the page path calls are real, so a
// handler that reached for anything else would nil-panic here rather than
// quietly materialize.
type pagedSpanModel struct {
	storage.Driver
	sessionsReader
	storage.SpanModelReader

	session storage.SessionRecord
	turns   []storage.TraceSummaryRecord
	links   []storage.SpanLinkRecord
	spans   map[string][]storage.SpanRecord
	// iterate, when set, replaces spans with a lazy per-trace generator —
	// how a driver that never holds a whole trace looks from the handler.
	iterate func(ctx context.Context, traceID string) iter.Seq2[storage.SpanRecord, error]
}

func (d *pagedSpanModel) GetSessionRecord(_ context.Context, _, id string) (*storage.SessionRecord, error) {
	if id != d.session.ID {
		return nil, nil
	}
	sess := d.session
	return &sess, nil
}

func (d *pagedSpanModel) ListTraceSummaries(_ context.Context, sessionID string) ([]storage.TraceSummaryRecord, error) {
	if sessionID != d.session.ID {
		return nil, nil
	}
	return append([]storage.TraceSummaryRecord(nil), d.turns...), nil
}

func (d *pagedSpanModel) ListSessionLinks(_ context.Context, sessionID string) ([]storage.SpanLinkRecord, error) {
	if sessionID != d.session.ID {
		return nil, nil
	}
	return append([]storage.SpanLinkRecord(nil), d.links...), nil
}

// IterateTraceSpans serves the trace's spans the way the postgres driver
// does for the mode: a preview read never selects the payload columns, so
// in preview mode the record reaches the handler with Input and Output
// nil and only the stored previews populated.
func (d *pagedSpanModel) IterateTraceSpans(ctx context.Context, _, traceID string, after storage.SpanCursor, mode storage.PayloadMode) iter.Seq2[storage.SpanRecord, error] {
	if d.iterate != nil {
		return d.iterate(ctx, traceID)
	}
	return func(yield func(storage.SpanRecord, error) bool) {
		for _, sp := range d.spans[traceID] {
			if !after.IsZero() && (sp.Seq < after.Seq || (sp.Seq == after.Seq && sp.SpanID <= after.SpanID)) {
				continue
			}
			if err := ctx.Err(); err != nil {
				yield(storage.SpanRecord{}, err)
				return
			}
			if !yield(recordForMode(sp, mode), nil) {
				return
			}
		}
	}
}

// GetTraceDetail serves one trace whole, with the same per-mode column
// discipline as IterateTraceSpans. Links are the ones touching the trace.
func (d *pagedSpanModel) GetTraceDetail(_ context.Context, _, traceID string, mode storage.PayloadMode) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	for _, t := range d.turns {
		if t.TraceID != traceID {
			continue
		}
		turn := t.SpanTurnRecord
		spans := make([]storage.SpanRecord, 0, len(d.spans[traceID]))
		for _, sp := range d.spans[traceID] {
			spans = append(spans, recordForMode(sp, mode))
		}
		var links []storage.SpanLinkRecord
		for _, l := range d.links {
			if l.FromTraceID == traceID || l.ToTraceID == traceID {
				links = append(links, l)
			}
		}
		return &turn, spans, links, nil
	}
	return nil, nil, nil, nil
}

// recordForMode strips what the mode's select list would not have read.
func recordForMode(sp storage.SpanRecord, mode storage.PayloadMode) storage.SpanRecord {
	if mode == storage.PayloadPreview {
		sp.Input, sp.Output = nil, nil
	}
	return sp
}

const pagedSessionID = "0f0e0d0c-0b0a-4908-8706-050403020100"

var fixtureEpoch = time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)

// newPagedSpanModel builds a session of traceCount turns. Trace ids run
// in the opposite direction to started_at on purpose: the composite emits
// traces in turn order, spans sort by trace id, and a page that confused
// the two would show up here as a reordered wire. Every third turn has no
// spans, the shape a synthetic opener leaves behind.
func newPagedSpanModel(traceCount, spansPerTrace int) *pagedSpanModel {
	ended := fixtureEpoch.Add(time.Hour)
	d := &pagedSpanModel{
		Driver: inmemory.NewDriver(),
		session: storage.SessionRecord{
			ID:               pagedSessionID,
			HarnessID:        "claude-code",
			HarnessSessionID: "harness-1",
			Name:             "paged session",
			StartedAt:        fixtureEpoch,
			LastSeenAt:       ended,
			EndedAt:          &ended,
			DerivedStatus:    "completed",
			TurnCount:        traceCount,
		},
		spans: map[string][]storage.SpanRecord{},
	}
	for i := range traceCount {
		traceID := fmt.Sprintf("trc-%04d", traceCount-1-i)
		startedAt := fixtureEpoch.Add(time.Duration(i) * time.Minute)
		count := spansPerTrace
		if i%3 == 2 {
			count = 0
		}
		d.turns = append(d.turns, storage.TraceSummaryRecord{
			SpanTurnRecord: storage.SpanTurnRecord{
				TraceID:    traceID,
				SessionID:  pagedSessionID,
				UserPrompt: fmt.Sprintf("prompt %d <&>", i),
				Status:     "completed",
				Source:     "wire",
				StartedAt:  startedAt,
				DurationNS: int64(time.Second),
			},
			SpanCount: count,
		})
		for j := range count {
			d.spans[traceID] = append(d.spans[traceID], fixtureSpan(traceID, j, startedAt, fmt.Sprintf("result %d/%d", i, j)))
		}
	}
	if traceCount > 1 {
		d.links = append(d.links, storage.SpanLinkRecord{
			Kind:        "compaction-seam",
			FromTraceID: d.turns[0].TraceID,
			FromSpanID:  "span-0000",
			FromIO:      "output",
			ToTraceID:   d.turns[1].TraceID,
			ToSpanID:    "span-0000",
			ToIO:        "input",
		})
	}
	return d
}

// fixtureSpan is one stored span with every payload column populated,
// including the html-escapable characters json.Marshal rewrites.
func fixtureSpan(traceID string, seq int, startedAt time.Time, text string) storage.SpanRecord {
	input := mustJSON([]map[string]string{{"type": "text", "text": "ask <" + text + ">"}})
	output := mustJSON([]map[string]string{{"type": "tool_output", "tool_output": text}})
	sp := storage.SpanRecord{
		TraceID:    traceID,
		SpanID:     fmt.Sprintf("span-%04d", seq),
		Kind:       "tool",
		Name:       "Read",
		Status:     "completed",
		CallKind:   "main",
		ThreadID:   "main",
		StartedAt:  startedAt.Add(time.Duration(seq) * time.Millisecond),
		DurationNS: int64(time.Millisecond),
		Seq:        int64(seq),
		Input:      input,
		Output:     output,
		Usage:      json.RawMessage(`{"input_tokens":3,"output_tokens":1}`),
		RawTurnID:  int64(seq + 1),
	}
	if seq%2 == 1 {
		sp.Verdict = json.RawMessage(`{"decision":"allow"}`)
	}
	return sp
}

// mustJSON encodes a fixture value; fixtures are built off the Ginkgo
// goroutine (inside a reader), where an assertion cannot run.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// reference is what the handler used to serve whole: BuildSessionTraces
// over the same rows, which stays the shape every page must match.
func (d *pagedSpanModel) reference(mode PayloadMode) *SessionTracesResponse {
	turns := make([]storage.SpanTurnRecord, 0, len(d.turns))
	spans := make([]storage.SpanRecord, 0, len(d.turns))
	for _, t := range d.turns {
		turns = append(turns, t.SpanTurnRecord)
		spans = append(spans, d.spans[t.TraceID]...)
	}
	return BuildSessionTraces(sessionItemFromStorage(d.session, time.Now()), turns, spans, d.links, mode)
}

func newPagedServer(driver *pagedSpanModel) *Server {
	server, err := NewServer(Config{ListenAddr: ":0"}, driver, logger.NewNoop())
	Expect(err).NotTo(HaveOccurred())
	return server
}

// getPage issues one request through app.Test, which is fine for every
// spec that only cares about the bytes that came back.
func getPage(server *Server, query string, header ...string) (*http.Response, []byte) {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions/"+pagedSessionID+"/traces"+query, nil)
	for i := 0; i+1 < len(header); i += 2 {
		request.Header.Set(header[i], header[i+1])
	}
	response, err := server.app.Test(request, fiber.TestConfig{Timeout: 30 * time.Second})
	Expect(err).NotTo(HaveOccurred())
	body, err := io.ReadAll(response.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(response.Body.Close()).To(Succeed())
	return response, body
}

func decodePage(body []byte) SessionTracesResponse {
	var page SessionTracesResponse
	ExpectWithOffset(1, json.Unmarshal(body, &page)).To(Succeed(), string(body))
	return page
}

// streamRequest serves req over an in-memory connection and returns a
// response whose body is read as the handler writes it. app.Test cannot
// do this: it collects the whole response into a buffer before returning
// it, which is exactly the materialization these specs measure. The
// returned func closes the connection and waits for the server side to
// finish with it.
func streamRequest(handler fasthttp.RequestHandler, req *http.Request) (*http.Response, func()) {
	clientConn, serverConn := net.Pipe()
	server := &fasthttp.Server{Handler: handler}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = server.ServeConn(serverConn)
	}()

	dump, err := httputil.DumpRequest(req, false)
	Expect(err).NotTo(HaveOccurred())
	// A pipe write completes only once the server has read it, so it
	// cannot share the goroutine that waits for the response.
	go func() { _, _ = clientConn.Write(dump) }()

	response, err := http.ReadResponse(bufio.NewReader(clientConn), req)
	Expect(err).NotTo(HaveOccurred())
	return response, func() {
		_ = clientConn.Close()
		<-served
	}
}

func heapAlloc() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

var _ = Describe("GET /v1/sessions/:id/traces pagination", func() {
	It("matches the materialized composite byte for byte", func() {
		// Full mode only: a full page embeds the stored payload bytes
		// verbatim, so the stream must reproduce json.Marshal of the
		// reference exactly. Preview mode serves stored previews, which
		// Postgres hands back JSONB-canonicalized, so byte identity is not
		// the contract there; preview_test.go compares it decoded.
		driver := newPagedSpanModel(5, 4)
		server := newPagedServer(driver)

		want, err := json.Marshal(driver.reference(PayloadFull))
		Expect(err).NotTo(HaveOccurred())

		response, body := getPage(server, "?payload=full")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		Expect(response.Header.Get(fiber.HeaderContentType)).To(Equal(fiber.MIMEApplicationJSON))
		Expect(string(body)).To(Equal(string(want)))
	})

	It("paginates traces with a continuing cursor", func() {
		driver := newPagedSpanModel(7, 3)
		server := newPagedServer(driver)
		want := driver.reference(PayloadFull)

		var walked []TraceDetail
		var pages int
		cursor := ""
		for {
			query := "?limit=2"
			if cursor != "" {
				query += "&cursor=" + cursor
			}
			response, body := getPage(server, query)
			Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
			page := decodePage(body)
			pages++

			// The envelope is whole on every page; only traces are paged.
			Expect(page.Schema).To(Equal(ProjectionSchema))
			Expect(page.Session.ID).To(Equal(want.Session.ID))
			Expect(page.Links).To(HaveLen(len(want.Links)))
			Expect(len(page.Traces)).To(BeNumerically("<=", 2))
			walked = append(walked, page.Traces...)

			if page.NextCursor == "" {
				break
			}
			Expect(pages).To(BeNumerically("<", 10), "the cursor never ran out")
			cursor = page.NextCursor
		}

		Expect(pages).To(Equal(4), "7 traces at 2 per page")
		got, err := json.Marshal(walked)
		Expect(err).NotTo(HaveOccurred())
		wantTraces, err := json.Marshal(want.Traces)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(wantTraces)),
			"the pages concatenated must be the unpaginated composite, in turn order")
	})

	It("rejects a cursor for another session", func() {
		driver := newPagedSpanModel(3, 1)
		server := newPagedServer(driver)

		foreign := encodeTracesPageCursor(tracesPageCursor{
			Session:   "ffffffff-ffff-4fff-8fff-ffffffffffff",
			TraceID:   driver.turns[0].TraceID,
			StartedAt: driver.turns[0].StartedAt,
		})
		response, body := getPage(server, "?cursor="+foreign)
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest), string(body))
		Expect(string(body)).To(ContainSubstring("cursor does not match session"))

		response, body = getPage(server, "?cursor=not-a-cursor")
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest), string(body))
		Expect(string(body)).To(ContainSubstring("invalid cursor"))
	})

	It("rejects a limit that is not a positive integer", func() {
		server := newPagedServer(newPagedSpanModel(1, 1))
		for _, bad := range []string{"0", "-1", "ten", "1.5"} {
			response, body := getPage(server, "?limit="+bad)
			Expect(response.StatusCode).To(Equal(http.StatusBadRequest), "limit=%s: %s", bad, body)
			Expect(string(body)).To(ContainSubstring("limit must be a positive integer"))
		}
	})

	It("clamps limit to 200", func() {
		server := newPagedServer(newPagedSpanModel(205, 0))
		response, body := getPage(server, "?limit=999")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		page := decodePage(body)
		Expect(page.Traces).To(HaveLen(200))
		Expect(page.NextCursor).NotTo(BeEmpty())

		response, body = getPage(server, "?limit=200&cursor="+page.NextCursor)
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		rest := decodePage(body)
		Expect(rest.Traces).To(HaveLen(5))
		Expect(rest.NextCursor).To(BeEmpty())
	})

	It("defaults to 50 traces", func() {
		server := newPagedServer(newPagedSpanModel(60, 0))
		response, body := getPage(server, "")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		page := decodePage(body)
		Expect(page.Traces).To(HaveLen(50))
		Expect(page.NextCursor).NotTo(BeEmpty())

		// A session that fits exactly in the default page has no next
		// page: absence of the cursor, not page length, is the end.
		exact := newPagedServer(newPagedSpanModel(50, 0))
		response, body = getPage(exact, "")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		Expect(decodePage(body).NextCursor).To(BeEmpty())
	})

	It("closes a page early once the byte budget is passed", func() {
		driver := newPagedSpanModel(4, 2)
		server := newPagedServer(driver)

		// Every trace is a few hundred bytes; with the default budget
		// they are one page.
		_, body := getPage(server, "")
		Expect(decodePage(body).NextCursor).To(BeEmpty())

		saved := tracesPageByteBudget
		tracesPageByteBudget = 1
		DeferCleanup(func() { tracesPageByteBudget = saved })

		// The budget is checked at trace boundaries, so the first trace
		// always lands whole and the page closes right after it. Every
		// page still carries the whole envelope, and the walk still ends
		// at the session's last trace.
		want := driver.reference(PayloadFull)
		var walked []TraceDetail
		cursor := ""
		for pages := 0; ; pages++ {
			Expect(pages).To(BeNumerically("<", 10))
			query := ""
			if cursor != "" {
				query = "?cursor=" + cursor
			}
			response, body := getPage(server, query)
			Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
			page := decodePage(body)
			Expect(page.Traces).To(HaveLen(1), "a page closes at the first trace boundary past the budget")
			Expect(page.Links).To(HaveLen(len(want.Links)))
			walked = append(walked, page.Traces...)
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		Expect(walked).To(HaveLen(len(want.Traces)))
		got, err := json.Marshal(walked)
		Expect(err).NotTo(HaveOccurred())
		wantTraces, err := json.Marshal(want.Traces)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(wantTraces)))
	})

	It("still compresses a streamed page", func() {
		driver := newPagedSpanModel(3, 3)
		server := newPagedServer(driver)

		_, plain := getPage(server, "")
		response, packed := getPage(server, "", fiber.HeaderAcceptEncoding, "gzip")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(packed))
		Expect(response.Header.Get(fiber.HeaderContentEncoding)).To(Equal("gzip"))

		reader, err := gzip.NewReader(bytes.NewReader(packed))
		Expect(err).NotTo(HaveOccurred())
		unpacked, err := io.ReadAll(reader)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(unpacked)).To(Equal(string(plain)))
	})

	It("keeps heap flat across a 10k-span session", func() {
		const spanCount = 10_000
		const spanBytes = 4096
		driver := newPagedSpanModel(1, 0)
		driver.turns[0].SpanCount = spanCount

		// Each span's payload is built when the row is yielded, so the
		// only way 10k of them can be resident at once is for the handler
		// to keep them. The heap is sampled from inside the reader — after
		// a GC, so garbage awaiting collection does not pass for growth.
		var peak int64
		driver.iterate = func(ctx context.Context, traceID string) iter.Seq2[storage.SpanRecord, error] {
			return func(yield func(storage.SpanRecord, error) bool) {
				for i := range spanCount {
					if i%500 == 0 {
						peak = max(peak, heapAlloc())
					}
					if err := ctx.Err(); err != nil {
						yield(storage.SpanRecord{}, err)
						return
					}
					sp := fixtureSpan(traceID, i, fixtureEpoch, strings.Repeat(fmt.Sprintf("%04d", i%10000), spanBytes/4))
					if !yield(sp, nil) {
						return
					}
				}
			}
		}
		server := newPagedServer(driver)
		handler := server.app.Handler()

		baseline := heapAlloc()
		request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+pagedSessionID+"/traces", nil)
		response, done := streamRequest(handler, request)
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		streamed, err := io.Copy(io.Discard, response.Body)
		Expect(err).NotTo(HaveOccurred())
		done()

		Expect(streamed).To(BeNumerically(">=", int64(spanCount*spanBytes)),
			"the whole session went over the wire")
		growth := peak - baseline
		GinkgoWriter.Printf("heap grew %d bytes while streaming %d bytes\n", growth, streamed)
		// A materialized composite would hold rows, items and the encoded
		// document — three copies of ~80 MiB. A stream holds one span, its
		// encoding and the writer's buffers: ~170 KiB measured. The bound
		// leaves room for runtime noise, not for a copy of the session.
		Expect(growth).To(BeNumerically("<", int64(1<<20)),
			"heap grew by %d bytes streaming %d bytes", growth, streamed)
	})

	It("stops writing when the context is done", func() {
		driver := newPagedSpanModel(1, 0)
		driver.turns[0].SpanCount = 1 << 30
		// An endless trace: only the context can end this stream.
		exited := make(chan struct{})
		driver.iterate = func(ctx context.Context, traceID string) iter.Seq2[storage.SpanRecord, error] {
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
		server := newPagedServer(driver)

		// The handler binds its stream to c.Context(); a middleware ahead
		// of it is how a caller (or a later deadline) supplies one that
		// can be cancelled.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		app := fiber.New()
		app.Use(func(c fiber.Ctx) error {
			c.SetContext(ctx)
			return c.Next()
		})
		app.Get("/v1/sessions/:id/traces", server.handleGetSessionTraces)
		handler := app.Handler()

		baseline := runtime.NumGoroutine()
		request := httptest.NewRequest(http.MethodGet, "/v1/sessions/"+pagedSessionID+"/traces", nil)
		response, done := streamRequest(handler, request)
		Expect(response.StatusCode).To(Equal(http.StatusOK))

		head := make([]byte, 64<<10)
		_, err := io.ReadFull(response.Body, head)
		Expect(err).NotTo(HaveOccurred(), "the stream was flowing before the cancel")
		cancel()

		// Draining is what a client does; the body must end rather than
		// keep coming.
		tail, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		done()

		Eventually(exited).Should(BeClosed(), "the reader was released")
		Expect(json.Valid(slices.Concat(head, tail))).To(BeFalse(),
			"a cut stream is a truncated document, not a complete one")
		Eventually(runtime.NumGoroutine, 5*time.Second).Should(BeNumerically("<=", baseline),
			"the writer goroutine must not outlive the request")
	})
})

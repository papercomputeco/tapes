package api

// GET /v1/traces/:trace_id pages spans the way the composite pages traces.
// These specs share the composite's fake (pagedSpanModel) and its
// streaming reader; the reference every page is held to is
// BuildTraceDetail over the same rows, which is what the handler used to
// serve whole and what `tapes dev trace-fixtures` still renders.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// traceReference is the materialized standalone detail for one of the
// model's traces: what a page that holds the whole trace must equal.
func (d *pagedSpanModel) traceReference(traceID string, mode PayloadMode) StandaloneTraceDetail {
	for _, t := range d.turns {
		if t.TraceID != traceID {
			continue
		}
		var links []storage.SpanLinkRecord
		for _, l := range d.links {
			if l.FromTraceID == traceID || l.ToTraceID == traceID {
				links = append(links, l)
			}
		}
		spans := make([]storage.SpanRecord, 0, len(d.spans[traceID]))
		for _, sp := range d.spans[traceID] {
			spans = append(spans, recordForMode(sp, mode))
		}
		return StandaloneTraceDetail{
			SessionID:   t.SessionID,
			TraceDetail: BuildTraceDetail(t.SpanTurnRecord, spans, links, mode),
		}
	}
	Fail("no such trace in the model: " + traceID)
	return StandaloneTraceDetail{}
}

func decodeTracePage(body []byte) StandaloneTraceDetail {
	var page StandaloneTraceDetail
	ExpectWithOffset(1, json.Unmarshal(body, &page)).To(Succeed(), string(body))
	return page
}

// getTracePage fetches one page of a trace, asserting it was served.
func getTracePage(server *Server, traceID, query string) StandaloneTraceDetail {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/traces/"+traceID+query, nil)
	response, err := server.app.Test(request, fiber.TestConfig{Timeout: 30 * time.Second})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ExpectWithOffset(1, response.StatusCode).To(Equal(http.StatusOK), string(body))
	ExpectWithOffset(1, response.Header.Get(fiber.HeaderContentType)).To(Equal(fiber.MIMEApplicationJSON))
	return decodeTracePage(body)
}

// walkTrace follows next_cursor from the first page to the last and
// returns every page in order. query is the per-request query string
// without a cursor; the walk appends its own.
func walkTrace(server *Server, traceID, query string) []StandaloneTraceDetail {
	var pages []StandaloneTraceDetail
	cursor := ""
	for {
		ExpectWithOffset(1, len(pages)).To(BeNumerically("<", 1000), "the cursor never ran out")
		q := query
		if cursor != "" {
			if q == "" {
				q = "?cursor=" + cursor
			} else {
				q += "&cursor=" + cursor
			}
		}
		page := getTracePage(server, traceID, q)
		pages = append(pages, page)
		if page.NextCursor == "" {
			return pages
		}
		cursor = page.NextCursor
	}
}

// concatSpans is the walk's spans in order — what must equal the
// unpaginated trace.
func concatSpans(pages []StandaloneTraceDetail) []SpanItem {
	var count int
	for _, p := range pages {
		count += len(p.Spans)
	}
	spans := make([]SpanItem, 0, count)
	for _, p := range pages {
		spans = append(spans, p.Spans...)
	}
	return spans
}

var _ = Describe("GET /v1/traces/:trace_id pagination", func() {
	It("matches the materialized trace detail byte for byte", func() {
		// Full mode only, as for the composite: a full page embeds the
		// stored payload bytes verbatim, so the stream must reproduce
		// json.Marshal of the reference exactly — including the links the
		// standalone detail carries and the ones it omits when empty.
		driver := newPagedSpanModel(3, 4)
		server := newPagedServer(driver)

		linked := driver.turns[0].TraceID
		want, err := json.Marshal(driver.traceReference(linked, PayloadFull))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(want)).To(ContainSubstring(`"links":[`), "the fixture's seam touches this trace")
		response, body := getTrace(server, linked, "?payload=full")
		Expect(response.StatusCode).To(Equal(http.StatusOK), string(body))
		Expect(response.Header.Get(fiber.HeaderContentType)).To(Equal(fiber.MIMEApplicationJSON))
		Expect(string(body)).To(Equal(string(want)))

		// An edge-less trace: TraceDetail omits empty links, and so must
		// the page.
		lone := newPagedSpanModel(1, 3)
		want, err = json.Marshal(lone.traceReference(lone.turns[0].TraceID, PayloadFull))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(want)).NotTo(ContainSubstring(`"links"`))
		_, body = getTrace(newPagedServer(lone), lone.turns[0].TraceID, "")
		Expect(string(body)).To(Equal(string(want)))
		Expect(string(body)).NotTo(ContainSubstring("next_cursor"))
	})

	It("pages spans by seq", func() {
		driver := newPagedSpanModel(2, 400)
		server := newPagedServer(driver)
		traceID := driver.turns[0].TraceID
		want := driver.traceReference(traceID, PayloadFull)
		Expect(want.Spans).To(HaveLen(400))

		pages := walkTrace(server, traceID, "?limit=50")
		Expect(pages).To(HaveLen(8), "400 spans at 50 per page")
		for i, page := range pages {
			// The envelope is whole on every page; only spans are paged.
			Expect(page.SessionID).To(Equal(want.SessionID))
			Expect(page.Schema).To(Equal(ProjectionSchema))
			Expect(page.Trace).To(Equal(want.Trace), "the header — span_count included — is the trace's, not the page's")
			Expect(page.Links).To(Equal(want.Links))
			Expect(page.Spans).To(HaveLen(50), "page %d", i)
			for j := 1; j < len(page.Spans); j++ {
				Expect(page.Spans[j].Seq).To(BeNumerically(">", page.Spans[j-1].Seq))
			}
		}
		Expect(pages[len(pages)-1].NextCursor).To(BeEmpty())

		got, err := json.Marshal(concatSpans(pages))
		Expect(err).NotTo(HaveOccurred())
		wantSpans, err := json.Marshal(want.Spans)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(wantSpans)),
			"the pages concatenated must be the unpaginated trace, in seq order")
	})

	It("defaults to 200 spans", func() {
		driver := newPagedSpanModel(1, 250)
		server := newPagedServer(driver)
		traceID := driver.turns[0].TraceID

		_, body := getTrace(server, traceID, "")
		page := decodeTracePage(body)
		Expect(page.Spans).To(HaveLen(200))
		Expect(page.NextCursor).NotTo(BeEmpty())

		_, body = getTrace(server, traceID, "?cursor="+page.NextCursor)
		rest := decodeTracePage(body)
		Expect(rest.Spans).To(HaveLen(50))
		Expect(rest.NextCursor).To(BeEmpty())
		Expect(rest.Spans[0].Seq).To(Equal(page.Spans[199].Seq + 1))

		// A trace that fits exactly in the default page has no next page:
		// absence of the cursor, not page length, is the end.
		exact := newPagedSpanModel(1, 200)
		_, body = getTrace(newPagedServer(exact), exact.turns[0].TraceID, "")
		Expect(decodeTracePage(body).NextCursor).To(BeEmpty())
	})

	It("clamps per-trace limit to 1000", func() {
		driver := newPagedSpanModel(1, 1005)
		server := newPagedServer(driver)
		traceID := driver.turns[0].TraceID

		_, body := getTrace(server, traceID, "?limit=9999")
		page := decodeTracePage(body)
		Expect(page.Spans).To(HaveLen(1000))
		Expect(page.NextCursor).NotTo(BeEmpty())

		_, body = getTrace(server, traceID, "?limit=1000&cursor="+page.NextCursor)
		rest := decodeTracePage(body)
		Expect(rest.Spans).To(HaveLen(5))
		Expect(rest.NextCursor).To(BeEmpty())
	})

	It("rejects a limit that is not a positive integer", func() {
		driver := newPagedSpanModel(1, 1)
		server := newPagedServer(driver)
		for _, bad := range []string{"0", "-1", "ten", "1.5"} {
			response, body := getTrace(server, driver.turns[0].TraceID, "?limit="+bad)
			Expect(response.StatusCode).To(Equal(http.StatusBadRequest), "limit=%s: %s", bad, body)
			Expect(string(body)).To(ContainSubstring("limit must be a positive integer"))
		}
	})

	It("rejects a cursor for another trace", func() {
		driver := newPagedSpanModel(2, 3)
		server := newPagedServer(driver)
		mine, other := driver.turns[0].TraceID, driver.turns[1].TraceID

		// A genuine cursor, minted by the other trace's walk.
		_, body := getTrace(server, other, "?limit=1")
		foreign := decodeTracePage(body).NextCursor
		Expect(foreign).NotTo(BeEmpty())

		response, body := getTrace(server, mine, "?cursor="+foreign)
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest), string(body))
		Expect(string(body)).To(ContainSubstring("cursor does not match trace"))

		response, body = getTrace(server, mine, "?cursor=not-a-cursor")
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest), string(body))
		Expect(string(body)).To(ContainSubstring("invalid cursor"))

		// Well-formed but naming no span: hand-crafted, not ours.
		hollow := encodeTracePageCursor(tracePageCursor{TraceID: mine})
		response, body = getTrace(server, mine, "?cursor="+hollow)
		Expect(response.StatusCode).To(Equal(http.StatusBadRequest), string(body))
		Expect(string(body)).To(ContainSubstring("invalid cursor"))
	})

	It("responds 404 before streaming for an unknown trace", func() {
		server := newPagedServer(newPagedSpanModel(1, 1))
		response, body := getTrace(server, "trc-nope", "")
		Expect(response.StatusCode).To(Equal(http.StatusNotFound), string(body))
		Expect(string(body)).To(ContainSubstring("trace not found"))
	})

	It("closes a trace page at the byte budget", func() {
		driver := newPagedSpanModel(2, 6)
		server := newPagedServer(driver)
		traceID := driver.turns[0].TraceID
		want := driver.traceReference(traceID, PayloadFull)

		// Every span is a few hundred bytes; with the default budget the
		// trace is one page.
		_, body := getTrace(server, traceID, "")
		Expect(decodeTracePage(body).NextCursor).To(BeEmpty())

		saved := tracesPageByteBudget
		tracesPageByteBudget = 1
		DeferCleanup(func() { tracesPageByteBudget = saved })

		// The budget is checked at span boundaries, so the first span
		// always lands whole and the page closes right after it. Every
		// page still carries the whole envelope, and the walk still ends
		// at the trace's last span.
		pages := walkTrace(server, traceID, "")
		Expect(pages).To(HaveLen(len(want.Spans)))
		for _, page := range pages {
			Expect(page.Spans).To(HaveLen(1), "a page closes at the first span boundary past the budget")
			Expect(page.Trace).To(Equal(want.Trace))
			Expect(page.Links).To(Equal(want.Links))
		}
		got, err := json.Marshal(concatSpans(pages))
		Expect(err).NotTo(HaveOccurred())
		wantSpans, err := json.Marshal(want.Spans)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(string(wantSpans)))
	})

	It("streams stored previews for trace detail", func() {
		// Decoded comparison: Postgres canonicalizes JSONB, so preview
		// bytes are not the contract. Rows with and without a stored
		// preview are mixed across pages so each is marked on its own row.
		driver := withStoredPreviews(newPagedSpanModel(2, 30))
		traceID := driver.turns[0].TraceID
		for i := range driver.spans[traceID] {
			if i%7 == 3 {
				driver.spans[traceID][i].InputPreview = nil
				driver.spans[traceID][i].OutputPreview = nil
				driver.spans[traceID][i].HasPreview = false
			}
		}
		server := newPagedServer(driver)
		want := driver.traceReference(traceID, PayloadPreview)

		pages := walkTrace(server, traceID, "?payload=preview&limit=10")
		Expect(pages).To(HaveLen(3), "30 spans at 10 per page")
		spans := concatSpans(pages)
		Expect(spans).To(HaveLen(30))
		var previewed, pending int
		for i, item := range spans {
			switch item.Payload {
			case "preview":
				previewed++
				Expect(decodeJSON(item.Input)).To(Equal(decodeJSON(storedPreview("input", item.SpanID))), item.SpanID)
				Expect(decodeJSON(item.Output)).To(Equal(decodeJSON(storedPreview("output", item.SpanID))), item.SpanID)
			case "preview_pending":
				pending++
				Expect(string(item.Input)).To(Equal("[]"))
				Expect(string(item.Output)).To(Equal("[]"))
			default:
				Fail(fmt.Sprintf("span %d carries no payload marker: %q", i, item.Payload))
			}
			// The payload never reaches the wire in preview mode.
			Expect(string(item.Input)).NotTo(ContainSubstring("ask "))
			Expect(string(item.Output)).NotTo(ContainSubstring("result "))
		}
		Expect(previewed).To(Equal(26))
		Expect(pending).To(Equal(4))

		got, err := json.Marshal(spans)
		Expect(err).NotTo(HaveOccurred())
		reference, err := json.Marshal(want.Spans)
		Expect(err).NotTo(HaveOccurred())
		Expect(decodeJSON(got)).To(Equal(decodeJSON(reference)),
			"the pages concatenated are what BuildTraceDetail renders in preview mode")
		Expect(pages[0].Trace).To(Equal(want.Trace))
		Expect(pages[0].Links).To(Equal(want.Links))
	})

	It("keeps heap flat across a 100k-span trace", func() {
		const spanCount = 100_000
		const spanBytes = 2048
		driver := newPagedSpanModel(1, 0)
		driver.turns[0].SpanCount = spanCount
		traceID := driver.turns[0].TraceID

		// Each span's payload is built when the row is yielded, so the
		// only way many of them can be resident at once is for the
		// handler to keep them. The heap is sampled from inside the
		// reader — after a GC, so garbage awaiting collection does not
		// pass for growth. Seq is unique in this fixture, so the cursor
		// resumes at seq+1.
		var peak int64
		driver.iterate = func(ctx context.Context, traceID string, after storage.SpanCursor) iter.Seq2[storage.SpanRecord, error] {
			return func(yield func(storage.SpanRecord, error) bool) {
				start := 0
				if !after.IsZero() {
					start = int(after.Seq) + 1
				}
				for i := start; i < spanCount; i++ {
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
		var streamed, served int64
		cursor := ""
		for pages := 0; ; pages++ {
			Expect(pages).To(BeNumerically("<", spanCount/maxTraceSpansLimit+1), "the cursor never ran out")
			query := "?limit=1000"
			if cursor != "" {
				query += "&cursor=" + cursor
			}
			request := httptest.NewRequest(http.MethodGet, "/v1/traces/"+traceID+query, nil)
			response, done := streamRequest(handler, request)
			Expect(response.StatusCode).To(Equal(http.StatusOK))
			// The body is drained, not held: only the trailing bytes are
			// kept, for the cursor.
			var tail tailBuffer
			n, err := io.Copy(&tail, response.Body)
			Expect(err).NotTo(HaveOccurred())
			done()
			streamed += n
			served += maxTraceSpansLimit
			cursor = tail.nextCursor()
			if cursor == "" {
				break
			}
		}

		Expect(served).To(BeNumerically(">=", int64(spanCount)))
		Expect(streamed).To(BeNumerically(">=", int64(spanCount*spanBytes)),
			"the whole trace went over the wire")
		growth := peak - baseline
		GinkgoWriter.Printf("heap grew %d bytes while streaming %d bytes\n", growth, streamed)
		// A materialized page would hold 1000 rows, their items and the
		// encoded document — several MiB. A stream holds one span, its
		// encoding and the writer's buffers. The bound leaves room for
		// runtime noise, not for a page.
		Expect(growth).To(BeNumerically("<", int64(1<<20)),
			"heap grew by %d bytes streaming %d bytes", growth, streamed)
	})

	It("stops writing when the context is done", func() {
		driver := newPagedSpanModel(1, 0)
		driver.turns[0].SpanCount = 1 << 30
		traceID := driver.turns[0].TraceID
		// An endless trace: only the context can end this stream.
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
		app.Get("/v1/traces/:trace_id", server.handleGetTrace)
		handler := app.Handler()

		baseline := runtime.NumGoroutine()
		request := httptest.NewRequest(http.MethodGet, "/v1/traces/"+traceID+"?limit=1000", nil)
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

// tailBuffer keeps only the last bytes written to it — enough to find
// next_cursor at the end of a page without holding the page.
type tailBuffer struct {
	tail []byte
}

const tailBufferKeep = 4096

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.tail = append(b.tail, p...)
	if len(b.tail) > tailBufferKeep {
		b.tail = append([]byte(nil), b.tail[len(b.tail)-tailBufferKeep:]...)
	}
	return len(p), nil
}

// nextCursor reads next_cursor off the page's tail, or "" when the page
// ended without one.
func (b *tailBuffer) nextCursor() string {
	const key = `"next_cursor":"`
	i := strings.LastIndex(string(b.tail), key)
	if i < 0 {
		return ""
	}
	rest := string(b.tail[i+len(key):])
	end := strings.IndexByte(rest, '"')
	ExpectWithOffset(1, end).To(BeNumerically(">", 0), "next_cursor is cut off: %q", rest)
	return rest[:end]
}

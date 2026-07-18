package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// pagingExportStubDriver is exportStubDriver plus a real, in-memory
// implementation of ListSessionRecords: keyset-paginated on
// (LastSeenAt DESC, ID DESC), honoring Since/Until/Limit exactly like the
// Postgres driver's contract, so it can stand in for a multi-page export.
type pagingExportStubDriver struct {
	exportStubDriver

	// all is every session across every org, newest-last-seen first is NOT
	// assumed; ListSessionRecords sorts on each call.
	all []storage.SessionRecord
	// orgOf maps session ID -> org ID, since SessionRecord itself carries
	// no org field (org-scoping lives in the driver, not the record).
	orgOf map[string]string

	listCalls  int
	listLimits []int
	listCtxs   []context.Context
}

func (d *pagingExportStubDriver) ListSessionRecords(ctx context.Context, orgID string, opts storage.SessionListOpts) ([]storage.SessionRecord, error) {
	d.listCalls++
	d.listLimits = append(d.listLimits, opts.Limit)
	d.listCtxs = append(d.listCtxs, ctx)

	var matched []storage.SessionRecord
	for _, s := range d.all {
		if d.orgOf[s.ID] != orgID {
			continue
		}
		if opts.Since != nil && s.LastSeenAt.Before(*opts.Since) {
			continue
		}
		if opts.Until != nil && !s.LastSeenAt.Before(*opts.Until) {
			continue
		}
		matched = append(matched, s)
	}

	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].LastSeenAt.Equal(matched[j].LastSeenAt) {
			return matched[i].LastSeenAt.After(matched[j].LastSeenAt)
		}
		return matched[i].ID > matched[j].ID
	})

	if opts.CursorVal != nil && opts.CursorID != nil {
		cursorTs, err := time.Parse(time.RFC3339Nano, *opts.CursorVal)
		if err != nil {
			return nil, fmt.Errorf("parse cursor value: %w", err)
		}
		start := len(matched)
		for i, s := range matched {
			if s.LastSeenAt.Before(cursorTs) ||
				(s.LastSeenAt.Equal(cursorTs) && s.ID < *opts.CursorID) {
				start = i
				break
			}
		}
		matched = matched[start:]
	}

	for i := range matched {
		matched[i].SortVal = matched[i].LastSeenAt.UTC().Format(time.RFC3339Nano)
	}

	limit := opts.Limit
	if limit <= 0 || limit > len(matched) {
		limit = len(matched)
	}
	return matched[:limit], nil
}

// newPagingDriver builds a driver with n sessions for org, spaced one
// minute apart ending at "latest", each with a single distinct turn so the
// export output can be counted/verified per session.
func newPagingDriver(org string, n int, latest time.Time) *pagingExportStubDriver {
	d := &pagingExportStubDriver{
		exportStubDriver: exportStubDriver{
			Driver:        inmemory.NewDriver(),
			sessionsByOrg: map[string]map[string]storage.SessionRecord{},
			summaries:     map[string][]storage.TraceSummaryRecord{},
		},
		orgOf: map[string]string{},
	}
	d.sessionsByOrg[org] = map[string]storage.SessionRecord{}

	for i := range n {
		id := fmt.Sprintf("session-%04d", i)
		seenAt := latest.Add(-time.Duration(i) * time.Minute)
		rec := storage.SessionRecord{
			ID:         id,
			HarnessID:  "claude",
			StartedAt:  seenAt,
			LastSeenAt: seenAt,
		}
		d.all = append(d.all, rec)
		d.orgOf[id] = org
		d.sessionsByOrg[org][id] = rec
		d.summaries[id] = []storage.TraceSummaryRecord{
			{SpanTurnRecord: storage.SpanTurnRecord{
				TraceID: id + "-t1", UserPrompt: "hi", ResponsePreview: "hello",
				StartedAt: seenAt, TotalInputTokens: 1, TotalOutputTokens: 1,
			}},
		}
	}
	return d
}

var _ = Describe("GET /v1/sessions/export", func() {
	const org = "33333333-3333-3333-3333-333333333333"
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	// T-7: default window (no since/until) + bypasses the 200-row UI cap.
	It("streams every session in the default 30-day window, past the 200-row cap", func() {
		drv := newPagingDriver(org, 250, now)
		server := newExportServer(drv)

		resp, body := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/x-ndjson"))

		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(250))

		// Must have paged internally: the driver's Postgres-mirroring
		// contract is capped at maxSessionsLimit per call, so covering 250
		// sessions requires more than one ListSessionRecords call.
		Expect(drv.listCalls).To(BeNumerically(">", 1))
		for _, l := range drv.listLimits {
			Expect(l).To(BeNumerically("<=", maxSessionsLimit+1))
		}
	})

	// T-8: since/until override the default window.
	It("honors since/until query params instead of the 30-day default", func() {
		drv := newPagingDriver(org, 10, now)
		server := newExportServer(drv)

		since := now.Add(-5 * time.Minute).Format(time.RFC3339)
		until := now.Add(-2 * time.Minute).Format(time.RFC3339)
		resp, body := getRaw(server, "/v1/sessions/export?since="+since+"&until="+until, org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

		// sessions at offsets 2,3,4 minutes back fall in [since, until);
		// the offset-5 session lands exactly on "since" and is included,
		// offset-2 (== until) is excluded (until is an exclusive upper
		// bound, matching handleListSessions' semantics).
		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(3))
	})

	// T-12 (R-20): since is clamped to a 30-day floor. A session older than
	// 30 days must never be returned, even when the caller explicitly
	// requests a since far in the past — the 30-day window is the maximum
	// span for v1, not just the default.
	It("clamps since to 30 days ago and excludes sessions older than that, even when since requests more history", func() {
		realNow := time.Now().UTC()
		drv := newPagingDriver(org, 0, realNow)
		// One session inside the 30-day floor (10 days old) and one older
		// than the floor (40 days old) — both would be included by a
		// naive unclamped since=1970-01-01, but only the in-window one
		// should survive the clamp.
		recentID := "recent-session"
		recentRec := storage.SessionRecord{ID: recentID, HarnessID: "claude", StartedAt: realNow.Add(-10 * 24 * time.Hour), LastSeenAt: realNow.Add(-10 * 24 * time.Hour)}
		drv.all = append(drv.all, recentRec)
		drv.orgOf[recentID] = org
		drv.sessionsByOrg[org][recentID] = recentRec
		drv.summaries[recentID] = []storage.TraceSummaryRecord{
			{SpanTurnRecord: storage.SpanTurnRecord{TraceID: "recent-t1", UserPrompt: "hi", ResponsePreview: "hello", StartedAt: recentRec.StartedAt}},
		}

		oldID := "old-session"
		oldRec := storage.SessionRecord{ID: oldID, HarnessID: "claude", StartedAt: realNow.Add(-40 * 24 * time.Hour), LastSeenAt: realNow.Add(-40 * 24 * time.Hour)}
		drv.all = append(drv.all, oldRec)
		drv.orgOf[oldID] = org
		drv.sessionsByOrg[org][oldID] = oldRec
		drv.summaries[oldID] = []storage.TraceSummaryRecord{
			{SpanTurnRecord: storage.SpanTurnRecord{TraceID: "old-t1", UserPrompt: "ancient", ResponsePreview: "history", StartedAt: oldRec.StartedAt}},
		}

		server := newExportServer(drv)
		resp, body := getRaw(server, "/v1/sessions/export?since=1970-01-01T00:00:00Z", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(1))
		Expect(string(body)).To(ContainSubstring("recent-t1"))
		Expect(string(body)).NotTo(ContainSubstring("old-t1"))
	})

	It("returns 400 when since is not a valid RFC3339 timestamp", func() {
		drv := newPagingDriver(org, 1, now)
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/export?since=not-a-time", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
	})

	DescribeTable("rejects an empty or reversed time window",
		func(since, until time.Time) {
			drv := newPagingDriver(org, 1, now)
			server := newExportServer(drv)

			path := "/v1/sessions/export?since=" + since.Format(time.RFC3339) + "&until=" + until.Format(time.RFC3339)
			resp, _ := getRaw(server, path, org)
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
			Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))
			Expect(resp.Header.Get("Content-Disposition")).To(BeEmpty())
		},
		Entry("when until equals since", now.Add(-5*time.Minute), now.Add(-5*time.Minute)),
		Entry("when until is before since", now.Add(-2*time.Minute), now.Add(-5*time.Minute)),
	)

	// detail=traces: one header-only line per session, grain in the filename.
	It("exports turn headers per session at detail=traces", func() {
		drv := newPagingDriver(org, 3, now)
		server := newExportServer(drv)

		resp, body := getRaw(server, "/v1/sessions/export?detail=traces", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Disposition")).To(ContainSubstring("-traces.jsonl"))

		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(3))
		Expect(string(body)).To(ContainSubstring(`"trace_id":"session-0000-t1"`))
		Expect(string(body)).NotTo(ContainSubstring(`"spans"`))
	})

	It("returns 400 for an unrecognized detail value", func() {
		drv := newPagingDriver(org, 1, now)
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/export?detail=bogus", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
	})

	// T-9: org isolation.
	It("only includes sessions belonging to the requesting org", func() {
		drv := newPagingDriver(org, 5, now)
		otherOrg := "44444444-4444-4444-4444-444444444444"
		// Sneak in a session for a different org.
		otherID := "other-session"
		otherRec := storage.SessionRecord{ID: otherID, HarnessID: "claude", StartedAt: now, LastSeenAt: now}
		drv.all = append(drv.all, otherRec)
		drv.orgOf[otherID] = otherOrg
		drv.sessionsByOrg[otherOrg] = map[string]storage.SessionRecord{otherID: otherRec}
		drv.summaries[otherID] = []storage.TraceSummaryRecord{
			{SpanTurnRecord: storage.SpanTurnRecord{TraceID: "leak-t1", UserPrompt: "secret", ResponsePreview: "shh", StartedAt: now}},
		}

		server := newExportServer(drv)
		resp, body := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(5))
		Expect(string(body)).NotTo(ContainSubstring("leak-t1"))
		Expect(string(body)).NotTo(ContainSubstring("secret"))
	})

	// T-10: streaming / no whole-bundle buffering. We can't directly observe
	// memory, but we can assert the handler writes via the fasthttp body
	// stream mechanism (Transfer-Encoding: chunked), which is the
	// externally-observable signature of SetBodyStreamWriter rather than a
	// single c.Send()/c.JSON() buffered write.
	It("streams the response body rather than buffering it as a single Content-Length body", func() {
		drv := newPagingDriver(org, 5, now)
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.TransferEncoding).To(ContainElement("chunked"))
	})

	It("cancels the stream context after the response completes", func() {
		drv := newPagingDriver(org, 5, now)
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(drv.listCtxs).NotTo(BeEmpty())
		for _, ctx := range drv.listCtxs {
			Expect(ctx.Err()).To(MatchError(context.Canceled))
		}
	})

	// T-11: route-shadowing — /v1/sessions/export must not be captured by
	// /v1/sessions/:id (which would 400 on "export" not being a UUID, or
	// 404, instead of listing).
	It("is not shadowed by the /v1/sessions/:id route", func() {
		drv := newPagingDriver(org, 3, now)
		server := newExportServer(drv)

		resp, body := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/x-ndjson"))
		lines := nonEmptyLinesAPI(string(body))
		Expect(lines).To(HaveLen(3))
	})

	// 501 gate mirrors the single-session endpoint.
	It("returns 501 when the driver does not implement the sessions/span read surface", func() {
		base := inmemory.NewDriver()
		server := newExportServer(base)

		resp, _ := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotImplemented))
	})

	It("sets Content-Disposition with a last-30-days filename", func() {
		drv := newPagingDriver(org, 1, now)
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		disposition := resp.Header.Get("Content-Disposition")
		Expect(disposition).To(ContainSubstring("attachment"))
		Expect(disposition).To(ContainSubstring("sessions-last-30-days-"))
		Expect(disposition).To(ContainSubstring(".jsonl"))
	})

	It("names the file after the window when since/until narrow it (not last-30-days)", func() {
		drv := newPagingDriver(org, 1, now)
		server := newExportServer(drv)

		since := now.Add(-10 * 24 * time.Hour)
		until := now.Add(-1 * 24 * time.Hour)
		path := "/v1/sessions/export?since=" + since.Format(time.RFC3339) + "&until=" + until.Format(time.RFC3339)
		resp, _ := getRaw(server, path, org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		disposition := resp.Header.Get("Content-Disposition")
		Expect(disposition).NotTo(ContainSubstring("last-30-days"))
		Expect(disposition).To(ContainSubstring("sessions-" + since.UTC().Format("2006-01-02") + "-to-" + until.UTC().Format("2006-01-02")))
		Expect(disposition).To(ContainSubstring(".jsonl"))
	})

	// Open risk from design.md: the bundle handler is the first
	// SetBodyStreamWriter use in tapes, and compress.New() (api/api.go)
	// sits in front of it — unvalidated whether gzip middleware interacts
	// correctly with a streamed body. Assert the response is actually
	// gzip-encoded when the client asks for it, AND that decoding it
	// reproduces the exact NDJSON the uncompressed path returns.
	It("gzip-encodes the streamed response for Accept-Encoding: gzip and decodes to the expected NDJSON", func() {
		drv := newPagingDriver(org, 5, now)
		server := newExportServer(drv)

		resp, gzipped := getGzip(server, "/v1/sessions/export", org)
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Encoding")).To(Equal("gzip"))

		decoded := decodeGzip(gzipped)
		lines := nonEmptyLinesAPI(decoded)
		Expect(lines).To(HaveLen(5))
		Expect(decoded).To(ContainSubstring(`"schema":"` + ProjectionSchema + `"`))

		// Cross-check against the uncompressed path so the assertion is
		// pinned to "identical NDJSON", not just "5 lines of something".
		_, plain := getRaw(server, "/v1/sessions/export", org)
		Expect(decoded).To(Equal(string(plain)))
	})
})

// getGzip issues a GET with Accept-Encoding: gzip and returns the raw
// (still-compressed) response body alongside the response, so callers can
// assert on Content-Encoding before decoding.
func getGzip(server *Server, path, org string) (*http.Response, []byte) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := server.app.Test(req, -1)
	Expect(err).NotTo(HaveOccurred())
	body, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	return resp, body
}

// decodeGzip gunzips a response body captured via getGzip.
func decodeGzip(gzipped []byte) string {
	r, err := gzip.NewReader(bytes.NewReader(gzipped))
	Expect(err).NotTo(HaveOccurred())
	defer r.Close()
	decoded, err := io.ReadAll(r)
	Expect(err).NotTo(HaveOccurred())
	return string(decoded)
}

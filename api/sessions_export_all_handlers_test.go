package api

import (
	"context"
	"fmt"
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
}

func (d *pagingExportStubDriver) ListSessionRecords(_ context.Context, orgID string, opts storage.SessionListOpts) ([]storage.SessionRecord, error) {
	d.listCalls++
	d.listLimits = append(d.listLimits, opts.Limit)

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

	if opts.CursorTs != nil && opts.CursorID != nil {
		start := len(matched)
		for i, s := range matched {
			if s.LastSeenAt.Before(*opts.CursorTs) ||
				(s.LastSeenAt.Equal(*opts.CursorTs) && s.ID < *opts.CursorID) {
				start = i
				break
			}
		}
		matched = matched[start:]
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

	for i := 0; i < n; i++ {
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

	It("returns 400 when since is not a valid RFC3339 timestamp", func() {
		drv := newPagingDriver(org, 1, now)
		server := newExportServer(drv)

		resp, _ := getRaw(server, "/v1/sessions/export?since=not-a-time", org)
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
})

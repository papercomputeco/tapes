package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// sessionsStubDriver wraps a real storage.Driver and implements the
// unexported sessionsReader capability interface with canned responses,
// recording the arguments it receives so specs can assert org threading and
// that validation short-circuits before any storage call. It follows the
// identityDriver pattern from v1_session_identity_test.go.
type sessionsStubDriver struct {
	storage.Driver

	// ListSessionRecords stubbing.
	listRecords  []storage.SessionRecord
	listErr      error
	listCalls    int
	lastListOrg  string
	lastLimit    int
	lastCursorTs *time.Time
	lastCursorID *string

	// GetSessionRecordByHarness stubbing.
	harnessRecord        *storage.SessionRecord
	harnessErr           error
	harnessCalls         int
	lastOrgID            string
	lastHarnessID        string
	lastHarnessSessionID string
}

func (d *sessionsStubDriver) ListSessionRecords(_ context.Context, orgID string, limit int, cursorTs *time.Time, cursorID *string) ([]storage.SessionRecord, error) {
	d.listCalls++
	d.lastListOrg = orgID
	d.lastLimit = limit
	d.lastCursorTs = cursorTs
	d.lastCursorID = cursorID
	return d.listRecords, d.listErr
}

func (d *sessionsStubDriver) GetSessionRecord(_ context.Context, _, _ string) (*storage.SessionRecord, error) {
	return nil, nil
}

func (d *sessionsStubDriver) GetSessionRecordByHarness(_ context.Context, orgID, harnessID, harnessSessionID string) (*storage.SessionRecord, error) {
	d.harnessCalls++
	d.lastOrgID = orgID
	d.lastHarnessID = harnessID
	d.lastHarnessSessionID = harnessSessionID
	return d.harnessRecord, d.harnessErr
}

func (d *sessionsStubDriver) ListNodesBySession(_ context.Context, _ string) ([]*merkle.Node, error) {
	return nil, nil
}

// getSessionList issues GET path against the server, optionally with the
// X-Tapes-Org-Id header (empty org sends no header), and decodes the body as
// a SessionListResponse on 200 or an llm.ErrorResponse otherwise.
func getSessionList(server *Server, path, org string) (SessionListResponse, llm.ErrorResponse, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())

	var body SessionListResponse
	var errBody llm.ErrorResponse
	if resp.StatusCode == fiber.StatusOK {
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
	} else {
		Expect(json.Unmarshal(raw, &errBody)).To(Succeed())
	}
	return body, errBody, resp.StatusCode
}

var _ = Describe("harness natural-key filter on GET /v1/sessions", func() {
	var record storage.SessionRecord

	newSessionsServer := func(driver storage.Driver) *Server {
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	BeforeEach(func() {
		started := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
		ended := started.Add(10 * time.Minute)
		record = storage.SessionRecord{
			ID:                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			HarnessID:         "claude",
			HarnessSessionID:  "sess-xyz",
			Name:              "menu work",
			Cwd:               "/home/dev/project",
			HarnessVersion:    "1.2.3",
			StartedAt:         started,
			LastSeenAt:        ended,
			EndedAt:           &ended,
			TurnCount:         4,
			TotalInputTokens:  100,
			TotalOutputTokens: 200,
			TotalCostUsd:      0.42,
			DerivedStatus:     "completed",
			Preview:           "first user turn",
		}
	})

	It("returns a 200 single-item SessionListResponse when both harness params match", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].ID).To(Equal(record.ID))
		Expect(body.Items[0].HarnessID).To(Equal("claude"))
		Expect(body.Items[0].HarnessSessionID).To(Equal("sess-xyz"))
		Expect(drv.harnessCalls).To(Equal(1), "the filter must hit the natural-key lookup exactly once")
		// The params are passed through verbatim — exact match, as stored.
		Expect(drv.lastHarnessID).To(Equal("claude"))
		Expect(drv.lastHarnessSessionID).To(Equal("sess-xyz"))
		Expect(drv.listCalls).To(BeZero(), "the paged-list path must be skipped when filtering")
	})

	It("returns 200 with empty items when the harness filter matches no session", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: nil}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-missing", "")
		Expect(status).To(Equal(fiber.StatusOK), "a nil record is a normal no-match, never 404/500")
		Expect(body.Items).NotTo(BeNil(), "no match must serialize as an empty items list, not null")
		Expect(body.Items).To(BeEmpty())
		Expect(body.NextCursor).To(BeEmpty())
		Expect(drv.harnessCalls).To(Equal(1))
	})

	It("treats empty harness params as absent and serves the unfiltered list", func() {
		// Both params present but empty must take the unfiltered paged-list
		// path, not the filter (and not a 400): the router keys on non-empty
		// values, and ingest guarantees no stored row carries an empty
		// harness id, so an empty value could never address a row anyway.
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			listRecords:   []storage.SessionRecord{record},
			harnessRecord: &record,
		}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_id=&harness_session_id=", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.harnessCalls).To(BeZero(), "empty params must not reach the natural-key lookup")
		Expect(drv.listCalls).To(Equal(1), "empty params must fall through to the paged list")
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].ID).To(Equal(record.ID))
	})

	It("returns 400 when only harness_id is supplied", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?harness_id=claude", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		// The error must name the both-or-neither rule so the caller can tell
		// the filter was rejected, not silently dropped.
		Expect(errBody.Error).To(ContainSubstring("harness_id"))
		Expect(errBody.Error).To(ContainSubstring("harness_session_id"))
		Expect(drv.harnessCalls).To(BeZero(), "validation must precede any storage call")
		Expect(drv.listCalls).To(BeZero(), "a lone param must not fall through to the unfiltered list")
	})

	It("returns 400 when only harness_session_id is supplied", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("harness_id"))
		Expect(errBody.Error).To(ContainSubstring("harness_session_id"))
		Expect(drv.harnessCalls).To(BeZero(), "validation must precede any storage call")
		Expect(drv.listCalls).To(BeZero(), "a lone param must not fall through to the unfiltered list")
	})

	It("threads the X-Tapes-Org-Id org through to the harness lookup", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		org := "11111111-1111-1111-1111-111111111111"
		_, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", org)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastOrgID).To(Equal(org), "the lookup must be scoped to the requested tenant")

		// Without the header the middleware falls back to the nil-org
		// sentinel, which must still be threaded down to the lookup.
		_, _, status = getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastOrgID).To(Equal(nilOrgID))
	})

	It("keeps the unfiltered paged list behavior when no harness params are supplied", func() {
		older := record
		older.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		older.LastSeenAt = record.LastSeenAt.Add(-time.Minute)
		oldest := record
		oldest.ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
		oldest.LastSeenAt = record.LastSeenAt.Add(-2 * time.Minute)

		// Three records back from storage against limit=2 means a next page
		// exists and the third row is trimmed.
		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{record, older, oldest},
		}
		server := newSessionsServer(drv)

		cursorTs := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
		cursor := encodeSessionsCursor(sessionsCursor{LastSeenAt: cursorTs, ID: record.ID})

		body, _, status := getSessionList(server, "/v1/sessions?limit=2&cursor="+cursor, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.harnessCalls).To(BeZero(), "the harness lookup must never run on the unfiltered path")
		Expect(drv.listCalls).To(Equal(1))
		Expect(drv.lastLimit).To(Equal(3), "the handler fetches limit+1 to detect the next page")
		Expect(drv.lastCursorTs).NotTo(BeNil())
		Expect(drv.lastCursorTs.Equal(cursorTs)).To(BeTrue())
		Expect(drv.lastCursorID).NotTo(BeNil())
		Expect(*drv.lastCursorID).To(Equal(record.ID))

		Expect(body.Items).To(HaveLen(2))
		Expect(body.Items[0].ID).To(Equal(record.ID))
		Expect(body.Items[1].ID).To(Equal(older.ID))
		Expect(body.NextCursor).NotTo(BeEmpty())
		next, err := decodeSessionsCursor(body.NextCursor)
		Expect(err).NotTo(HaveOccurred())
		Expect(next.ID).To(Equal(older.ID))
		Expect(next.LastSeenAt.Equal(older.LastSeenAt)).To(BeTrue())
	})

	It("ignores limit on the harness filter path", func() {
		// The point lookup returns at most one row, so limit — even a
		// malformed one that would 400 on the paged-list path — is ignored
		// rather than validated when the filter is active.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?limit=banana&harness_id=claude&harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].ID).To(Equal(record.ID))
		Expect(drv.harnessCalls).To(Equal(1))
		Expect(drv.listCalls).To(BeZero())
	})

	It("returns 400 when cursor is combined with the harness filter", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		// A well-formed cursor: the rejection is about combining pagination
		// with a point lookup, not about cursor decoding.
		cursor := encodeSessionsCursor(sessionsCursor{LastSeenAt: record.LastSeenAt, ID: record.ID})

		_, errBody, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&cursor="+cursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))
		Expect(drv.harnessCalls).To(BeZero(), "validation must precede any storage call")
		Expect(drv.listCalls).To(BeZero())
	})

	It("omits next_cursor and populates the filtered item like a list row", func() {
		// The same record is served by both the paged-list path and the
		// harness lookup so the two response rows can be compared field by
		// field — the filtered item must be built exactly like a list row,
		// preview included.
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			listRecords:   []storage.SessionRecord{record},
			harnessRecord: &record,
		}
		server := newSessionsServer(drv)

		listBody, _, listStatus := getSessionList(server, "/v1/sessions", "")
		Expect(listStatus).To(Equal(fiber.StatusOK))
		Expect(listBody.Items).To(HaveLen(1))

		filtered, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(filtered.NextCursor).To(BeEmpty(), "a point lookup has no next page")
		Expect(filtered.Items).To(HaveLen(1))
		Expect(filtered.Items[0]).To(Equal(listBody.Items[0]))
		Expect(filtered.Items[0].Preview).To(Equal(record.Preview), "the filtered row must carry preview like a list row")
	})

	It("returns 501 when the driver does not implement sessionsReader regardless of filter params", func() {
		base := inmemory.NewDriver()
		_, hasReader := storage.Driver(base).(sessionsReader)
		Expect(hasReader).To(BeFalse(), "precondition: the bare inmemory driver must not implement sessionsReader")

		server := newSessionsServer(base)

		_, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))

		_, _, status = getSessionList(server, "/v1/sessions", "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
	})
})

package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	listRecords     []storage.SessionRecord
	listErr         error
	listCalls       int
	lastListOrg     string
	lastAuthSubject string
	lastLimit       int
	lastCursorVal   *string
	lastCursorID    *string
	lastSort        storage.SessionSortField
	lastDir         storage.SortDirection

	// GetSessionRecordByHarness stubbing.
	harnessRecord        *storage.SessionRecord
	harnessErr           error
	harnessCalls         int
	lastOrgID            string
	lastHarnessID        string
	lastHarnessSessionID string

	// DeleteSession stubbing. deletable holds the ids that exist; a hit
	// removes the id and reports true, a miss reports false — mirroring the
	// real driver's (deleted bool) contract.
	deletable     map[string]bool
	deleteErr     error
	deleteCalls   int
	lastDeleteOrg string
	lastDeleteID  string

	// GetSessionRecord stubbing (used by handleUpdateSession's post-write
	// re-read; the harness-filter helper above has its own canned nil
	// return and is left untouched).
	getRecord    *storage.SessionRecord
	getErr       error
	getCalls     int
	lastGetOrgID string
	lastGetID    string

	// UpdateSessionName stubbing.
	updateRowsAffected int64
	updateErr          error
	updateCalls        int
	lastUpdateOrgID    string
	lastUpdateID       string
	lastUpdateName     *string
}

// errStubDelete is the canned failure the stub returns to exercise the
// handler's 500 path.
var errStubDelete = errors.New("stub delete failure")

func (d *sessionsStubDriver) DeleteSession(_ context.Context, orgID, id string) (bool, error) {
	d.deleteCalls++
	d.lastDeleteOrg = orgID
	d.lastDeleteID = id
	if d.deleteErr != nil {
		return false, d.deleteErr
	}
	if !d.deletable[id] {
		return false, nil
	}
	delete(d.deletable, id)
	return true, nil
}

func (d *sessionsStubDriver) ListSessionRecords(_ context.Context, orgID string, opts storage.SessionListOpts) ([]storage.SessionRecord, error) {
	d.listCalls++
	d.lastListOrg = orgID
	d.lastAuthSubject = opts.AuthSubject
	d.lastLimit = opts.Limit
	d.lastCursorVal = opts.CursorVal
	d.lastCursorID = opts.CursorID
	d.lastSort = opts.Sort
	d.lastDir = opts.Dir
	return d.listRecords, d.listErr
}

func (d *sessionsStubDriver) GetSessionRecord(_ context.Context, orgID, id string) (*storage.SessionRecord, error) {
	d.getCalls++
	d.lastGetOrgID = orgID
	d.lastGetID = id
	return d.getRecord, d.getErr
}

func (d *sessionsStubDriver) GetSessionRecordByHarness(_ context.Context, orgID, harnessID, harnessSessionID string) (*storage.SessionRecord, error) {
	d.harnessCalls++
	d.lastOrgID = orgID
	d.lastHarnessID = harnessID
	d.lastHarnessSessionID = harnessSessionID
	return d.harnessRecord, d.harnessErr
}

// UpdateSessionName records the call (org/id/name) and returns the canned
// rowsAffected/err, mirroring the real driver's contract: the handler must
// treat rowsAffected==0 as "not in this org / unknown id" (CC-2) rather than
// inspecting the name it sent.
func (d *sessionsStubDriver) UpdateSessionName(_ context.Context, orgID, id string, name *string) (int64, error) {
	d.updateCalls++
	d.lastUpdateOrgID = orgID
	d.lastUpdateID = id
	d.lastUpdateName = name
	return d.updateRowsAffected, d.updateErr
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

	It("threads auth_subject through to the paged list and echoes it on items", func() {
		// Given a stored record attributed to a user
		attributed := record
		attributed.AuthSubject = "user_01HXYZ"
		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{attributed},
		}
		server := newSessionsServer(drv)

		// When listing with the auth_subject filter
		body, _, status := getSessionList(server, "/v1/sessions?auth_subject=user_01HXYZ", "")

		// Then the subject reaches storage verbatim and the item
		// carries it back
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.listCalls).To(Equal(1))
		Expect(drv.lastAuthSubject).To(Equal("user_01HXYZ"))
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].AuthSubject).To(Equal("user_01HXYZ"))
	})

	It("lists every user's sessions when auth_subject is absent", func() {
		// Given storage rows
		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{record},
		}
		server := newSessionsServer(drv)

		// When listing without the filter
		_, _, status := getSessionList(server, "/v1/sessions", "")

		// Then storage sees the empty (no-filter) subject
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastAuthSubject).To(BeEmpty())
	})

	It("keeps the unfiltered paged list behavior when no harness params are supplied", func() {
		older := record
		older.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		older.LastSeenAt = record.LastSeenAt.Add(-time.Minute)
		older.SortVal = "2026-06-01 12:09:00+00" // canonical ::text of older.LastSeenAt
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

		cursorVal := "2026-06-02 00:00:00+00"
		// The request omits sort, so it defaults to last_active/desc; the cursor
		// must carry that same context now that bare {val,id} cursors are gone.
		cursor := encodeSessionsCursor(sessionsCursor{
			Sort: string(storage.SortLastActive),
			Dir:  string(storage.SortDesc),
			Val:  cursorVal,
			ID:   record.ID,
		})

		body, _, status := getSessionList(server, "/v1/sessions?limit=2&cursor="+cursor, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.harnessCalls).To(BeZero(), "the harness lookup must never run on the unfiltered path")
		Expect(drv.listCalls).To(Equal(1))
		Expect(drv.lastLimit).To(Equal(3), "the handler fetches limit+1 to detect the next page")
		Expect(drv.lastCursorVal).NotTo(BeNil())
		Expect(*drv.lastCursorVal).To(Equal(cursorVal))
		Expect(drv.lastCursorID).NotTo(BeNil())
		Expect(*drv.lastCursorID).To(Equal(record.ID))

		Expect(body.Items).To(HaveLen(2))
		Expect(body.Items[0].ID).To(Equal(record.ID))
		Expect(body.Items[1].ID).To(Equal(older.ID))
		Expect(body.NextCursor).NotTo(BeEmpty())
		next, err := decodeSessionsCursor(body.NextCursor)
		Expect(err).NotTo(HaveOccurred())
		Expect(next.ID).To(Equal(older.ID))
		Expect(next.Val).To(Equal(older.SortVal))
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

	It("ignores sort params on the harness filter path", func() {
		// The point lookup returns before sort parsing, so an otherwise-invalid
		// sort key — which would 400 on the paged-list path — is ignored rather
		// than validated when the filter is active. This locks the early-return
		// ordering against a future reorder.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&sort=bogus", "")
		Expect(status).To(Equal(fiber.StatusOK), "the harness branch must return before sort parsing")
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
		cursor := encodeSessionsCursor(sessionsCursor{Val: "2026-06-01 12:10:00+00", ID: record.ID})

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

var _ = Describe("DELETE /v1/sessions/:id", func() {
	const (
		validID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
		org     = "11111111-1111-1111-1111-111111111111"
	)

	newServer := func(driver storage.Driver) *Server {
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	It("deletes an existing session and returns 204, scoped to the org", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deletable: map[string]bool{validID: true}}
		server := newServer(drv)

		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org, "")
		Expect(status).To(Equal(fiber.StatusNoContent))
		Expect(drv.deleteCalls).To(Equal(1))
		Expect(drv.lastDeleteOrg).To(Equal(org), "the delete must be scoped to the requested tenant")
		Expect(drv.lastDeleteID).To(Equal(validID))
		Expect(drv.deletable).NotTo(HaveKey(validID))
	})

	It("returns 404 when the session id is absent", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deletable: map[string]bool{}}
		server := newServer(drv)

		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org, "")
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(drv.deleteCalls).To(Equal(1), "a well-formed id reaches storage; the miss surfaces as 404")
	})

	It("returns 400 for a malformed (non-UUID) id without touching storage", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deletable: map[string]bool{}}
		server := newServer(drv)

		body, status := doJSON(server, http.MethodDelete, "/v1/sessions/not-a-uuid", "", org, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(body["error"]).To(ContainSubstring("UUID"))
		Expect(drv.deleteCalls).To(BeZero(), "the parse failure must short-circuit before the driver call")
	})

	It("returns 500 when the driver fails to delete", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deleteErr: errStubDelete}
		server := newServer(drv)

		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org, "")
		Expect(status).To(Equal(fiber.StatusInternalServerError))
	})

	It("returns 501 when the backend does not support session writes", func() {
		// The bare in-memory driver implements the read surface but not
		// sessionsWriter, so the handler must report 501.
		base := inmemory.NewDriver()
		_, hasWriter := storage.Driver(base).(sessionsWriter)
		Expect(hasWriter).To(BeFalse(), "precondition: the bare inmemory driver must not implement sessionsWriter")

		server := newServer(base)
		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org, "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
	})
})

var _ = Describe("sort and direction params on GET /v1/sessions", func() {
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
			ID:               "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			HarnessID:        "claude",
			HarnessSessionID: "sess-xyz",
			StartedAt:        started,
			LastSeenAt:       ended,
			DerivedStatus:    "completed",
		}
	})

	It("threads sort and direction through to storage opts", func() {
		cheap := record
		cheap.TotalCostUsd = 0.10
		cheap.SortVal = "0.10"
		pricey := record
		pricey.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		pricey.TotalCostUsd = 0.90
		pricey.SortVal = "0.90"

		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{cheap, pricey},
		}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?sort=total_cost_usd&direction=asc", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastSort).To(Equal(storage.SortTotalCost))
		Expect(drv.lastDir).To(Equal(storage.SortAsc))
		Expect(body.Items).To(HaveLen(2))
	})

	It("rejects an unknown sort key with 400", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?sort=bogus", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("invalid sort"))
		Expect(drv.listCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("rejects an invalid direction with 400", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?direction=sideways", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("invalid direction"))
		Expect(drv.listCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("rejects a cursor whose sort disagrees with the request with 400", func() {
		// Mint a raw cursor encoding sort=total_cost_usd so we can replay it
		// under a different sort to trigger the mismatch rejection.
		cursorJSON := `{"sort":"total_cost_usd","dir":"desc","val":"42.50","id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
		mismatchCursor := base64.RawURLEncoding.EncodeToString([]byte(cursorJSON))

		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?sort=turn_count&cursor="+mismatchCursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))
		Expect(drv.listCalls).To(BeZero(), "mismatch must be rejected before any storage call")
	})

	It("rejects a pre-sort {ts,id} cursor (no sort context) as 400", func() {
		// Legacy cursors are no longer supported: the console ships alongside
		// this change and always mints sort-aware cursors, so a token that
		// carries no sort context is malformed and must be rejected before any
		// storage call rather than defaulted into a last_active boundary.
		legacyJSON := `{"ts":"2026-06-26T00:00:00Z","id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
		legacyCursor := base64.RawURLEncoding.EncodeToString([]byte(legacyJSON))

		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?cursor="+legacyCursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))
		Expect(drv.listCalls).To(BeZero(), "malformed cursor must be rejected before any storage call")
	})

	It("rejects an empty boundary value on a numeric sort as 400", func() {
		// An empty val would cast as ''::bigint in the keyset predicate and 500
		// mid-scan; for a numeric/timestamptz sort column the handler must
		// surface it as a client error instead of forwarding it to storage.
		emptyValJSON := `{"sort":"total_cost_usd","dir":"desc","val":"","id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}`
		emptyValCursor := base64.RawURLEncoding.EncodeToString([]byte(emptyValJSON))

		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?sort=total_cost_usd&cursor="+emptyValCursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))
		Expect(drv.listCalls).To(BeZero(), "empty numeric boundary must be rejected before any storage call")
	})

	It("the next_cursor encodes sort and direction for keyset continuity", func() {
		// Seed 2 records against limit=1 so there is a next page.
		first := record
		first.SortVal = "0.90"
		second := record
		second.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		second.SortVal = "0.10"

		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{first, second},
		}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?sort=total_cost_usd&direction=desc&limit=1", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.NextCursor).NotTo(BeEmpty())

		// Decode the next cursor and verify sort context is embedded.
		next, err := decodeSessionsCursor(body.NextCursor)
		Expect(err).NotTo(HaveOccurred())
		Expect(next.Sort).To(Equal(string(storage.SortTotalCost)))
		Expect(next.Dir).To(Equal(string(storage.SortDesc)))
		Expect(next.ID).To(Equal(first.ID))
		Expect(next.Val).To(Equal(first.SortVal))
	})
})

package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
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

	// ListSessionRecordsByHarnessSessionID stubbing (the lone-id form of
	// the harness filter).
	loneIDRecords            []storage.SessionRecord
	loneIDErr                error
	loneIDCalls              int
	lastLoneIDOrg            string
	lastLoneHarnessSessionID string

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

	// Claimed published-filter stubbing: what ListSessionRecords received
	// (empty when no claimed param reached storage), and the point-lookup
	// matcher's canned answer.
	lastClaimedFilters []storage.PublishedFilter
	matcherResult      bool
	matcherErr         error
	matcherCalls       int
	lastMatcherFilter  *storage.PublishedFilter
	lastMatcherID      string
}

// MatchesPublishedFilter records the call and returns the canned match,
// mirroring the real driver's SQL-side point evaluation.
func (d *sessionsStubDriver) MatchesPublishedFilter(_ context.Context, filter *storage.PublishedFilter, primitiveID string) (bool, error) {
	d.matcherCalls++
	d.lastMatcherFilter = filter
	d.lastMatcherID = primitiveID
	return d.matcherResult, d.matcherErr
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
	d.lastClaimedFilters = opts.ClaimedFilters
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

func (d *sessionsStubDriver) ListSessionRecordsByHarnessSessionID(_ context.Context, orgID, harnessSessionID string) ([]storage.SessionRecord, error) {
	d.loneIDCalls++
	d.lastLoneIDOrg = orgID
	d.lastLoneHarnessSessionID = harnessSessionID
	return d.loneIDRecords, d.loneIDErr
}

// UpdateSessionName records the call (org/id/name) and returns the canned
// rowsAffected/err, mirroring the real driver's contract: the handler must
// treat rowsAffected==0 as "not in this org / unknown id" (CC-2) rather than
// inspecting the name it sent.
func (d *sessionsStubDriver) UpdateSessionDisplayName(_ context.Context, orgID, id string, name *string) (int64, error) {
	d.updateCalls++
	d.lastUpdateOrgID = orgID
	d.lastUpdateID = id
	d.lastUpdateName = name
	return d.updateRowsAffected, d.updateErr
}

func (d *sessionsStubDriver) ListNodesBySession(_ context.Context, _ string) ([]*merkle.Node, error) {
	return nil, nil
}

// legacyOrgIDHeader is the retired X-Tapes-Org-Id request header. The server
// no longer reads it; the tests keep the ability to send it so they can prove
// that a client asserting its own tenant is ignored rather than obeyed.
const legacyOrgIDHeader = "X-Tapes-Org-Id"

// getSessionList issues GET path against the server, optionally with the
// legacy org header, and decodes the body as a SessionListResponse on 200 or
// an llm.ErrorResponse otherwise.
func getSessionList(server *Server, path, org string) (SessionListResponse, llm.ErrorResponse, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(legacyOrgIDHeader, org)
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
		// A lone harness_id names a harness, not a session: honoring it
		// would be an unbounded, unpaginated list, so it stays rejected.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?harness_id=claude", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		// The error must name the missing param so the caller can tell the
		// filter was rejected, not silently dropped.
		Expect(errBody.Error).To(ContainSubstring("harness_id"))
		Expect(errBody.Error).To(ContainSubstring("harness_session_id"))
		Expect(drv.harnessCalls).To(BeZero(), "validation must precede any storage call")
		Expect(drv.loneIDCalls).To(BeZero(), "validation must precede any storage call")
		Expect(drv.listCalls).To(BeZero(), "a lone param must not fall through to the unfiltered list")
	})

	It("matches across all harnesses when only harness_session_id is supplied", func() {
		// The lone id is an exact-match filter on its own: a harness session
		// id is unique within a harness, so the cross-harness lookup returns
		// at most one row per harness — never the paired point lookup, never
		// the paged list.
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			loneIDRecords: []storage.SessionRecord{record},
		}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].ID).To(Equal(record.ID))
		Expect(body.Items[0].HarnessSessionID).To(Equal("sess-xyz"))
		Expect(body.NextCursor).To(BeEmpty(), "the harness filter never pages")
		Expect(drv.loneIDCalls).To(Equal(1), "the lone id must hit the cross-harness lookup exactly once")
		Expect(drv.lastLoneIDOrg).To(Equal(singleTenantOrgID))
		// The param is passed through verbatim — exact match, as stored.
		Expect(drv.lastLoneHarnessSessionID).To(Equal("sess-xyz"))
		Expect(drv.harnessCalls).To(BeZero(), "the paired point lookup must be skipped on the lone-id form")
		Expect(drv.listCalls).To(BeZero(), "the paged-list path must be skipped when filtering")
	})

	It("returns every harness's row when a lone harness_session_id collides across harnesses", func() {
		other := record
		other.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		other.HarnessID = "codex"
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			loneIDRecords: []storage.SessionRecord{record, other},
		}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.Items).To(HaveLen(2))
		Expect(body.Items[0].HarnessID).To(Equal("claude"))
		Expect(body.Items[1].HarnessID).To(Equal("codex"))
		Expect(body.NextCursor).To(BeEmpty())
	})

	It("returns 200 with empty items when the lone harness_session_id matches no session", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)

		body, _, status := getSessionList(server, "/v1/sessions?harness_session_id=sess-missing", "")
		Expect(status).To(Equal(fiber.StatusOK), "no match is a normal outcome, never 404/500")
		Expect(body.Items).NotTo(BeNil(), "no match must serialize as an empty items list, not null")
		Expect(body.Items).To(BeEmpty())
		Expect(body.NextCursor).To(BeEmpty())
		Expect(drv.loneIDCalls).To(Equal(1))
	})

	It("returns 400 when cursor is combined with a lone harness_session_id", func() {
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			loneIDRecords: []storage.SessionRecord{record},
		}
		server := newSessionsServer(drv)

		cursor := encodeSessionsCursor(sessionsCursor{Val: "2026-06-01 12:10:00+00", ID: record.ID})
		_, errBody, status := getSessionList(server, "/v1/sessions?harness_session_id=sess-xyz&cursor="+cursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))
		Expect(drv.loneIDCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("returns 400 naming the params when paged-list options ride along with the paired filter", func() {
		// sort/direction/since/until belong to the paged-list path; the
		// harness lookup has no ordering or window to apply them to. They
		// used to be silently discarded — the caller believed a sort was in
		// effect when it wasn't — so the combination is refused loudly.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server,
			"/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&sort=started_at&direction=asc", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		// The error must name each offending param so the caller knows
		// exactly what to drop.
		Expect(errBody.Error).To(ContainSubstring("sort"))
		Expect(errBody.Error).To(ContainSubstring("direction"))
		Expect(drv.harnessCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("returns 400 naming the params when paged-list options ride along with a lone harness_session_id", func() {
		// Same refusal on the lone-id form: both harness-filter paths must
		// treat the paged-list options identically.
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			loneIDRecords: []storage.SessionRecord{record},
		}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server,
			"/v1/sessions?harness_session_id=sess-xyz&since=2026-06-01T00:00:00Z&until=2026-06-02T00:00:00Z", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("since"))
		Expect(errBody.Error).To(ContainSubstring("until"))
		Expect(errBody.Error).NotTo(ContainSubstring("sort"), "only the params actually supplied are named")
		Expect(drv.loneIDCalls).To(BeZero(), "validation must precede any storage call")
	})

	It("returns 500 when the lone-id lookup fails", func() {
		drv := &sessionsStubDriver{
			Driver:    inmemory.NewDriver(),
			loneIDErr: errors.New("stub lone-id failure"),
		}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusInternalServerError))
		Expect(errBody.Error).To(Equal("failed to list sessions"))
	})

	It("ignores a client-asserted org header and scopes every read to the single tenant", func() {
		// The retired header let any caller name the tenant it wanted to
		// read, with nothing verifying the claim: reaching the read API was
		// the whole of the authorization. Tenancy is now settled at the
		// gateway, which pins the JWT's org_id claim to the tenant that owns
		// these routes, so a value arriving here must carry no weight.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		otherTenant := "11111111-1111-1111-1111-111111111111"
		_, _, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", otherTenant)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastOrgID).NotTo(Equal(otherTenant),
			"a client-asserted tenant must never reach the storage lookup")
		Expect(drv.lastOrgID).To(Equal(singleTenantOrgID))

		// And with no header at all, the same scoping.
		_, _, status = getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastOrgID).To(Equal(singleTenantOrgID))
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

	It("refuses sort on the harness filter path by presence, not validity", func() {
		// The harness branch returns before sort parsing, so even a sort key
		// that could never be valid is refused with the combination message,
		// not "invalid sort field". This locks the early-return ordering
		// against a future reorder: presence is the offense, validation
		// never runs.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record}
		server := newSessionsServer(drv)

		_, errBody, status := getSessionList(server, "/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&sort=bogus", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("sort"))
		Expect(errBody.Error).To(ContainSubstring("harness filter"))
		Expect(errBody.Error).NotTo(ContainSubstring("invalid sort field"),
			"the combination is refused before the sort value is ever parsed")
		Expect(drv.harnessCalls).To(BeZero())
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
		Expect(filtered.Items[0].Rollup.Preview).To(Equal(record.Preview), "the filtered row must carry preview like a list row")
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
		org     = singleTenantOrgID
	)

	newServer := func(driver storage.Driver) *Server {
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	It("deletes an existing session and returns 204, scoped to the org", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deletable: map[string]bool{validID: true}}
		server := newServer(drv)

		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org)
		Expect(status).To(Equal(fiber.StatusNoContent))
		Expect(drv.deleteCalls).To(Equal(1))
		Expect(drv.lastDeleteOrg).To(Equal(org), "the delete must be scoped to the requested tenant")
		Expect(drv.lastDeleteID).To(Equal(validID))
		Expect(drv.deletable).NotTo(HaveKey(validID))
	})

	It("returns 404 when the session id is absent", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deletable: map[string]bool{}}
		server := newServer(drv)

		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org)
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(drv.deleteCalls).To(Equal(1), "a well-formed id reaches storage; the miss surfaces as 404")
	})

	It("returns 400 for a malformed (non-UUID) id without touching storage", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deletable: map[string]bool{}}
		server := newServer(drv)

		body, status := doJSON(server, http.MethodDelete, "/v1/sessions/not-a-uuid", "", org)
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(body["error"]).To(ContainSubstring("UUID"))
		Expect(drv.deleteCalls).To(BeZero(), "the parse failure must short-circuit before the driver call")
	})

	It("returns 500 when the driver fails to delete", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), deleteErr: errStubDelete}
		server := newServer(drv)

		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org)
		Expect(status).To(Equal(fiber.StatusInternalServerError))
	})

	It("returns 501 when the backend does not support session writes", func() {
		// The bare in-memory driver implements the read surface but not
		// sessionsWriter, so the handler must report 501.
		base := inmemory.NewDriver()
		_, hasWriter := storage.Driver(base).(sessionsWriter)
		Expect(hasWriter).To(BeFalse(), "precondition: the bare inmemory driver must not implement sessionsWriter")

		server := newServer(base)
		_, status := doJSON(server, http.MethodDelete, "/v1/sessions/"+validID, "", org)
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

var _ = Describe("sessionItemFromStorage liveness", func() {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * time.Second) // inside the 5m window
	stale := now.Add(-10 * time.Minute)  // outside the 5m window
	ended := now.Add(-1 * time.Minute)   // an explicit close, recently

	liveOf := func(s storage.SessionRecord) bool {
		return sessionItemFromStorage(s, now).Live
	}

	It("stays live between turns even though the last turn folded to a terminal status", func() {
		// The regression this guards: an interactive session reads
		// derived_status=completed after every end_turn while still open.
		// Liveness must key on recency + ended_at, never on that status.
		Expect(liveOf(storage.SessionRecord{
			ID: "s1", LastSeenAt: recent, EndedAt: nil, DerivedStatus: "completed",
		})).To(BeTrue())
	})

	It("ignores every terminal status value for a recent, unended session", func() {
		for _, status := range []string{"completed", "failed", "abandoned", "unknown", ""} {
			Expect(liveOf(storage.SessionRecord{
				ID: "s", LastSeenAt: recent, EndedAt: nil, DerivedStatus: status,
			})).To(BeTrue(), "status %q should not gate liveness", status)
		}
	})

	It("is not live once seen outside the liveness window", func() {
		Expect(liveOf(storage.SessionRecord{
			ID: "s2", LastSeenAt: stale, EndedAt: nil, DerivedStatus: "completed",
		})).To(BeFalse())
	})

	It("is not live once the session has a recorded end, even if recently seen", func() {
		Expect(liveOf(storage.SessionRecord{
			ID: "s3", LastSeenAt: recent, EndedAt: &ended, DerivedStatus: "completed",
		})).To(BeFalse())
	})
})

// doJSON issues a request (optionally with the org header) and returns the
// decoded body map (on 2xx) plus the status code.
func doJSON(server *Server, method, path, body, org string) (map[string]any, int) {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewBufferString(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, path, rdr)
	Expect(err).NotTo(HaveOccurred())
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if org != "" {
		req.Header.Set(legacyOrgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out, resp.StatusCode
}

// claimTestManifest parses a v1alpha1 manifest that publishes the neutral
// fixture view (testpub.attachments) and claims the vintage param on the
// sessions surface. normalizeJSON is spliced verbatim after the match object
// ("" for a claim with no declared profile).
func claimTestManifest(normalizeJSON string) *v1alpha1.Manifest {
	doc := `{
	  "kind":"cassette/v1alpha1",
	  "cassette":{"name":"testpub","version":"1.0.0"},
	  "depends":{"core":"v1"},
	  "api":{"health":"/ping","openapi":"/openapi"},
	  "publishes":{
	    "views":["testpub.attachments"],
	    "filters":[{
	      "param":"vintage",
	      "surface":"sessions",
	      "view":"testpub.attachments",
	      "match":{"primitive_type":"session","value_column":"value"}` + normalizeJSON + `
	    }]
	  }
	}`
	parsed, err := v1alpha1.Parse([]byte(doc))
	Expect(err).NotTo(HaveOccurred())
	Expect(parsed.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
	return parsed
}

// fullNormalizeJSON is the fully-declared profile: the three verbs, in
// the order the key derivation applies them.
const fullNormalizeJSON = `,"normalize":["trim","nfc","casefold"]`

// installSessionsClaim admits and arms a claim-holding cassette instance
// directly on the server's registry — these specs are about what the handler
// does with an armed claim, not about how admission or arming happened (both
// are the runner's suite).
func installSessionsClaim(server *Server, manifest *v1alpha1.Manifest) {
	GinkgoHelper()
	installNamedClaim(server, "testpub", manifest)
}

// installNamedClaim admits and arms an additional claim-holding cassette
// under its own name, for specs exercising several distinct armed claims at
// once. Arming mirrors the runner's successful-probe outcome: only armed
// claims reach the request path at all.
func installNamedClaim(server *Server, name string, manifest *v1alpha1.Manifest) {
	GinkgoHelper()
	installUnarmedClaim(server, name, manifest)
	for _, claim := range cassetterunner.ManifestClaims(cassette.Name(name), manifest) {
		server.cassettes.ArmClaim(claim)
	}
}

// installUnarmedClaim admits a claim-holding cassette without arming its
// claims: the exact state the runner leaves an instance in when the
// published-view probe fails.
func installUnarmedClaim(server *Server, name string, manifest *v1alpha1.Manifest) {
	GinkgoHelper()
	Expect(server.cassettes.Put(&cassetterunner.Instance{
		Name:     cassette.Name(name),
		Manifest: manifest,
		URL:      "http://127.0.0.1:9999",
		Anchors:  cassette.Anchors{Health: "/ping", OpenAPI: "/openapi", Prefix: "api"},
	})).To(Succeed())
}

// flavorClaimManifest publishes a second fixture view under its own schema
// and claims the flavor param with a non-default value column — the shape of
// both review gaps at once: a second active claim on the surface, matching
// against a column that is not literally named value.
func flavorClaimManifest() *v1alpha1.Manifest {
	doc := `{
	  "kind":"cassette/v1alpha1",
	  "cassette":{"name":"otherpub","version":"1.0.0"},
	  "depends":{"core":"v1"},
	  "api":{"health":"/ping","openapi":"/openapi"},
	  "publishes":{
	    "views":["otherpub.flavors"],
	    "filters":[{
	      "param":"flavor",
	      "surface":"sessions",
	      "view":"otherpub.flavors",
	      "match":{"primitive_type":"session","value_column":"tag"}
	    }]
	  }
	}`
	parsed, err := v1alpha1.Parse([]byte(doc))
	Expect(err).NotTo(HaveOccurred())
	Expect(parsed.Validate([]cassette.ContractVersion{"v1"})).To(Succeed())
	return parsed
}

// getRawSessionList issues GET path and returns the raw body bytes and status
// — raw because the fail-open contract is literally byte-identity, and a
// decoded struct would hide the very differences the assertion is about.
func getRawSessionList(server *Server, path string) ([]byte, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	return raw, resp.StatusCode
}

var _ = Describe("claimed filter params on GET /v1/sessions", func() {
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
			EndedAt:          &ended,
			DerivedStatus:    "completed",
		}
	})

	It("ignores the param byte-identically when no admitted cassette claims it", func() {
		// Against an empty registry first.
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		server := newSessionsServer(drv)

		plain, status := getRawSessionList(server, "/v1/sessions")
		Expect(status).To(Equal(fiber.StatusOK))
		withParam, status := getRawSessionList(server, "/v1/sessions?vintage=alpha&vintage=beta")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(withParam).To(Equal(plain),
			"unclaimed, the param must be invisible: full-body byte identity, not shape equality")
		Expect(drv.lastClaimedFilters).To(BeEmpty(), "no claim, no filter reaches storage")

		// And against a registry holding a cassette that claims nothing.
		nonClaiming, err := v1alpha1.Parse([]byte(`{
		  "kind":"cassette/v1alpha1",
		  "cassette":{"name":"testpub","version":"1.0.0"},
		  "depends":{"core":"v1"},
		  "api":{"health":"/ping","openapi":"/openapi"}
		}`))
		Expect(err).NotTo(HaveOccurred())
		drv2 := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		server2 := newSessionsServer(drv2)
		installSessionsClaim(server2, nonClaiming)

		plain2, status := getRawSessionList(server2, "/v1/sessions")
		Expect(status).To(Equal(fiber.StatusOK))
		withParam2, status := getRawSessionList(server2, "/v1/sessions?vintage=alpha")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(withParam2).To(Equal(plain2),
			"a cassette with no claim must leave the param exactly as unclaimed")
		Expect(drv2.lastClaimedFilters).To(BeEmpty())
	})

	It("returns 500 when a claimed filter cannot be evaluated and never unfiltered results", func() {
		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{record},
			listErr:     errors.New(`relation "testpub.attachments" does not exist`),
		}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))

		raw, status := getRawSessionList(server, "/v1/sessions?vintage=alpha")
		Expect(status).To(Equal(fiber.StatusInternalServerError),
			"claimed-but-broken is loud; only an unclaimed param fails open")
		Expect(string(raw)).NotTo(ContainSubstring(record.ID),
			"no session rows may accompany the failure — unfiltered results are the forbidden degradation")
		Expect(drv.lastClaimedFilters).NotTo(BeEmpty(),
			"the failure must come from evaluating the filter, not from dropping it")
	})

	It("applies the manifest-declared normalization profile before binding filter values", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))

		query := url.Values{}
		query.Add("vintage", "  BUG  ") // trim then casefold
		query.Add("vintage", "Été")   // decomposed; nfc composes, casefold folds
		_, status := getRawSessionList(server, "/v1/sessions?"+query.Encode())
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastClaimedFilters).To(HaveLen(1))
		Expect(drv.lastClaimedFilters[0].Values).To(Equal([]string{"bug", "été"}),
			"the declared verbs run in declared order: trim, nfc, casefold")
		Expect(drv.lastClaimedFilters[0].TypeValue).To(Equal("session"))
		Expect(drv.lastClaimedFilters[0].View.String()).To(Equal("testpub.attachments"))
		Expect(drv.lastClaimedFilters[0].Column.String()).To(Equal("value"),
			"the claim-declared value column travels with its filter")

		// A claim that declares no profile passes values raw: core applies
		// only declared normalizations, never its own idea of hygiene.
		drv2 := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		server2 := newSessionsServer(drv2)
		installSessionsClaim(server2, claimTestManifest(""))

		rawQuery := url.Values{}
		rawQuery.Add("vintage", "  BUG  ")
		_, status = getRawSessionList(server2, "/v1/sessions?"+rawQuery.Encode())
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv2.lastClaimedFilters[0].Values).To(Equal([]string{"  BUG  "}))
	})

	It("returns 200 empty items for unmatchable filter values when claimed", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver()}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))

		body, _, status := getSessionList(server,
			"/v1/sessions?vintage="+url.QueryEscape("NoSuch,,weird ~ value"), "")
		Expect(status).To(Equal(fiber.StatusOK),
			"a value that could never match is an empty page, never an error")
		Expect(body.Items).NotTo(BeNil())
		Expect(body.Items).To(BeEmpty())
		Expect(body.NextCursor).To(BeEmpty())
		Expect(drv.lastClaimedFilters).NotTo(BeEmpty(),
			"honest-empty comes from the filter matching nothing, not from skipping it")
	})

	It("applies every admitted claim whose param the request supplies, never just the first", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))
		installNamedClaim(server, "otherpub", flavorClaimManifest())

		_, status := getRawSessionList(server, "/v1/sessions?vintage=%20Alpha%20&flavor=sweet")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastClaimedFilters).To(HaveLen(2),
			"both supplied claimed params must reach storage; dropping either is silent unfiltering")
		// ClaimsFor returns cassette-name order: otherpub before testpub.
		Expect(drv.lastClaimedFilters[0].View.String()).To(Equal("otherpub.flavors"))
		Expect(drv.lastClaimedFilters[0].Column.String()).To(Equal("tag"),
			"each filter carries its own claim's declared value column")
		Expect(drv.lastClaimedFilters[0].Values).To(Equal([]string{"sweet"}),
			"a claim with no declared profile passes its values raw")
		Expect(drv.lastClaimedFilters[1].View.String()).To(Equal("testpub.attachments"))
		Expect(drv.lastClaimedFilters[1].Column.String()).To(Equal("value"))
		Expect(drv.lastClaimedFilters[1].Values).To(Equal([]string{"alpha"}),
			"each claim folds its own values per its own declared profile")

		// One param alone still produces exactly its own filter.
		_, status = getRawSessionList(server, "/v1/sessions?flavor=sweet")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(drv.lastClaimedFilters).To(HaveLen(1))
		Expect(drv.lastClaimedFilters[0].View.String()).To(Equal("otherpub.flavors"))
	})

	It("binds the cursor to the combined, param-qualified filter set across claims", func() {
		first := record
		first.SortVal = "2026-06-01 12:10:00+00"
		second := record
		second.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		second.SortVal = "2026-06-01 12:09:00+00"
		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{first, second},
		}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))
		installNamedClaim(server, "otherpub", flavorClaimManifest())

		body, _, status := getSessionList(server, "/v1/sessions?vintage=alpha&flavor=sweet&limit=1", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.NextCursor).NotTo(BeEmpty())
		minted, err := decodeSessionsCursor(body.NextCursor)
		Expect(err).NotTo(HaveOccurred())
		Expect(minted.Filters).To(Equal([]string{"flavor=sweet", "vintage=alpha"}),
			"the binding is the combined set, sorted so it is canonical")

		// Dropping either claimed param invalidates the boundary.
		_, errBody, status := getSessionList(server, "/v1/sessions?vintage=alpha&cursor="+body.NextCursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))

		// The same combined set paginates on.
		body2, _, status := getSessionList(server, "/v1/sessions?vintage=alpha&flavor=sweet&cursor="+body.NextCursor, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body2.Items).NotTo(BeEmpty())
	})

	It("rejects a cursor minted under a different filter set", func() {
		first := record
		first.SortVal = "2026-06-01 12:10:00+00"
		second := record
		second.ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
		second.SortVal = "2026-06-01 12:09:00+00"
		drv := &sessionsStubDriver{
			Driver:      inmemory.NewDriver(),
			listRecords: []storage.SessionRecord{first, second},
		}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))

		// Mint through the real endpoint so the binding is the one the
		// handler actually writes.
		body, _, status := getSessionList(server, "/v1/sessions?vintage=alpha&limit=1", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.NextCursor).NotTo(BeEmpty())
		minted, err := decodeSessionsCursor(body.NextCursor)
		Expect(err).NotTo(HaveOccurred())
		Expect(minted.Filters).To(Equal([]string{"vintage=alpha"}),
			"the cursor binds the folded, sorted, param-qualified filter set")
		mintCalls := drv.listCalls

		// A different filter set is rejected through the same cursor-context
		// path, before any storage call.
		_, errBody, status := getSessionList(server, "/v1/sessions?vintage=beta&cursor="+body.NextCursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))
		Expect(drv.listCalls).To(Equal(mintCalls), "the mismatch must be rejected before any storage call")

		// So is presenting a filter-bound cursor with no claimed filter at all.
		_, errBody, status = getSessionList(server, "/v1/sessions?cursor="+body.NextCursor, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("cursor"))

		// The same set paginates on.
		body2, _, status := getSessionList(server, "/v1/sessions?vintage=alpha&cursor="+body.NextCursor, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body2.Items).NotTo(BeEmpty())
		Expect(drv.lastCursorVal).NotTo(BeNil())
	})

	It("applies the claimed predicate to the harness point lookup instead of rejecting it", func() {
		drv := &sessionsStubDriver{
			Driver:        inmemory.NewDriver(),
			harnessRecord: &record,
			matcherResult: false,
		}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))

		body, _, status := getSessionList(server,
			"/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&vintage=alpha", "")
		Expect(status).To(Equal(fiber.StatusOK), "the combination is filtered, never rejected")
		Expect(body.Items).NotTo(BeNil())
		Expect(body.Items).To(BeEmpty(), "a non-matching looked-up row yields empty items")
		Expect(drv.matcherCalls).To(Equal(1), "the predicate evaluates through the SQL matcher")
		Expect(drv.lastMatcherID).To(Equal(record.ID))
		Expect(drv.lastMatcherFilter.Values).To(Equal([]string{"alpha"}))

		drv.matcherResult = true
		body, _, status = getSessionList(server,
			"/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&vintage=alpha", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].ID).To(Equal(record.ID))

		// The matcher failing is claimed-but-broken: loud on this path too.
		drv.matcherErr = errors.New("view gone")
		_, _, status = getSessionList(server,
			"/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&vintage=alpha", "")
		Expect(status).To(Equal(fiber.StatusInternalServerError))
	})

	It("evaluates every active claim on the harness point lookup", func() {
		drv := &sessionsStubDriver{Driver: inmemory.NewDriver(), harnessRecord: &record, matcherResult: true}
		server := newSessionsServer(drv)
		installSessionsClaim(server, claimTestManifest(fullNormalizeJSON))
		installNamedClaim(server, "otherpub", flavorClaimManifest())

		body, _, status := getSessionList(server,
			"/v1/sessions?harness_id=claude&harness_session_id=sess-xyz&vintage=alpha&flavor=sweet", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body.Items).To(HaveLen(1))
		Expect(drv.matcherCalls).To(Equal(2),
			"one SQL point probe per active claim — both must be evaluated")
		Expect(drv.lastMatcherFilter.View.String()).To(Equal("testpub.attachments"),
			"the second filter (cassette-name order) is the last one evaluated")
	})

	It("leaves the unfiltered query plan and results unchanged when no claim param is sent", func() {
		claimed := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		claimedServer := newSessionsServer(claimed)
		installSessionsClaim(claimedServer, claimTestManifest(fullNormalizeJSON))

		unclaimed := &sessionsStubDriver{Driver: inmemory.NewDriver(), listRecords: []storage.SessionRecord{record}}
		unclaimedServer := newSessionsServer(unclaimed)

		claimedBody, status := getRawSessionList(claimedServer, "/v1/sessions")
		Expect(status).To(Equal(fiber.StatusOK))
		baseline, status := getRawSessionList(unclaimedServer, "/v1/sessions")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(claimedBody).To(Equal(baseline),
			"claims active but not invoked must cost nothing: pre-feature baseline behavior")
		Expect(claimed.lastClaimedFilters).To(BeEmpty(),
			"storage receives no filter — no view access on the unfiltered path")
	})
})

// normalizationCorpus is the shared normalization test-vector corpus (CC-10).
// The publishing cassette's key derivation must produce byte-identical
// results for these same vectors; if either implementation drifts, its copy
// of this corpus fails. Keep the vectors in sync with the publishing cassette's copy.
var normalizationCorpus = []struct {
	name string
	raw  string
	key  string
}{
	// Basic casing and trimming.
	{"ascii lowercase is untouched", "bug", "bug"},
	{"ascii uppercase folds", "Bug", "bug"},
	{"shortcode text folds around the colons", ":bug: BUG", ":bug: bug"},
	{"surrounding whitespace trims", "  padded  ", "padded"},
	{"interior whitespace is preserved", "two  spaces", "two  spaces"},
	{"unicode whitespace trims", " nbsp ", "nbsp"},

	// Emoji and symbols: never cased, never folded away.
	{"emoji is preserved verbatim", "\U0001F525 HOT", "\U0001F525 hot"},
	{"emoji variation selector is preserved", "❤️", "❤️"},
	{"bare heart stays distinct from the emoji presentation", "❤", "❤"},

	// NFC: combining sequences compose to one canonical form.
	{"combining acute composes", "Café", "café"},
	{"precomposed and combining forms derive one key", "Café", "café"},

	// Simple casefold edge cases.
	{"kelvin sign folds to k", "K", "k"},
	{"long s folds to s", "ſ", "s"},
	{"final sigma folds to sigma", "οδος", "οδοσ"},
	{"capital sigma folds to sigma", "ΟΔΟΣ", "οδοσ"},
	{"micro sign folds to greek mu", "µ", "μ"},
	{"sharp s is preserved (simple fold, not full)", "straße", "straße"},
	{"capital sharp s folds to sharp s", "STRAẞE", "straße"},

	// Turkic dotted/dotless i: conservative profile keeps them distinct.
	{"dotted capital I folds to itself", "İstanbul", "İstanbul"},
	{"dotless i folds to itself", "ı", "ı"},
	{"ascii I folds to ascii i", "I", "i"},

	// Compatibility variants deliberately remain distinct values (Q13).
	{"ligature fi is not decomposed", "ﬁle", "ﬁle"},
	{"full-width letters case-fold but stay full-width", "ＦＵＬＬ", "ｆｕｌｌ"},
}

var _ = Describe("normalization verbs for claimed filter values", func() {
	It("matches the shared corpus byte-for-byte in declared order", func() {
		for _, vector := range normalizationCorpus {
			normalized, err := applyNormalizeVerbs([]string{"trim", "nfc", "casefold"}, vector.raw)
			Expect(err).NotTo(HaveOccurred(), vector.name)
			Expect(normalized).To(Equal(vector.key), vector.name)
		}
	})

	It("refuses a verb outside the admitted vocabulary", func() {
		_, err := applyNormalizeVerbs([]string{"trim", "lowercase"}, "x")
		Expect(err).To(MatchError(ContainSubstring("lowercase")),
			"an unknown verb is a claim core cannot execute, never a silent skip")
	})
})

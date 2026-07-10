package postgres_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// idsOf extracts just the IDs from a slice of SessionRecord.
func idsOf(recs []storage.SessionRecord) []string {
	ids := make([]string, len(recs))
	for i, r := range recs {
		ids[i] = r.ID
	}
	return ids
}

var _ = Describe("Driver.GetSessionRecordByHarness", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ingester storage.SessionIngester
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		ingester, ok = driver.(storage.SessionIngester)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionIngester")
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// seedSession ingests a 2-node turn for the given org and harness
	// identity, returning the tapes-minted session UUID. The text seeds
	// the user message so it becomes the session's preview.
	seedSession := func(orgID, harnessID, harnessSessionID, text string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subject-reads",
				HarnessID:        harnessID,
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture(text),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	It("returns the matching record for an exact org-scoped natural key", func() {
		orgID := newTestOrgID()
		sessionID := seedSession(orgID, "claude", "harness-exact", "preview text for exact match")

		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, "claude", "harness-exact")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())

		Expect(rec.ID).To(Equal(sessionID))
		Expect(rec.HarnessID).To(Equal("claude"))
		Expect(rec.HarnessSessionID).To(Equal("harness-exact"))
		// Token/turn/cost counters are no longer folded by IngestTurn — they
		// are owned by the derive-time span fold (FoldSessionRollupsFromSpans).
		// Preview is likewise a derived-surface value (span_turns.user_prompt,
		// covered by the derive specs). Neither is populated by a bare ingest
		// that does not run the deriver, so they stay at their zero values.
		Expect(rec.TurnCount).To(Equal(0))
		Expect(rec.TotalInputTokens).To(Equal(int64(0)))
		Expect(rec.TotalOutputTokens).To(Equal(int64(0)))
		Expect(rec.Preview).To(BeEmpty())

		// Parity with the list path: the single filtered row carries the
		// same field population as a ListSessionRecords row.
		listed, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10})
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(HaveLen(1))
		Expect(listed[0].ID).To(Equal(rec.ID))
		Expect(listed[0].Preview).To(Equal(rec.Preview))
	})

	It("returns nil without error when no row matches the natural key", func() {
		orgID := newTestOrgID()
		// Seed an unrelated session so the miss exercises the index
		// against real rows rather than an empty table.
		seedSession(orgID, "claude", "harness-present", "some other session")

		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, "claude", "harness-absent")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil())
	})

	It("does not return a session with the same harness identity under a different org", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()

		// Identical (harness_id, harness_session_id) pair seeded under
		// two different orgs; the unique index is only unique per-org.
		idA := seedSession(orgA, "claude", "shared-harness-session", "org A turn")
		idB := seedSession(orgB, "claude", "shared-harness-session", "org B turn")
		Expect(idA).NotTo(Equal(idB))

		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgA, "claude", "shared-harness-session")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.ID).To(Equal(idA))
		Expect(rec.ID).NotTo(Equal(idB), "org B's session UUID must never surface for org A")
	})

	It("filters the paged list by auth_subject within an org", func() {
		// Given two sessions in one org captured for different users
		orgID := newTestOrgID()
		seedFor := func(subject, harnessSession, text string) string {
			res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
				Session: &sessions.IngestEnvelope{
					OrgID:            orgID,
					AuthSubject:      subject,
					HarnessID:        "claude",
					HarnessSessionID: harnessSession,
				},
				Nodes: sessionFixture(text),
			})
			Expect(err).NotTo(HaveOccurred())
			return res.SessionID
		}
		idAlice := seedFor("user_alice", "sess-alice", "alice turn")
		_ = seedFor("user_bob", "sess-bob", "bob turn")

		// When listing with alice's subject
		mine, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{AuthSubject: "user_alice", Limit: 10})
		Expect(err).NotTo(HaveOccurred())

		// Then only alice's session returns, carrying the subject
		Expect(mine).To(HaveLen(1))
		Expect(mine[0].ID).To(Equal(idAlice))
		Expect(mine[0].AuthSubject).To(Equal("user_alice"))

		// And the unfiltered list still returns both users' sessions
		all, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(2))
	})

	It("does not match case-folded, trimmed, or prefix variants of the harness ids", func() {
		orgID := newTestOrgID()
		seedSession(orgID, "Claude-Code", "Sess-ABC-123", "variant matching")

		// Sanity anchor: the exact stored triple hits.
		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, "Claude-Code", "Sess-ABC-123")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())

		variants := []struct {
			desc             string
			harnessID        string
			harnessSessionID string
		}{
			{"case-folded harness_id", "claude-code", "Sess-ABC-123"},
			{"case-folded harness_session_id", "Claude-Code", "sess-abc-123"},
			{"case-folded both", "CLAUDE-CODE", "SESS-ABC-123"},
			{"whitespace-padded harness_id", " Claude-Code ", "Sess-ABC-123"},
			{"whitespace-padded harness_session_id", "Claude-Code", " Sess-ABC-123 "},
			{"prefix of harness_id", "Claude", "Sess-ABC-123"},
			{"prefix of harness_session_id", "Claude-Code", "Sess-ABC"},
		}
		for _, v := range variants {
			rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, v.harnessID, v.harnessSessionID)
			Expect(err).NotTo(HaveOccurred(), v.desc)
			Expect(rec).To(BeNil(), "%s must not match the stored natural key", v.desc)
		}
	})
})

var _ = Describe("Driver.ListSessionRecords (dynamic sort)", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ingester storage.SessionIngester
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		ingester, ok = driver.(storage.SessionIngester)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionIngester")
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// seedWithCost ingests a minimal turn for the given org and harness
	// identity with the specified cost, returning the session UUID.
	seedWithCost := func(orgID, harnessSessionID string, cost float64) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subject-sort",
				HarnessID:        "claude",
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture("turn for " + harnessSessionID),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	It("sorts by total_cost_usd ascending with a stable id tiebreak", func() {
		orgID := newTestOrgID()
		// seed three sessions: costs 0.10, 0.30, 0.30 (tie)
		_ = seedWithCost(orgID, "sess-cheap", 0.10)
		_ = seedWithCost(orgID, "sess-tie-a", 0.30)
		_ = seedWithCost(orgID, "sess-tie-b", 0.30)

		page1, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Sort: storage.SortTotalCost, Dir: storage.SortAsc, Limit: 2,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(page1).To(HaveLen(2))
		// cheapest session must come first
		Expect(page1[0].TotalCostUsd).To(BeNumerically("==", 0.10))
		// SortVal must be populated
		Expect(page1[0].SortVal).NotTo(BeEmpty())
		Expect(page1[1].SortVal).NotTo(BeEmpty())

		// keyset cursor: page2 must not repeat any page1 row
		last := page1[len(page1)-1]
		page2, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Sort:      storage.SortTotalCost,
			Dir:       storage.SortAsc,
			Limit:     2,
			CursorVal: &last.SortVal,
			CursorID:  &last.ID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(page2).To(HaveLen(1), "one tied row must appear on page2")
		// the tie is split deterministically by id; no page1 row reappears
		for _, id := range idsOf(page1) {
			Expect(idsOf(page2)).NotTo(ContainElement(id),
				"page2 must not repeat any row from page1")
		}
	})

	It("returns sessions in descending last_active order by default", func() {
		orgID := newTestOrgID()
		// ingest two sessions; DB assigns last_seen_at via the upsert
		_ = seedWithCost(orgID, "sess-first", 0.05)
		_ = seedWithCost(orgID, "sess-second", 0.05)

		all, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(2))
		// default sort is last_seen_at DESC; more-recently upserted row is first
		Expect(all[0].LastSeenAt).To(BeTemporally(">=", all[1].LastSeenAt))
		// SortVal is populated even for the default sort
		Expect(all[0].SortVal).NotTo(BeEmpty())
	})

	// seedVaried ingests one turn with explicitly varied counters and subject so
	// several sort columns end up with distinct values (cost, tokens, subject,
	// last_seen_at); columns that tie (turn_count, derived_status, duration_ns)
	// fall through to the id tiebreak. Returns the session UUID.
	seedVaried := func(orgID, harnessSessionID, authSubject string, inTok, outTok int64, cost float64) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      authSubject,
				HarnessID:        "claude",
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture("turn for " + harnessSessionID),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	// collectAllPages walks the keyset-paginated list for one sort/direction
	// with a tiny page size, returning every id seen across pages in order. The
	// loop bound is a runaway guard: if the cursor ever stops advancing, the
	// walk would otherwise spin forever.
	collectAllPages := func(orgID string, sort storage.SessionSortField, dir storage.SortDirection) []string {
		var ids []string
		var cursorVal, cursorID *string
		for i := range 100 {
			page, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
				Sort: sort, Dir: dir, Limit: 2,
				CursorVal: cursorVal, CursorID: cursorID,
			})
			Expect(err).NotTo(HaveOccurred(), "sort=%s dir=%s page=%d", sort, dir, i)
			if len(page) == 0 {
				return ids
			}
			ids = append(ids, idsOf(page)...)
			last := page[len(page)-1]
			v, id := last.SortVal, last.ID
			cursorVal, cursorID = &v, &id
		}
		Fail("pagination did not terminate for sort=" + string(sort) + " dir=" + string(dir))
		return ids
	}

	// The keyset contract — every row exactly once, no duplicates, no drops —
	// must hold for every sortable column in both directions, not just the
	// total_cost_usd path the cases above cover. This walks the full set through
	// a 2-row page window for all 8 fields × {asc,desc} and asserts the set
	// returned equals the seeded set. Catches a broken cursor predicate, a wrong
	// cast type, or a sort_val that doesn't round-trip for any one field.
	It("paginates every sortable column in both directions with no dupes or drops", func() {
		orgID := newTestOrgID()
		want := map[string]bool{
			seedVaried(orgID, "s1", "subj-a", 10, 5, 0.10):  true,
			seedVaried(orgID, "s2", "subj-b", 40, 1, 0.30):  true,
			seedVaried(orgID, "s3", "subj-c", 20, 20, 0.05): true,
			seedVaried(orgID, "s4", "subj-d", 5, 50, 0.30):  true,
			seedVaried(orgID, "s5", "subj-e", 99, 0, 0.20):  true,
		}
		Expect(want).To(HaveLen(5), "seeded sessions must be distinct rows")

		fields := []storage.SessionSortField{
			storage.SortLastActive, storage.SortStartedAt, storage.SortTurnCount,
			storage.SortTotalCost, storage.SortTotalTokens, storage.SortDurationNs,
			storage.SortDerivedStatus, storage.SortAuthSubject,
		}
		dirs := []storage.SortDirection{storage.SortAsc, storage.SortDesc}

		for _, f := range fields {
			for _, d := range dirs {
				ids := collectAllPages(orgID, f, d)

				seen := map[string]int{}
				for _, id := range ids {
					seen[id]++
				}
				for id, n := range seen {
					Expect(n).To(Equal(1), "sort=%s dir=%s row %s appeared %d times", f, d, id, n)
				}
				Expect(ids).To(HaveLen(len(want)),
					"sort=%s dir=%s must return every seeded row exactly once", f, d)
				for id := range seen {
					Expect(want).To(HaveKey(id), "sort=%s dir=%s returned unexpected id %s", f, d, id)
				}
			}
		}
	})
})

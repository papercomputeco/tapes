package postgres_test

import (
	"context"
	"strings"
	"time"

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
		dsn := testPostgresDSN
		var err error

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

	It("windows the list on turns started in the window, matching /v1/stats", func() {
		// The since/until window is an EXISTS over span_turns started in
		// range — the same predicate AggregateSpanStats matches — so the
		// list row set equals the stat strip's session set for a window.
		// Bare ingest writes no span_turns, so the window is driven entirely
		// by these planted turn rows.
		orgID := newTestOrgID()
		plantTurn := func(sessionID, traceID string, startedAt time.Time) {
			_, err := pgDriver.DB().Exec(ctx, `
				INSERT INTO span_turns_20260615 (org_id, trace_id, session_id, started_at)
				VALUES ($1::uuid, $2, $3::uuid, $4::timestamptz)`,
				orgID, traceID, sessionID, startedAt)
			Expect(err).NotTo(HaveOccurred())
		}

		now := time.Now().UTC()
		recent := seedSession(orgID, "claude", "sess-recent", "recent turn")
		plantTurn(recent, "trace-recent", now.Add(-1*time.Hour)) // inside a 24h window
		old := seedSession(orgID, "claude", "sess-old", "old turn")
		plantTurn(old, "trace-old", now.Add(-48*time.Hour)) // before a 24h window
		// Ingested but never derived: no span_turn, so out of every window
		// even though the session row exists.
		undived := seedSession(orgID, "claude", "sess-undived", "no derived turn")

		// Unwindowed: every session in the org, derived or not.
		all, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(all)).To(ConsistOf(recent, old, undived))

		// Windowed to the last 24h: only the session whose turn started in
		// the window; the older-turn and never-derived sessions drop out.
		since := now.Add(-24 * time.Hour)
		windowed, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10, Since: &since})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(windowed)).To(ConsistOf(recent))

		// The exclusive upper bound drops a turn that starts exactly at
		// `until` and keeps only the strictly-earlier one.
		until := now.Add(-30 * time.Minute)
		earlier := now.Add(-90 * time.Minute)
		bounded, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit: 10, Since: &earlier, Until: &until,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(bounded)).To(ConsistOf(recent)) // trace-recent at -1h is in [-90m, -30m)
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

var _ = Describe("Driver.ListSessionRecordsByHarnessSessionID", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ingester storage.SessionIngester
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn := testPostgresDSN
		var err error

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

	seedSession := func(orgID, harnessID, harnessSessionID, text string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subject-lone-id",
				HarnessID:        harnessID,
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture(text),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	It("matches the exact harness_session_id without a harness_id, org-scoped", func() {
		orgID := newTestOrgID()
		otherOrg := newTestOrgID()
		wanted := seedSession(orgID, "claude", "lone-sess-1", "the session we want")
		seedSession(orgID, "claude", "lone-sess-other", "an unrelated session")
		// Same harness identity under a different org must never surface.
		seedSession(otherOrg, "claude", "lone-sess-1", "other org's session")

		recs, err := pgDriver.ListSessionRecordsByHarnessSessionID(ctx, orgID, "lone-sess-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(recs).To(HaveLen(1))
		Expect(recs[0].ID).To(Equal(wanted))
		Expect(recs[0].HarnessID).To(Equal("claude"))
		Expect(recs[0].HarnessSessionID).To(Equal("lone-sess-1"))
	})

	It("returns one row per harness when the id collides across harnesses, ordered by harness_id", func() {
		orgID := newTestOrgID()
		idClaude := seedSession(orgID, "claude", "lone-shared", "claude's turn")
		idCodex := seedSession(orgID, "codex", "lone-shared", "codex's turn")
		Expect(idClaude).NotTo(Equal(idCodex))

		recs, err := pgDriver.ListSessionRecordsByHarnessSessionID(ctx, orgID, "lone-shared")
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(recs)).To(Equal([]string{idClaude, idCodex}),
			"both harnesses' rows must return, ordered by harness_id")
	})

	It("returns an empty slice without error when nothing matches", func() {
		orgID := newTestOrgID()
		// Seed an unrelated session so the miss runs against real rows.
		seedSession(orgID, "claude", "lone-present", "some other session")

		recs, err := pgDriver.ListSessionRecordsByHarnessSessionID(ctx, orgID, "lone-absent")
		Expect(err).NotTo(HaveOccurred())
		Expect(recs).To(BeEmpty())
	})

	It("does not match case-folded, trimmed, or prefix variants of the id", func() {
		orgID := newTestOrgID()
		seedSession(orgID, "claude", "Lone-Sess-ABC", "variant matching")

		for _, variant := range []string{"lone-sess-abc", "LONE-SESS-ABC", " Lone-Sess-ABC ", "Lone-Sess"} {
			recs, err := pgDriver.ListSessionRecordsByHarnessSessionID(ctx, orgID, variant)
			Expect(err).NotTo(HaveOccurred(), variant)
			Expect(recs).To(BeEmpty(), "%q must not match the stored id", variant)
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
		dsn := testPostgresDSN
		var err error

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

	// seedWithCost ingests a minimal turn for the given org and harness
	// identity, then stamps the requested cost onto the session row.
	// Ingest no longer folds counters (the derive-time span fold owns
	// them), and this suite exercises the sort/pagination SQL — not the
	// fold — so writing the rollup directly keeps the test focused.
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

		_, err = pgDriver.DB().Exec(ctx,
			"UPDATE sessions SET total_cost_usd = $1 WHERE id = $2::uuid", cost, res.SessionID)
		Expect(err).NotTo(HaveOccurred())
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

var _ = Describe("Driver.GetSessionRecord preview", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn := testPostgresDSN
		var err error
		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	insertSession := func(orgID, id, harnessSessionID string) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'subj', 'claude', $3, NOW(), NOW())`, id, orgID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	}
	insertTurn := func(orgID, id, traceID, prompt, synthetic string, offset time.Duration) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615 (org_id, trace_id, session_id, user_prompt, synthetic, started_at)
			VALUES ($1, $2, $3, $4, $5, NOW() + $6)`, orgID, traceID, id, prompt, synthetic, offset)
		Expect(err).NotTo(HaveOccurred())
	}

	// K + M together: the detail endpoint attaches the preview at all, and
	// picks the first GENUINE turn rather than a shadow-only opener.
	It("attaches the first non-synthetic, non-empty turn's prompt on the detail read", func() {
		orgID := newTestOrgID()
		const id = "01900000-0000-7000-8000-0000000000c1"
		insertSession(orgID, id, "sess-preview")
		// Earliest turn is a shadow-only opener (synthetic, empty prompt); the
		// user's real prompt is the later, genuine turn.
		insertTurn(orgID, id, "trc-1", "", "shadow-opener", 0)
		insertTurn(orgID, id, "trc-2", "the real first question", "", time.Second)

		rec, err := pgDriver.GetSessionRecord(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.Preview).To(Equal("the real first question"),
			"detail must attach the preview (K) and skip the shadow opener (M)")
	})

	It("falls back to an empty preview when the session has no genuine turn", func() {
		orgID := newTestOrgID()
		const id = "01900000-0000-7000-8000-0000000000c2"
		insertSession(orgID, id, "sess-empty")
		insertTurn(orgID, id, "trc-1", "", "shadow-opener", 0)

		rec, err := pgDriver.GetSessionRecord(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.Preview).To(BeEmpty())
	})

	It("classifies valid JSON before truncating its preview", func() {
		orgID := newTestOrgID()
		const id = "01900000-0000-7000-8000-0000000000c3"
		insertSession(orgID, id, "sess-json-preview")
		prompt := `{"payload":"` + strings.Repeat("x", 160) + `"}`
		insertTurn(orgID, id, "trc-1", prompt, "", 0)

		rec, err := pgDriver.GetSessionRecord(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect([]rune(rec.Preview)).To(HaveLen(120))
		Expect(rec.PreviewIsJSON).To(BeTrue())
	})
})

var _ = Describe("Driver.ListSessionRecords (published-view filter)", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ingester storage.SessionIngester
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		ingester, ok = driver.(storage.SessionIngester)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionIngester")

		// The fixture published view core's generic mechanism is exercised
		// against. Its column DDL — (primitive_type text, primitive_id text,
		// value text) — is textually identical to the publishing cassette's
		// own view-shape contract pin; the fixture here and that pin are the
		// two halves of one contract, so a drift in the published shape fails
		// the cassette repo's pin and keeps this fixture honest. Core tests
		// deliberately never depend on any real cassette.
		for _, statement := range []string{
			`CREATE SCHEMA IF NOT EXISTS testpub`,
			`CREATE TABLE IF NOT EXISTS testpub.fixture_attachments (
				primitive_type text NOT NULL,
				primitive_id   text NOT NULL,
				value          text NOT NULL
			)`,
			`TRUNCATE testpub.fixture_attachments`,
			`CREATE OR REPLACE VIEW testpub.attachments AS
				SELECT a.primitive_type, a.primitive_id, a.value
				FROM testpub.fixture_attachments a`,
		} {
			_, err = pgDriver.DB().Exec(ctx, statement)
			Expect(err).NotTo(HaveOccurred(), statement)
		}
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	seedSession := func(orgID, harnessSessionID, subject string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      subject,
				HarnessID:        "claude",
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture("turn for " + harnessSessionID),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	attach := func(primitiveID, value string) {
		_, err := pgDriver.DB().Exec(ctx,
			"INSERT INTO testpub.fixture_attachments (primitive_type, primitive_id, value) VALUES ('session', $1, $2)",
			primitiveID, value)
		Expect(err).NotTo(HaveOccurred())
	}

	filterFor := func(values ...string) *storage.PublishedFilter {
		view, err := storage.ParsePublishedViewName("testpub.attachments")
		Expect(err).NotTo(HaveOccurred())
		column, err := storage.ParsePublishedColumnName("value")
		Expect(err).NotTo(HaveOccurred())
		return &storage.PublishedFilter{View: view, TypeValue: "session", Column: column, Values: values}
	}

	filtersFor := func(values ...string) []storage.PublishedFilter {
		return []storage.PublishedFilter{*filterFor(values...)}
	}

	// A second published view whose value lives in a column named tag — the
	// non-default shape the declared match.value_column exists for.
	ensureFlavorsView := func() {
		for _, statement := range []string{
			`CREATE TABLE IF NOT EXISTS testpub.fixture_flavors (
				primitive_type text NOT NULL,
				primitive_id   text NOT NULL,
				tag            text NOT NULL
			)`,
			`TRUNCATE testpub.fixture_flavors`,
			`CREATE OR REPLACE VIEW testpub.flavors AS
				SELECT f.primitive_type, f.primitive_id, f.tag
				FROM testpub.fixture_flavors f`,
		} {
			_, err := pgDriver.DB().Exec(ctx, statement)
			Expect(err).NotTo(HaveOccurred(), statement)
		}
	}

	attachFlavor := func(primitiveID, tag string) {
		_, err := pgDriver.DB().Exec(ctx,
			"INSERT INTO testpub.fixture_flavors (primitive_type, primitive_id, tag) VALUES ('session', $1, $2)",
			primitiveID, tag)
		Expect(err).NotTo(HaveOccurred())
	}

	flavorFilterFor := func(values ...string) storage.PublishedFilter {
		view, err := storage.ParsePublishedViewName("testpub.flavors")
		Expect(err).NotTo(HaveOccurred())
		column, err := storage.ParsePublishedColumnName("tag")
		Expect(err).NotTo(HaveOccurred())
		return storage.PublishedFilter{View: view, TypeValue: "session", Column: column, Values: values}
	}

	It("filters the paged list through an EXISTS on the published view", func() {
		orgID := newTestOrgID()
		tagged1 := seedSession(orgID, "pf-tagged-1", "subject-pf")
		tagged2 := seedSession(orgID, "pf-tagged-2", "subject-pf")
		plain := seedSession(orgID, "pf-plain", "subject-pf")
		attach(tagged1, "alpha")
		attach(tagged2, "alpha")
		// An attachment for a different primitive type must never leak into
		// the session filter even when its id collides.
		_, err := pgDriver.DB().Exec(ctx,
			"INSERT INTO testpub.fixture_attachments (primitive_type, primitive_id, value) VALUES ('other_thing', $1, $2)",
			plain, "alpha")
		Expect(err).NotTo(HaveOccurred())

		filtered, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit: 10, ClaimedFilters: filtersFor("alpha"),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(filtered)).To(ConsistOf(tagged1, tagged2))
		// The filtered page keeps the default sort contract intact.
		Expect(filtered[0].LastSeenAt).To(BeTemporally(">=", filtered[1].LastSeenAt))
		Expect(filtered[0].SortVal).NotTo(BeEmpty())

		all, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10})
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(3), "no filter, no predicate — the mechanism costs nothing unless invoked")
	})

	It("ANDs repeated values of a claimed param", func() {
		orgID := newTestOrgID()
		onlyA := seedSession(orgID, "pf-only-a", "subject-pf")
		onlyB := seedSession(orgID, "pf-only-b", "subject-pf")
		both := seedSession(orgID, "pf-both", "subject-pf")
		attach(onlyA, "alpha")
		attach(onlyB, "beta")
		attach(both, "alpha")
		attach(both, "beta")

		filtered, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit: 10, ClaimedFilters: filtersFor("alpha", "beta"),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(filtered)).To(ConsistOf(both),
			"every returned session must carry all supplied values")
	})

	It("composes the claimed filter with window, subject, sort, and keyset cursor in one query", func() {
		orgID := newTestOrgID()
		now := time.Now().UTC()
		plantTurn := func(sessionID, traceID string, startedAt time.Time) {
			_, err := pgDriver.DB().Exec(ctx, `
				INSERT INTO span_turns_20260615 (org_id, trace_id, session_id, started_at)
				VALUES ($1::uuid, $2, $3::uuid, $4::timestamptz)`,
				orgID, traceID, sessionID, startedAt)
			Expect(err).NotTo(HaveOccurred())
		}
		setCost := func(sessionID string, cost float64) {
			_, err := pgDriver.DB().Exec(ctx,
				"UPDATE sessions SET total_cost_usd = $1 WHERE id = $2::uuid", cost, sessionID)
			Expect(err).NotTo(HaveOccurred())
		}

		cheap := seedSession(orgID, "pf-cheap", "subject-pf")
		attach(cheap, "alpha")
		plantTurn(cheap, "trc-cheap", now.Add(-time.Hour))
		setCost(cheap, 0.10)

		pricey := seedSession(orgID, "pf-pricey", "subject-pf")
		attach(pricey, "alpha")
		plantTurn(pricey, "trc-pricey", now.Add(-2*time.Hour))
		setCost(pricey, 0.30)

		otherSubject := seedSession(orgID, "pf-other-subject", "subject-other")
		attach(otherSubject, "alpha")
		plantTurn(otherSubject, "trc-other-subject", now.Add(-time.Hour))
		setCost(otherSubject, 0.20)

		outOfWindow := seedSession(orgID, "pf-out-of-window", "subject-pf")
		attach(outOfWindow, "alpha")
		plantTurn(outOfWindow, "trc-out-of-window", now.Add(-48*time.Hour))
		setCost(outOfWindow, 0.05)

		untagged := seedSession(orgID, "pf-untagged", "subject-pf")
		plantTurn(untagged, "trc-untagged", now.Add(-time.Hour))
		setCost(untagged, 0.20)

		since := now.Add(-24 * time.Hour)
		base := storage.SessionListOpts{
			Sort:           storage.SortTotalCost,
			Dir:            storage.SortAsc,
			Limit:          1,
			Since:          &since,
			AuthSubject:    "subject-pf",
			ClaimedFilters: filtersFor("alpha"),
		}

		page1, err := pgDriver.ListSessionRecords(ctx, orgID, base)
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(page1)).To(Equal([]string{cheap}))
		Expect(page1[0].SortVal).NotTo(BeEmpty())

		next := base
		next.CursorVal = &page1[0].SortVal
		next.CursorID = &page1[0].ID
		page2, err := pgDriver.ListSessionRecords(ctx, orgID, next)
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(page2)).To(Equal([]string{pricey}),
			"the keyset must advance under the claimed filter without repeats or drops")

		last := next
		last.CursorVal = &page2[0].SortVal
		last.CursorID = &page2[0].ID
		page3, err := pgDriver.ListSessionRecords(ctx, orgID, last)
		Expect(err).NotTo(HaveOccurred())
		Expect(page3).To(BeEmpty(),
			"only the in-window, subject-matching, filter-matching sessions paginate")
	})

	It("quotes published view identifiers and binds all values as named args", func() {
		// The hostile side first: nothing that fails the grammar can even
		// become a PublishedViewName, so nothing hostile can reach the one
		// quoting helper.
		for _, hostile := range []string{
			`bad"name.view`,
			`testpub.attach"; DROP TABLE sessions; --`,
			"unqualified",
			"Testpub.attachments",
			"test-pub.attachments",
			"testpub.attachments.extra",
			strings.Repeat("a", 64) + ".attachments",
			"",
		} {
			_, err := storage.ParsePublishedViewName(hostile)
			Expect(err).To(HaveOccurred(), "%q must not parse into an identifier position", hostile)
		}

		view, err := storage.ParsePublishedViewName("testpub.attachments")
		Expect(err).NotTo(HaveOccurred())
		Expect(view.Quoted()).To(Equal(`"testpub"."attachments"`),
			"the one quoting helper renders both segments double-quoted")

		// The value column is the same kind of remote input reaching the same
		// identifier position, so it gets the same treatment: nothing that
		// fails the grammar can even become a PublishedColumnName.
		for _, hostileColumn := range []string{
			`value"; DROP TABLE sessions; --`,
			`va"lue`,
			"Value",
			"va lue",
			"value.extra",
			strings.Repeat("a", 64),
			"",
		} {
			_, err := storage.ParsePublishedColumnName(hostileColumn)
			Expect(err).To(HaveOccurred(), "%q must not parse into an identifier position", hostileColumn)
		}
		column, err := storage.ParsePublishedColumnName("value")
		Expect(err).NotTo(HaveOccurred())
		Expect(column.Quoted()).To(Equal(`"value"`),
			"the one quoting helper renders the column double-quoted")

		// The value side: SQL metacharacters travel as bound named args, so a
		// hostile value is an empty result, never an executed statement.
		orgID := newTestOrgID()
		tagged := seedSession(orgID, "pf-quoting", "subject-pf")
		attach(tagged, "alpha")
		for _, hostileValue := range []string{
			`x' OR '1'='1`,
			`alpha"; DROP TABLE sessions; --`,
			`alpha' UNION SELECT 1,1,1 --`,
		} {
			records, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
				Limit: 10, ClaimedFilters: filtersFor(hostileValue),
			})
			Expect(err).NotTo(HaveOccurred(), "a hostile value binds; it does not error")
			Expect(records).To(BeEmpty(), "and it matches nothing")
		}
		var count int
		Expect(pgDriver.DB().QueryRow(ctx, "SELECT count(*) FROM sessions").Scan(&count)).To(Succeed())
		Expect(count).To(Equal(1), "the sessions table survives every hostile value")
	})

	It("probes the claim-declared value column, never a hardcoded one", func() {
		ensureFlavorsView()
		orgID := newTestOrgID()
		sweet := seedSession(orgID, "pf-col-sweet", "subject-pf")
		plain := seedSession(orgID, "pf-col-plain", "subject-pf")
		attachFlavor(sweet, "sweet")

		flavorFilter := flavorFilterFor("sweet")
		records, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit: 10, ClaimedFilters: []storage.PublishedFilter{flavorFilter},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(records)).To(Equal([]string{sweet}),
			"the declared column must reach the probe; a hardcoded pv.value would error or match nothing")

		matched, err := pgDriver.MatchesPublishedFilter(ctx, &flavorFilter, sweet)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(BeTrue(), "the point lookup probes the same declared column")
		matched, err = pgDriver.MatchesPublishedFilter(ctx, &flavorFilter, plain)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(BeFalse())
	})

	It("ANDs filters from distinct claims inside one paginated query", func() {
		ensureFlavorsView()
		orgID := newTestOrgID()
		both := seedSession(orgID, "pf-and-both", "subject-pf")
		onlyAttached := seedSession(orgID, "pf-and-attached", "subject-pf")
		onlyFlavor := seedSession(orgID, "pf-and-flavor", "subject-pf")
		attach(both, "alpha")
		attach(onlyAttached, "alpha")
		attachFlavor(both, "sweet")
		attachFlavor(onlyFlavor, "sweet")

		records, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit:          10,
			ClaimedFilters: append(filtersFor("alpha"), flavorFilterFor("sweet")),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(idsOf(records)).To(Equal([]string{both}),
			"every supplied claim filters; a session missing either predicate must not appear")
	})

	It("errors on a filter whose value column was never parsed", func() {
		orgID := newTestOrgID()
		seedSession(orgID, "pf-zero-col", "subject-pf")
		view, err := storage.ParsePublishedViewName("testpub.attachments")
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit:          10,
			ClaimedFilters: []storage.PublishedFilter{{View: view, TypeValue: "session", Values: []string{"alpha"}}},
		})
		Expect(err).To(MatchError(ContainSubstring("value column")),
			"a zero column is an evaluation failure, never a hardcoded default")
	})

	It("errors instead of returning unfiltered rows when the view cannot be evaluated", func() {
		// The storage half of the claimed-but-broken contract: a filter that
		// cannot be evaluated is an error, never silently-unfiltered rows.
		orgID := newTestOrgID()
		seedSession(orgID, "pf-broken", "subject-pf")
		_, err := pgDriver.DB().Exec(ctx, "DROP VIEW testpub.attachments")
		Expect(err).NotTo(HaveOccurred())

		records, err := pgDriver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{
			Limit: 10, ClaimedFilters: filtersFor("alpha"),
		})
		Expect(err).To(HaveOccurred())
		Expect(records).To(BeEmpty(), "no rows may accompany the failure")
	})

	It("evaluates the published filter for a point lookup in SQL", func() {
		// MatchesPublishedFilter serves the harness natural-key path: the
		// predicate still runs as an indexed EXISTS in SQL — there is just no
		// list query to compose it into.
		orgID := newTestOrgID()
		tagged := seedSession(orgID, "pf-point-tagged", "subject-pf")
		plain := seedSession(orgID, "pf-point-plain", "subject-pf")
		attach(tagged, "alpha")

		matched, err := pgDriver.MatchesPublishedFilter(ctx, filterFor("alpha"), tagged)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(BeTrue())

		matched, err = pgDriver.MatchesPublishedFilter(ctx, filterFor("alpha"), plain)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(BeFalse())

		matched, err = pgDriver.MatchesPublishedFilter(ctx, filterFor("alpha", "beta"), tagged)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(BeFalse(), "AND semantics apply to the point lookup too")

		matched, err = pgDriver.MatchesPublishedFilter(ctx, nil, tagged)
		Expect(err).NotTo(HaveOccurred())
		Expect(matched).To(BeTrue(), "no filter matches everything")

		_, err = pgDriver.DB().Exec(ctx, "DROP VIEW testpub.attachments")
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.MatchesPublishedFilter(ctx, filterFor("alpha"), tagged)
		Expect(err).To(HaveOccurred(), "broken is loud on the point path as well")
	})
})

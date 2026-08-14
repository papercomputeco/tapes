package postgres_test

// The /v1/stats aggregate, and its subject filter.
//
// Attribution lives on sessions; the projection table the aggregate reads
// carries only a session_id. So "scope the stats to a user" is a join away
// from every total, and the risk this file exists for is a filter that
// reaches some of them but not others. Every figure, tool_calls included,
// now comes from the turn rollups in one CTE (PCC-936), so one predicate
// scopes them all — and each spec below asserts every field to pin that.

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver.AggregateSpanStats", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ingester storage.SessionIngester
		ctx      context.Context
		orgID    string
	)

	// One instant for everything, so the specs are about attribution and not
	// about the window. The window has its own coverage.
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		ingester, ok = driver.(storage.SessionIngester)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionIngester")

		// A fresh org per spec isolates these rows without truncating tables
		// other suites are using: every read here is org-scoped anyway.
		orgID = newTestOrgID()
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// seedSubjectSession ingests a session attributed to authSubject and
	// returns the tapes-minted session id. Ingest is what stamps
	// sessions.auth_subject from the gateway-verified JWT header, so going
	// through it is the point: the filter must match what capture actually
	// writes, not what a test inserted by hand.
	seedSubjectSession := func(authSubject, harnessSessionID string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      authSubject,
				HarnessID:        "claude",
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture("stats " + harnessSessionID),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	markCompleted := func(sessionID string) {
		_, err := pgDriver.DB().Exec(ctx,
			"UPDATE sessions SET derived_status = 'completed' WHERE id = $1", mustUUID(sessionID))
		Expect(err).NotTo(HaveOccurred())
	}

	// insertTurn writes one projection row directly. The deriver owns these
	// in production; writing them here lets a spec state the exact totals it
	// expects instead of reverse-engineering them from a fixture.
	insertTurn := func(traceID, sessionID string, in, out int64, costUSD float64, dur time.Duration, toolCalls int) {
		var sid any
		if sessionID == "" {
			sid = nil // an unattributed trace: no session row, so no subject
		} else {
			sid = mustUUID(sessionID)
		}
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615
			    (org_id, trace_id, session_id, started_at, duration_ns,
			     total_input_tokens, total_output_tokens, total_cost_usd, tool_calls)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			mustUUID(orgID), traceID, sid, base, dur.Nanoseconds(), in, out, costUSD, toolCalls)
		Expect(err).NotTo(HaveOccurred())
	}

	insertToolSpan := func(traceID, spanID, sessionID string) {
		var sid any
		if sessionID == "" {
			sid = nil
		} else {
			sid = mustUUID(sessionID)
		}
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, session_id, kind, started_at)
			VALUES ($1, $2, $3, $4, 'tool', $5)`,
			mustUUID(orgID), traceID, spanID, sid, base)
		Expect(err).NotTo(HaveOccurred())
	}

	// Two users in one org. alice has one completed session — one trace, one
	// tool call; bob has two traces on a single session and two tool calls,
	// and is deliberately not completed, so completed_count can distinguish
	// them rather than agreeing by coincidence.
	var aliceSession, bobSession string

	BeforeEach(func() {
		aliceSession = seedSubjectSession("user_alice", "alice-1")
		bobSession = seedSubjectSession("user_bob", "bob-1")
		markCompleted(aliceSession)

		insertTurn("trace-alice-1", aliceSession, 100, 10, 1.5, 2*time.Second, 1)
		insertToolSpan("trace-alice-1", "span-alice-tool-1", aliceSession)

		insertTurn("trace-bob-1", bobSession, 200, 20, 2.5, 3*time.Second, 1)
		insertTurn("trace-bob-2", bobSession, 400, 40, 4.0, 5*time.Second, 1)
		insertToolSpan("trace-bob-1", "span-bob-tool-1", bobSession)
		insertToolSpan("trace-bob-2", "span-bob-tool-2", bobSession)
	})

	It("sums both subjects when no subject is given", func() {
		stats, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "")
		Expect(err).NotTo(HaveOccurred())

		Expect(stats.TurnCount).To(Equal(3))
		Expect(stats.SessionCount).To(Equal(2))
		Expect(stats.CompletedCount).To(Equal(1))
		Expect(stats.InputTokens).To(Equal(int64(700)))
		Expect(stats.OutputTokens).To(Equal(int64(70)))
		Expect(stats.TotalCostUSD).To(BeNumerically("~", 8.0, 0.0001))
		Expect(stats.TotalDurationNS).To(Equal((10 * time.Second).Nanoseconds()))
		Expect(stats.ToolCalls).To(Equal(3))
	})

	It("narrows every total to the named subject", func() {
		stats, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "user_alice")
		Expect(err).NotTo(HaveOccurred())

		Expect(stats.TurnCount).To(Equal(1))
		Expect(stats.SessionCount).To(Equal(1))
		Expect(stats.CompletedCount).To(Equal(1))
		Expect(stats.InputTokens).To(Equal(int64(100)))
		Expect(stats.OutputTokens).To(Equal(int64(10)))
		Expect(stats.TotalCostUSD).To(BeNumerically("~", 1.5, 0.0001))
		Expect(stats.TotalDurationNS).To(Equal((2 * time.Second).Nanoseconds()))
		// tool_calls comes from the same matched CTE as everything else, so
		// the subject filter scopes it too; org-wide it is 3.
		Expect(stats.ToolCalls).To(Equal(1))
	})

	It("gives the other subject its own totals, not the remainder", func() {
		stats, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "user_bob")
		Expect(err).NotTo(HaveOccurred())

		Expect(stats.TurnCount).To(Equal(2))
		Expect(stats.SessionCount).To(Equal(1))
		// bob's session was never marked completed: a filter that leaked
		// alice's session into the completed count would show 1 here.
		Expect(stats.CompletedCount).To(BeZero())
		Expect(stats.InputTokens).To(Equal(int64(600)))
		Expect(stats.OutputTokens).To(Equal(int64(60)))
		Expect(stats.TotalCostUSD).To(BeNumerically("~", 6.5, 0.0001))
		Expect(stats.TotalDurationNS).To(Equal((8 * time.Second).Nanoseconds()))
		Expect(stats.ToolCalls).To(Equal(2))
	})

	It("returns zeros for a subject with no sessions, not an error", func() {
		// The new user's tray. Zeros are the honest answer and a 200; an
		// error here would read as a broken surface.
		stats, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "user_nobody")
		Expect(err).NotTo(HaveOccurred())

		Expect(stats).To(Equal(storage.SpanStats{}))
	})

	It("treats a blank-looking subject as a subject, not as absent", func() {
		// Widening to org-wide totals on a personal surface is the bug this
		// filter exists to fix, so a subject that trims to nothing must
		// aggregate to zeros rather than to everyone.
		stats, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "   ")
		Expect(err).NotTo(HaveOccurred())

		Expect(stats).To(Equal(storage.SpanStats{}))
	})

	It("counts an unattributed trace org-wide but for no subject", func() {
		// A trace with no session row has no subject, so it belongs to
		// nobody: it keeps counting in the org-wide totals it has always
		// counted in, and joins no user's. The filter tightening the LEFT
		// JOIN to an inner match is what makes the second half true.
		insertTurn("trace-orphan", "", 1_000, 100, 9.0, 7*time.Second, 0)

		orgWide, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(orgWide.TurnCount).To(Equal(4))
		Expect(orgWide.InputTokens).To(Equal(int64(1_700)))

		alice, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "user_alice")
		Expect(err).NotTo(HaveOccurred())
		Expect(alice.TurnCount).To(Equal(1))
		Expect(alice.InputTokens).To(Equal(int64(100)))
	})

	It("applies the subject and the window together", func() {
		// Each filter must survive the other. bob gets a second session far
		// outside the window; scoping to bob alone counts it, scoping to bob
		// within the window must not.
		outside := base.Add(-30 * 24 * time.Hour)
		late := seedSubjectSession("user_bob", "bob-2")
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615
			    (org_id, trace_id, session_id, started_at, duration_ns,
			     total_input_tokens, total_output_tokens, total_cost_usd)
			VALUES ($1, 'trace-bob-old', $2, $3, 0, 999, 99, 9.0)`,
			mustUUID(orgID), mustUUID(late), outside)
		Expect(err).NotTo(HaveOccurred())

		unwindowed, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "user_bob")
		Expect(err).NotTo(HaveOccurred())
		Expect(unwindowed.TurnCount).To(Equal(3))

		since := base.Add(-time.Hour)
		until := base.Add(time.Hour)
		windowed, err := pgDriver.AggregateSpanStats(ctx, orgID, &since, &until, "user_bob")
		Expect(err).NotTo(HaveOccurred())
		Expect(windowed.TurnCount).To(Equal(2))
		Expect(windowed.InputTokens).To(Equal(int64(600)))
	})

	It("keeps subjects from other orgs out of a subject's totals", func() {
		// auth_subject is unique to a person, not to a person within an org,
		// so a filter that forgot the org scope would still look right on a
		// single-org fixture. Give another org the same subject name.
		otherOrg := newTestOrgID()
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            otherOrg,
				AuthSubject:      "user_alice",
				HarnessID:        "claude",
				HarnessSessionID: "alice-elsewhere",
			},
			Nodes: sessionFixture("other org"),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615
			    (org_id, trace_id, session_id, started_at, duration_ns,
			     total_input_tokens, total_output_tokens, total_cost_usd, tool_calls)
			VALUES ($1, 'trace-alice-elsewhere', $2, $3, 0, 5000, 500, 50.0, 1)`,
			mustUUID(otherOrg), mustUUID(res.SessionID), base)
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, session_id, kind, started_at)
			VALUES ($1, 'trace-alice-elsewhere', 'span-elsewhere', $2, 'tool', $3)`,
			mustUUID(otherOrg), mustUUID(res.SessionID), base)
		Expect(err).NotTo(HaveOccurred())

		stats, err := pgDriver.AggregateSpanStats(ctx, orgID, nil, nil, "user_alice")
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.TurnCount).To(Equal(1))
		Expect(stats.InputTokens).To(Equal(int64(100)))
		Expect(stats.ToolCalls).To(Equal(1))
	})
})

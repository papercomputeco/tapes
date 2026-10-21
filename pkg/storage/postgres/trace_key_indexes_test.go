package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("trace key indexes [postgres]", func() {
	// Cassettes read the tapes_v1 views by the org-free identity — a span
	// as (trace_id, span_id), a turn as trace_id — and never name org_id.
	// spans_20260615_trace_span_idx and span_turns_20260615_trace_idx carry
	// exactly those keys, so a lookup through the view must be an index
	// probe; without them every org-led index is useless to the reader and
	// the lookup scans the whole projection.
	var (
		ctx       context.Context
		pgDriver  *postgres.Driver
		orgID     string
		sessionID string
	)

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		pgDriver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(pgDriver.Close)

		orgID = newTestOrgID()
		sessionID = uuid.New().String()
		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'test', 'claude', $3, $4, $4)`,
			mustUUID(sessionID), mustUUID(orgID), "trace-key-"+sessionID, base)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, err := pgDriver.DB().Exec(ctx, "DELETE FROM sessions WHERE id = $1", mustUUID(sessionID))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	// explain returns the plan for query, under the SSD planner costs a
	// deployment runs with; SET LOCAL in a rolled-back transaction leaves
	// the pooled connection as it was.
	explain := func(query string, args ...any) string {
		tx, err := pgDriver.DB().Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = tx.Exec(ctx, "SET LOCAL random_page_cost = 1.1")
		Expect(err).NotTo(HaveOccurred())
		rows, err := tx.Query(ctx, "EXPLAIN "+query, args...)
		Expect(err).NotTo(HaveOccurred())
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		Expect(err).NotTo(HaveOccurred())
		plan := strings.Join(lines, "\n")
		fmt.Fprintf(GinkgoWriter, "plan:\n%s\n", plan)
		return plan
	}

	It("probes spans and span turns by trace key through the views", func() {
		// Enough rows that the planner decides from statistics: on a few
		// pages it reads the table whole whatever indexes exist.
		const (
			traces        = 500
			spansPerTrace = 20
		)
		prefix := "tk-" + sessionID[:8] + "-"
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615
			    (org_id, trace_id, session_id, started_at, duration_ns,
			     total_input_tokens, total_output_tokens, total_cost_usd, tool_calls)
			SELECT $1, $5 || lpad(t::text, 4, '0'), $2, $3, 0, 0, 0, 0, 0
			FROM generate_series(1, $4) AS t`,
			mustUUID(orgID), mustUUID(sessionID), base, traces, prefix)
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, session_id, kind, started_at, seq, input)
			SELECT $1,
			       $6 || lpad((((g - 1) / $5) + 1)::text, 4, '0'),
			       'span-' || lpad(g::text, 6, '0'),
			       $2, 'llm',
			       $3::timestamptz + (g * interval '1 second'),
			       (g - 1) % $5,
			       '[{"type":"text","text":"i"}]'::jsonb
			FROM generate_series(1, $4) AS g`,
			mustUUID(orgID), mustUUID(sessionID), base, traces*spansPerTrace, spansPerTrace, prefix)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, err := pgDriver.DB().Exec(ctx, "DELETE FROM spans_20260615 WHERE session_id = $1", mustUUID(sessionID))
			Expect(err).NotTo(HaveOccurred())
			_, err = pgDriver.DB().Exec(ctx, "DELETE FROM span_turns_20260615 WHERE session_id = $1", mustUUID(sessionID))
			Expect(err).NotTo(HaveOccurred())
		})
		_, err = pgDriver.DB().Exec(ctx, "ANALYZE spans_20260615, span_turns_20260615")
		Expect(err).NotTo(HaveOccurred())

		spanPlan := explain(`SELECT * FROM tapes_v1.spans WHERE trace_id = $1 AND span_id = $2`,
			prefix+"0100", "span-002000")
		Expect(spanPlan).To(MatchRegexp(`(Index (Only )?Scan using|Bitmap Index Scan on) spans_20260615_trace_span_idx\b`))
		Expect(spanPlan).NotTo(ContainSubstring("Seq Scan"))

		turnPlan := explain(`SELECT * FROM tapes_v1.span_turns WHERE trace_id = $1`, prefix+"0100")
		Expect(turnPlan).To(MatchRegexp(`(Index (Only )?Scan using|Bitmap Index Scan on) span_turns_20260615_trace_idx\b`))
		Expect(turnPlan).NotTo(ContainSubstring("Seq Scan"))
	})
})

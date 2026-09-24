package postgres_test

// The payload-free half of a trace: the header with its span count and
// the links touching it. The streaming trace page reads these whole
// before it commits a status, then streams the spans, so each must be
// served without a span row being read — and must agree with the
// session-scoped summaries the lazy session detail is built from. Rows
// are stated directly, as in the iterator specs: the deriver owns them in
// production, but the property under test is the read.

import (
	"context"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("trace headers [postgres]", func() {
	var (
		driver    storage.Driver
		pgDriver  *postgres.Driver
		ctx       context.Context
		orgID     string
		sessionID string
	)

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())

		// A fresh org and session per spec; the projection rows hang off
		// the session through ON DELETE CASCADE, so the AfterEach delete
		// is enough.
		orgID = newTestOrgID()
		sessionID = uuid.New().String()
		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'test', 'claude', $3, $4, $4)`,
			mustUUID(sessionID), mustUUID(orgID), "hdr-"+sessionID, base)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if pgDriver != nil {
			_, err := pgDriver.DB().Exec(ctx, "DELETE FROM sessions WHERE id = $1", mustUUID(sessionID))
			Expect(err).NotTo(HaveOccurred())
		}
		if driver != nil {
			driver.Close()
		}
	})

	insertTurn := func(traceID string, startedAt time.Time) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615
			    (org_id, trace_id, session_id, user_prompt, started_at, duration_ns,
			     total_input_tokens, total_output_tokens, main_input_tokens, main_output_tokens,
			     cache_read_tokens, cache_creation_tokens, total_cost_usd, tool_calls)
			VALUES ($1, $2, $3, $4, $5, 7, 11, 5, 9, 4, 3, 2, 0.25, 1)`,
			mustUUID(orgID), traceID, mustUUID(sessionID), "prompt for "+traceID, startedAt)
		Expect(err).NotTo(HaveOccurred())
	}

	insertSpan := func(traceID, spanID string, seq int64) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, session_id, kind, started_at, seq, input)
			VALUES ($1, $2, $3, $4, 'tool', $5, $6, '[]'::jsonb)`,
			mustUUID(orgID), traceID, spanID, mustUUID(sessionID), base.Add(time.Duration(seq)*time.Second), seq)
		Expect(err).NotTo(HaveOccurred())
	}

	insertLink := func(fromTrace, fromSpan, toTrace, toSpan, kind string) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO span_links_20260615
			    (org_id, from_trace_id, from_span_id, from_io, to_trace_id, to_span_id, to_io, kind, session_id)
			VALUES ($1, $2, $3, 'output', $4, $5, 'input', $6, $7)`,
			mustUUID(orgID), fromTrace, fromSpan, toTrace, toSpan, kind, mustUUID(sessionID))
		Expect(err).NotTo(HaveOccurred())
	}

	// summaryOf is the session-scoped summary row for one trace — the
	// lazy session detail's view of it, which the standalone header must
	// agree with.
	summaryOf := func(traceID string) storage.TraceSummaryRecord {
		rows, err := pgDriver.ListTraceSummaries(ctx, sessionID)
		Expect(err).NotTo(HaveOccurred())
		for _, row := range rows {
			if row.TraceID == traceID {
				return row
			}
		}
		Fail("no summary for " + traceID)
		return storage.TraceSummaryRecord{}
	}

	It("reads a trace header with its span count", func() {
		insertTurn("trc-x", base)
		insertTurn("trc-y", base.Add(time.Minute))
		for i := range 3 {
			insertSpan("trc-x", "x-"+string(rune('0'+i)), int64(i))
		}

		got, err := pgDriver.GetTraceSummary(ctx, orgID, "trc-x")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got.SpanCount).To(Equal(3))
		Expect(got.SessionID).To(Equal(sessionID))
		Expect(got.UserPrompt).To(Equal("prompt for trc-x"))
		Expect(got.TotalCostUSD).To(Equal(0.25))
		Expect(*got).To(Equal(summaryOf("trc-x")),
			"the standalone header is the session summary's row for the trace")

		// A span-less turn counts zero, and is still a header.
		empty, err := pgDriver.GetTraceSummary(ctx, orgID, "trc-y")
		Expect(err).NotTo(HaveOccurred())
		Expect(empty).NotTo(BeNil())
		Expect(empty.SpanCount).To(Equal(0))
		Expect(*empty).To(Equal(summaryOf("trc-y")))

		// Unknown is nil, not an error — the handler's 404.
		missing, err := pgDriver.GetTraceSummary(ctx, orgID, "trc-none")
		Expect(err).NotTo(HaveOccurred())
		Expect(missing).To(BeNil())

		// Another org does not see it.
		foreign, err := pgDriver.GetTraceSummary(ctx, newTestOrgID(), "trc-x")
		Expect(err).NotTo(HaveOccurred())
		Expect(foreign).To(BeNil())
	})

	It("lists a trace's links from either end", func() {
		insertTurn("trc-x", base)
		insertTurn("trc-y", base.Add(time.Minute))
		insertTurn("trc-z", base.Add(2*time.Minute))
		insertSpan("trc-x", "x-0", 0)
		insertSpan("trc-y", "y-0", 0)
		insertLink("trc-x", "x-0", "trc-y", "y-0", "compaction-seam")

		seam := storage.SpanLinkRecord{
			FromTraceID: "trc-x", FromSpanID: "x-0", FromIO: "output",
			ToTraceID: "trc-y", ToSpanID: "y-0", ToIO: "input",
			Kind: "compaction-seam",
		}
		from, err := pgDriver.ListTraceLinks(ctx, orgID, "trc-x")
		Expect(err).NotTo(HaveOccurred())
		Expect(from).To(Equal([]storage.SpanLinkRecord{seam}), "the seam is the from-trace's")
		to, err := pgDriver.ListTraceLinks(ctx, orgID, "trc-y")
		Expect(err).NotTo(HaveOccurred())
		Expect(to).To(Equal([]storage.SpanLinkRecord{seam}), "and the to-trace's")

		// No edges: an empty list, never nil, so the wire pins [] rather
		// than null.
		none, err := pgDriver.ListTraceLinks(ctx, orgID, "trc-z")
		Expect(err).NotTo(HaveOccurred())
		Expect(none).NotTo(BeNil())
		Expect(none).To(BeEmpty())
	})
})

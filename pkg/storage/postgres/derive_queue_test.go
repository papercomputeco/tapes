package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/storagetest"
)

// The shared DeriveQueue conformance specs run against the Postgres
// driver (the only driver hosting the raw layer + dirty queue).
var _ = storagetest.RunDeriveQueueSpecs("postgres", func() storage.Driver {
	ctx := context.Background()
	dsn, err := testPostgresDSN()
	Expect(err).ToNot(HaveOccurred())

	d, err := postgres.NewDriver(ctx, dsn)
	Expect(err).NotTo(HaveOccurred())

	for _, stmt := range []string{
		"TRUNCATE TABLE derive_queue",
		"TRUNCATE TABLE raw_turns RESTART IDENTITY",
	} {
		_, err = d.DB().Exec(ctx, stmt)
		Expect(err).NotTo(HaveOccurred())
	}
	return d
})

var _ = Describe("Derive worker storage (postgres)", func() {
	var (
		driver *postgres.Driver
		ctx    context.Context
	)

	const (
		harnessID = "claude-code"
		sessionA  = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
		sessionB  = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
		// sessions-row UUIDs for attribution + prune scoping.
		sessionARowID = "01900000-0000-7000-8000-00000000000a"
		sessionBRowID = "01900000-0000-7000-8000-00000000000b"
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).ToNot(HaveOccurred())

		d, err := postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		driver = d

		for _, stmt := range []string{
			"TRUNCATE TABLE derive_queue",
			"TRUNCATE TABLE raw_turns RESTART IDENTITY",
			"TRUNCATE TABLE sessions CASCADE",
		} {
			_, err = driver.DB().Exec(ctx, stmt)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	insertSessionRow := func(rowID, harnessSessionID string) {
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, '00000000-0000-0000-0000-000000000000', 'test', $2, $3, NOW(), NOW())`,
			rowID, harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	}

	putWireTurn := func(requestID, harnessSessionID, userText string) {
		rec := storage.RawTurnRecord{
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "claude",
			HarnessID:        harnessID,
			HarnessSessionID: harnessSessionID,
			RequestID:        requestID,
			RawRequest: json.RawMessage(fmt.Sprintf(
				`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":%q}]}`, userText)),
			Response: json.RawMessage(fmt.Sprintf(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"reply to %s"}]},"stop_reason":"end_turn"}`, userText)),
			SessionEnvelope: json.RawMessage(fmt.Sprintf(
				`{"harness_id":%q,"harness_session_id":%q}`, harnessID, harnessSessionID)),
		}
		_, err := driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())
	}

	// spanTurnCountForSession counts the derived trace rows for a session.
	// Node persistence is retired — span_turns is the sole derived read
	// surface — so per-session derive coverage is asserted here.
	spanTurnCountForSession := func(rowID string) int {
		var n int
		err := driver.DB().QueryRow(ctx,
			"SELECT COUNT(*) FROM span_turns_20260615 WHERE session_id = $1", rowID).Scan(&n)
		Expect(err).NotTo(HaveOccurred())
		return n
	}

	Describe("TryDeriveSessionLock", func() {
		It("is exclusive per session and reentrant after release", func() {
			release1, acquired, err := driver.TryDeriveSessionLock(ctx, "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(acquired).To(BeTrue())

			_, acquired, err = driver.TryDeriveSessionLock(ctx, "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(acquired).To(BeFalse(), "second acquisition of the same session must fail")

			releaseB, acquired, err := driver.TryDeriveSessionLock(ctx, "", harnessID, sessionB)
			Expect(err).NotTo(HaveOccurred())
			Expect(acquired).To(BeTrue(), "a different session locks independently")
			releaseB()

			release1()
			release2, acquired, err := driver.TryDeriveSessionLock(ctx, "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(acquired).To(BeTrue(), "release must free the lock")
			release2()
		})
	})

	Describe("RederiveSession", func() {
		It("writes the span projection alongside nodes, idempotently", func() {
			insertSessionRow(sessionARowID, sessionA)
			putWireTurn("req-a-1", sessionA, "hello from session A")
			putWireTurn("req-a-2", sessionA, "second turn in session A")

			_, err := driver.RederiveSession(ctx, "", "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())

			countRows := func(table string) int {
				var n int
				Expect(driver.DB().QueryRow(ctx,
					"SELECT COUNT(*) FROM "+table+" WHERE session_id = $1", sessionARowID).Scan(&n),
				).To(Succeed())
				return n
			}
			turns := countRows("span_turns_20260615")
			spanRows := countRows("spans_20260615")
			Expect(turns).To(BeNumerically(">", 0), "derive must land span turns")
			Expect(spanRows).To(BeNumerically(">", turns), "each trace carries spans_20260615 beyond its root")

			var llmSpans int
			Expect(driver.DB().QueryRow(ctx,
				"SELECT COUNT(*) FROM spans_20260615 WHERE session_id = $1 AND kind = 'llm' AND raw_turn_id IS NOT NULL",
				sessionARowID).Scan(&llmSpans)).To(Succeed())
			Expect(llmSpans).To(Equal(2), "one llm span per wire call, referencing its raw row")

			// Idempotence: deterministic ids upsert in place; rerun
			// changes nothing.
			_, err = driver.RederiveSession(ctx, "", "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(countRows("span_turns_20260615")).To(Equal(turns))
			Expect(countRows("spans_20260615")).To(Equal(spanRows))
		})

		It("derives one session, is idempotent, and never touches siblings", func() {
			insertSessionRow(sessionARowID, sessionA)
			insertSessionRow(sessionBRowID, sessionB)

			putWireTurn("req-a-1", sessionA, "hello from session A")
			putWireTurn("req-a-2", sessionA, "second turn in session A")
			putWireTurn("req-b-1", sessionB, "hello from session B")

			report, err := driver.RederiveSession(ctx, "", "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(report.RawTurns).To(Equal(2))

			// Node persistence is retired: derive writes only the span
			// projection. Session A's wire turns project to trace rows; the
			// sibling session B is out of scope for this session-scoped
			// derive, so it stays empty.
			countA := spanTurnCountForSession(sessionARowID)
			Expect(countA).To(BeNumerically(">", 0))
			Expect(spanTurnCountForSession(sessionBRowID)).To(BeZero(),
				"session-scoped derive must not write sibling sessions")

			// Idempotence: re-run upserts the same set in place.
			_, err = driver.RederiveSession(ctx, "", "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(spanTurnCountForSession(sessionARowID)).To(Equal(countA))

			// Sibling derive populates B and leaves session A untouched.
			_, err = driver.RederiveSession(ctx, "", "", harnessID, sessionB)
			Expect(err).NotTo(HaveOccurred())
			Expect(spanTurnCountForSession(sessionARowID)).To(Equal(countA))
			Expect(spanTurnCountForSession(sessionBRowID)).To(BeNumerically(">", 0))
		})

		It("prunes stale derived span rows scoped to the session", func() {
			insertSessionRow(sessionARowID, sessionA)
			insertSessionRow(sessionBRowID, sessionB)
			putWireTurn("req-a-1", sessionA, "hello from session A")

			// Node persistence is retired; the span projection is the sole
			// derived surface, so the prune is exercised there. Seed a stale
			// span_turn from a superseded projection attributed to session A
			// (a trace the re-derive will not re-emit) and a sibling stale row
			// under session B that must survive a session-A-scoped derive.
			for _, row := range []struct{ traceID, sid string }{
				{"stale-trace-session-a", sessionARowID},
				{"stale-trace-session-b", sessionBRowID},
			} {
				_, err := driver.DB().Exec(ctx, `
					INSERT INTO span_turns_20260615 (org_id, trace_id, session_id, started_at)
					VALUES ('00000000-0000-0000-0000-000000000000', $1, $2, NOW())`,
					row.traceID, row.sid)
				Expect(err).NotTo(HaveOccurred())
			}

			_, err := driver.RederiveSession(ctx, "", "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())

			var staleA, staleB int
			Expect(driver.DB().QueryRow(ctx,
				"SELECT COUNT(*) FROM span_turns_20260615 WHERE trace_id = 'stale-trace-session-a'").Scan(&staleA)).To(Succeed())
			Expect(driver.DB().QueryRow(ctx,
				"SELECT COUNT(*) FROM span_turns_20260615 WHERE trace_id = 'stale-trace-session-b'").Scan(&staleB)).To(Succeed())
			Expect(staleA).To(BeZero(), "session A's stale span row is pruned by the re-derive")
			Expect(staleB).To(Equal(1), "sibling sessions' rows are out of scope")
		})
	})
})

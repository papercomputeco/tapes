package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/derive/worker"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/storagetest"
)

// droppingStore wraps the real driver but silently drops the just-
// derived session's newest trace after each re-derive — a stand-in for a
// deriver bug that loses a turn. The concurrency gate's full-projection
// assertion must notice: a gate that still passes against this store is
// vacuous.
type droppingStore struct {
	*postgres.Driver
}

func (s droppingStore) RederiveSession(ctx context.Context, project, orgID, harnessID, harnessSessionID string) (*derive.RederiveReport, error) {
	report, err := s.Driver.RederiveSession(ctx, project, orgID, harnessID, harnessSessionID)
	if err != nil {
		return report, err
	}
	// Simulate a lost turn: delete this session's newest trace across all
	// three span tables right after the idempotent upsert re-lands it.
	_, err = s.Driver.DB().Exec(ctx, `
		WITH sess AS (
			SELECT id FROM sessions WHERE harness_id = $1 AND harness_session_id = $2
		), newest AS (
			SELECT trace_id FROM span_turns_20260615
			WHERE session_id = (SELECT id FROM sess)
			ORDER BY started_at DESC, trace_id DESC LIMIT 1
		), dl AS (
			DELETE FROM span_links_20260615
			WHERE session_id = (SELECT id FROM sess) AND from_trace_id IN (SELECT trace_id FROM newest)
		), ds AS (
			DELETE FROM spans_20260615
			WHERE session_id = (SELECT id FROM sess) AND trace_id IN (SELECT trace_id FROM newest)
		)
		DELETE FROM span_turns_20260615
		WHERE session_id = (SELECT id FROM sess) AND trace_id IN (SELECT trace_id FROM newest)`,
		harnessID, harnessSessionID)
	return report, err
}

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

	Describe("AcquireDeriveSessionLock", func() {
		It("blocks behind a concurrent holder, then acquires on release", func() {
			// The worker's non-blocking hold on session A.
			release1, acquired, err := driver.TryDeriveSessionLock(ctx, "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())
			Expect(acquired).To(BeTrue())

			got := make(chan func(), 1)
			errc := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				// The nil-UUID org spelling must canonicalize to the SAME lock
				// as the "" the holder used, so this blocks rather than taking
				// a different key and racing.
				rel, err := driver.AcquireDeriveSessionLock(
					ctx, "00000000-0000-0000-0000-000000000000", harnessID, sessionA)
				if err != nil {
					errc <- err
					return
				}
				got <- rel
			}()

			// While the worker holds it, the blocking acquire must not return.
			Consistently(got, "80ms", "10ms").ShouldNot(Receive())

			release1() // worker done — the blocking acquire can now proceed.
			var rel func()
			Eventually(got, "5s", "10ms").Should(Receive(&rel))
			Consistently(errc, "10ms").ShouldNot(Receive())
			rel()
		})
	})

	Describe("RederiveSession", func() {
		It("clears a stale task fold when the session re-derives to no tasks", func() {
			// Rebuild-from-raw: the raw layer is the sole source of truth, so a
			// task no longer present in the fold must vanish from the rollup. A
			// session whose current derive folds no tasks (plain text turns) but
			// carries a stale non-empty tasks JSONB (an old deriver, or a task
			// since deleted) must be CLEARED to [] — not left untouched because
			// the empty fold was omitted from the write.
			insertSessionRow(sessionARowID, sessionA)
			putWireTurn("req-a-1", sessionA, "hello from session A")

			_, err := driver.DB().Exec(ctx,
				`UPDATE sessions SET tasks = '[{"id":"1","subject":"stale","status":"pending","updates":0}]'::jsonb WHERE id = $1`,
				sessionARowID)
			Expect(err).NotTo(HaveOccurred())

			_, err = driver.RederiveSession(ctx, "", "", harnessID, sessionA)
			Expect(err).NotTo(HaveOccurred())

			var tasks string
			Expect(driver.DB().QueryRow(ctx,
				"SELECT COALESCE(tasks::text, 'null') FROM sessions WHERE id = $1", sessionARowID).Scan(&tasks),
			).To(Succeed())
			Expect(tasks).To(Equal("[]"), "the re-derive must overwrite the stale tasks with an empty array")
		})

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

		// fullProjection digests a session's ENTIRE derived projection,
		// independent of its session/trace/span ids and timestamps: the session
		// rollup (status, git activity, tool counts, tasks, kind_counts,
		// model_usage, model), every trace in started_at order (folded prompt,
		// capture source, synthetic flag, token usage), every span in seq order
		// (kind, name, status, call_kind, model, stop_reason, usage, verdict),
		// and every link normalized to id-free (trace-ordinal:span-seq)
		// endpoints. Two sessions built from identical turn content share this
		// digest even though their deterministic ids differ — so a
		// concurrent-ingest session can be proven byte-equal to a serial control,
		// and dropping ANY turn, span, link, task, or rollup field breaks the
		// match (unlike a prompt+kind digest, which a dropped verdict, link, or
		// usage delta would slip past).
		fullProjection := func(rowID string) string {
			var sb strings.Builder

			var status, model, tasks, kindCounts, modelUsage string
			var git bool
			var toolResults, toolErrors int32
			Expect(driver.DB().QueryRow(ctx, `
				SELECT derived_status, has_git_activity, tool_result_count, tool_error_count,
				       derived_model, COALESCE(tasks::text, ''), COALESCE(kind_counts::text, ''),
				       COALESCE(model_usage::text, '')
				FROM sessions WHERE id = $1`, rowID).Scan(
				&status, &git, &toolResults, &toolErrors, &model, &tasks, &kindCounts, &modelUsage),
			).To(Succeed())
			fmt.Fprintf(&sb, "rollup status=%s git=%v toolResults=%d toolErrors=%d model=%q tasks=%s kinds=%s usage=%s",
				status, git, toolResults, toolErrors, model, tasks, kindCounts, modelUsage)

			traceOrdinal := map[string]int{}
			rows, err := driver.DB().Query(ctx, `
				SELECT trace_id, user_prompt, source, synthetic,
				       total_input_tokens, total_output_tokens, main_input_tokens,
				       main_output_tokens, cache_read_tokens, cache_creation_tokens
				FROM span_turns_20260615 WHERE session_id = $1
				ORDER BY started_at, trace_id`, rowID)
			Expect(err).NotTo(HaveOccurred())
			ordinal := 0
			for rows.Next() {
				var id, prompt, source, synthetic string
				var ti, to, mi, mo, cr, cc int64
				Expect(rows.Scan(&id, &prompt, &source, &synthetic, &ti, &to, &mi, &mo, &cr, &cc)).To(Succeed())
				ordinal++
				traceOrdinal[id] = ordinal
				fmt.Fprintf(&sb, "\ntrace %d prompt=%q source=%s synthetic=%s in=%d out=%d mainIn=%d mainOut=%d cacheR=%d cacheC=%d",
					ordinal, prompt, source, synthetic, ti, to, mi, mo, cr, cc)
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			rows.Close()

			spanKey := map[string]string{}
			srows, err := driver.DB().Query(ctx, `
				SELECT sp.trace_id, sp.span_id, sp.seq, sp.kind, sp.name, sp.status,
				       sp.call_kind, sp.model, sp.stop_reason,
				       COALESCE(sp.usage::text, ''), COALESCE(sp.verdict::text, '')
				FROM spans_20260615 sp
				JOIN span_turns_20260615 t ON t.trace_id = sp.trace_id AND t.org_id = sp.org_id
				WHERE sp.session_id = $1
				ORDER BY t.started_at, sp.trace_id, sp.seq`, rowID)
			Expect(err).NotTo(HaveOccurred())
			for srows.Next() {
				var traceID, spanID, kind, name, spStatus, callKind, spModel, stop, usage, verdict string
				var seq int64
				Expect(srows.Scan(&traceID, &spanID, &seq, &kind, &name, &spStatus, &callKind, &spModel, &stop, &usage, &verdict)).To(Succeed())
				ord := traceOrdinal[traceID]
				spanKey[spanID] = fmt.Sprintf("%d:%d", ord, seq)
				fmt.Fprintf(&sb, "\n  span t%d seq=%d [%s:%s] status=%s call=%s model=%q stop=%q usage=%s verdict=%s",
					ord, seq, kind, name, spStatus, callKind, spModel, stop, usage, verdict)
			}
			Expect(srows.Err()).NotTo(HaveOccurred())
			srows.Close()

			lrows, err := driver.DB().Query(ctx, `
				SELECT from_span_id, from_io, to_span_id, to_io, kind
				FROM span_links_20260615 WHERE session_id = $1`, rowID)
			Expect(err).NotTo(HaveOccurred())
			var links []string
			for lrows.Next() {
				var fromSpan, fromIO, toSpan, toIO, kind string
				Expect(lrows.Scan(&fromSpan, &fromIO, &toSpan, &toIO, &kind)).To(Succeed())
				links = append(links, fmt.Sprintf("%s.%s->%s.%s:%s", spanKey[fromSpan], fromIO, spanKey[toSpan], toIO, kind))
			}
			Expect(lrows.Err()).NotTo(HaveOccurred())
			lrows.Close()
			sort.Strings(links)
			for _, l := range links {
				fmt.Fprintf(&sb, "\nlink %s", l)
			}
			return sb.String()
		}

		// newTestWorker builds a fast-cadence derive worker over the given
		// store: a few-ms poll/debounce so it interleaves with test-speed
		// ingest, and a long sweep interval so the periodic backstop never
		// interferes (the one startup sweep is harmless — it only re-enqueues
		// already-dirty sessions).
		newTestWorker := func(store worker.Store) *worker.Worker {
			return worker.NewWorker(worker.Config{
				PollInterval:  3 * time.Millisecond,
				Debounce:      3 * time.Millisecond,
				MaxDeriveLag:  6 * time.Millisecond,
				SweepInterval: time.Hour,
				PageSize:      50,
			}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
		}

		// runWorkerUntilDrained runs the worker while ingest lands, then blocks
		// until the dirty queue empties (every mark cleared = every settled turn
		// derived) and stops the worker. No manual re-derive of the session
		// under test happens anywhere: the projection asserted after this
		// returns is purely the worker's own output.
		runWorkerUntilDrained := func(w *worker.Worker, ingest func()) {
			runCtx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); _ = w.Run(runCtx) }()

			ingest() // blocks until all raw turns have landed (quiesce)

			Eventually(func() int64 {
				stats, err := driver.DeriveQueueStats(ctx)
				Expect(err).NotTo(HaveOccurred())
				return stats.Depth
			}, 15*time.Second, 3*time.Millisecond).Should(BeZero(),
				"the worker must drain the dirty queue after ingest quiesces")

			cancel()
			<-done
		}

		// The property under test (RE-DERIVE-TEST-PLAN §0): the raw layer is the
		// sole source of truth, so the projection is rebuildable from raw "at any
		// time" — including while ingest is landing new turns through the real
		// derive worker. We cannot assert consistency mid-flight (raw is still
		// growing), but once ingest quiesces and the worker drains the dirty
		// queue, its projection must equal a from-scratch serial derive over the
		// same turns — with no trailing manual re-derive papering over a lost
		// turn. Driving the ACTUAL worker (poll → debounce → per-session lock →
		// RederiveSession → conditional clear) is what makes this gate exercise
		// the dirty-queue re-trigger rather than a bare re-derive loop.
		It("converges to the canonical projection under concurrent worker-driven ingest", func() {
			userTexts := []string{"first prompt", "second prompt", "third prompt", "fourth prompt", "fifth prompt", "sixth prompt"}

			// Control: ingest everything up front, derive once, snapshot BEFORE
			// the worker exists. This is the canonical projection the experiment
			// must reproduce (snapshotting first means an idempotent re-derive of
			// the control by the worker's startup sweep can't affect the compare).
			insertSessionRow(sessionBRowID, sessionB)
			for i, txt := range userTexts {
				putWireTurn(fmt.Sprintf("req-ctrl-%d", i), sessionB, txt)
			}
			_, err := driver.RederiveSession(ctx, "", "", harnessID, sessionB)
			Expect(err).NotTo(HaveOccurred())
			canonical := fullProjection(sessionBRowID)
			Expect(canonical).NotTo(BeEmpty(), "control derive must produce a projection")

			// Experiment: ingest the opener, then land the remaining turns while
			// the worker re-derives underneath — the raw-layer append racing the
			// dirty-queue-triggered derives.
			insertSessionRow(sessionARowID, sessionA)
			putWireTurn("req-exp-0", sessionA, userTexts[0])

			runWorkerUntilDrained(newTestWorker(driver), func() {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					for i := 1; i < len(userTexts); i++ {
						putWireTurn(fmt.Sprintf("req-exp-%d", i), sessionA, userTexts[i])
						time.Sleep(3 * time.Millisecond)
					}
				}()
				wg.Wait()
			})

			// Convergence: the worker's drained projection equals the canonical
			// serial projection — concurrent ingest during derivation changed
			// nothing about the result, and no intermediate turn was dropped.
			Expect(fullProjection(sessionARowID)).To(Equal(canonical),
				"worker-drained concurrent-ingest projection equals the serial-derive projection")
		})

		// Sanity: the convergence assertion above is only meaningful if it FAILS
		// when the worker loses a turn. Drive the same drain through a store that
		// drops the newest trace on every re-derive; the full-projection digest
		// must then diverge from canonical. A gate that still matched here would
		// be vacuous — exactly the defect the prior quiesce-fixpoint gate had.
		It("full-projection convergence fails when the worker drops a turn", func() {
			userTexts := []string{"first prompt", "second prompt", "third prompt", "fourth prompt"}

			insertSessionRow(sessionBRowID, sessionB)
			for i, txt := range userTexts {
				putWireTurn(fmt.Sprintf("req-ctrl-%d", i), sessionB, txt)
			}
			_, err := driver.RederiveSession(ctx, "", "", harnessID, sessionB)
			Expect(err).NotTo(HaveOccurred())
			canonical := fullProjection(sessionBRowID)

			insertSessionRow(sessionARowID, sessionA)
			for i, txt := range userTexts {
				putWireTurn(fmt.Sprintf("req-exp-%d", i), sessionA, txt)
			}
			runWorkerUntilDrained(newTestWorker(droppingStore{Driver: driver}), func() {})

			Expect(fullProjection(sessionARowID)).NotTo(Equal(canonical),
				"a full-projection gate must notice a dropped turn — else it is vacuous")
		})
	})
})

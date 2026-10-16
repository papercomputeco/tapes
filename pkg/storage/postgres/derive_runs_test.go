package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Derive runs (postgres)", func() {
	const harnessID = "claude"

	var (
		ctx    context.Context
		driver *postgres.Driver
		orgID  string
	)

	BeforeEach(func() {
		ctx = context.Background()
		orgID = newTestOrgID()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE derive_runs, raw_turn_attribution_corrections, derive_queue, raw_turns RESTART IDENTITY CASCADE")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	putTurn := func(harnessSessionID, requestID string) {
		inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: harnessID,
			HarnessID: harnessID, HarnessSessionID: harnessSessionID, RequestID: requestID,
			RawRequest: json.RawMessage(fmt.Sprintf(
				`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":%q}]}`, requestID)),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]},"stop_reason":"end_turn"}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
	}

	insertSession := func(harnessSessionID string) {
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	}

	seedSession := func(harnessSessionID string, turns int) {
		for i := range turns {
			putTurn(harnessSessionID, fmt.Sprintf("%s-%d", harnessSessionID, i))
		}
		insertSession(harnessSessionID)
	}

	projectedTurns := func() int {
		var n int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT count(*) FROM span_turns_20260615 WHERE org_id = $1", orgID).Scan(&n)).To(Succeed())
		return n
	}

	finishedSessions := func(runID string) int {
		var n int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT count(*) FROM derive_run_sessions WHERE run_id = $1 AND finished_at IS NOT NULL", runID).Scan(&n)).To(Succeed())
		return n
	}

	It("snapshots the session set and derives every session", func() {
		seedSession("a", 2)
		seedSession("b", 1)
		seedSession("c", 1)

		run, created, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())
		Expect(run.State).To(Equal(derive.RunPending))
		Expect(run.SessionsTotal).To(Equal(3))
		Expect(run.SessionsDone).To(BeZero())
		Expect(projectedTurns()).To(BeZero(), "creating a run does no derive work")

		done, err := driver.ExecuteDeriveRun(ctx, run.ID, "test-worker")
		Expect(err).NotTo(HaveOccurred())
		Expect(done.State).To(Equal(derive.RunCompleted))
		Expect(done.SessionsDone).To(Equal(3))
		Expect(done.SessionsFailed).To(BeZero())
		Expect(done.Worker).To(Equal("test-worker"))
		Expect(done.StartedAt).NotTo(BeNil())
		Expect(done.FinishedAt).NotTo(BeNil())
		Expect(done.Report).NotTo(BeNil())
		Expect(done.Report.RawTurns).To(Equal(4))
		Expect(done.Errors).To(BeEmpty())
		Expect(projectedTurns()).To(Equal(4))

		got, err := driver.GetDeriveRun(ctx, run.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(done))
	})

	It("answers a second start with the run already active", func() {
		seedSession("a", 1)

		first, created, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())

		second, created, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeFalse())
		Expect(second.ID).To(Equal(first.ID))

		_, err = driver.ExecuteDeriveRun(ctx, first.ID, "w")
		Expect(err).NotTo(HaveOccurred())

		third, created, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue(), "a completed run does not block the next one")
		Expect(third.ID).NotTo(Equal(first.ID))

		runs, err := driver.ListDeriveRuns(ctx, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(runs).To(HaveLen(2))
		Expect(runs[0].ID).To(Equal(third.ID), "newest first")
	})

	It("resumes from the first unfinished session after an interruption", func() {
		for _, id := range []string{"a", "b", "c", "d"} {
			seedSession(id, 1)
		}
		run, _, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		// Stand in for an executor that died after finishing two sessions:
		// those two are marked finished without being derived, so a resume
		// that re-derived them would project their turns.
		_, err = driver.DB().Exec(ctx, `
			UPDATE derive_run_sessions SET finished_at = NOW()
			WHERE run_id = $1 AND harness_session_id IN ('a', 'b')`, run.ID)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, `
			UPDATE derive_runs SET state = 'running', sessions_done = 2 WHERE id = $1`, run.ID)
		Expect(err).NotTo(HaveOccurred())

		done, err := driver.ExecuteDeriveRun(ctx, run.ID, "w2")
		Expect(err).NotTo(HaveOccurred())
		Expect(done.State).To(Equal(derive.RunCompleted))
		Expect(done.SessionsDone).To(Equal(4))
		Expect(done.Report.RawTurns).To(Equal(2), "only the unfinished sessions were derived")
		Expect(projectedTurns()).To(Equal(2))
	})

	It("stops at a cancel and leaves the rest unfinished", func() {
		seedSession("a", 1)
		seedSession("b", 1)
		run, _, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		canceled, err := driver.CancelDeriveRun(ctx, run.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(canceled.State).To(Equal(derive.RunCanceled))
		Expect(canceled.FinishedAt).NotTo(BeNil())

		again, err := driver.CancelDeriveRun(ctx, run.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(again.State).To(Equal(derive.RunCanceled), "canceling twice is harmless")

		after, err := driver.ExecuteDeriveRun(ctx, run.ID, "w")
		Expect(err).NotTo(HaveOccurred())
		Expect(after.State).To(Equal(derive.RunCanceled))
		Expect(finishedSessions(run.ID)).To(BeZero(), "a canceled run is not executed")

		ids, err := driver.ListActiveDeriveRunIDs(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(ids).To(BeEmpty())
	})

	It("records a failing session and carries on", func() {
		seedSession("good", 1)
		// A transcript row whose body is not a record array fails to parse,
		// which fails that session's derive. Ingest would refuse to write
		// one, so a captured wire row is relabelled in place.
		putTurn("bad", "bad-transcript")
		_, err := driver.DB().Exec(ctx,
			"UPDATE raw_turns SET source = $1 WHERE harness_session_id = 'bad'", storage.RawTurnSourceTranscript)
		Expect(err).NotTo(HaveOccurred())
		insertSession("bad")

		run, _, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		done, err := driver.ExecuteDeriveRun(ctx, run.ID, "w")
		Expect(err).NotTo(HaveOccurred())
		Expect(done.State).To(Equal(derive.RunCompleted))
		Expect(done.SessionsDone).To(Equal(2))
		Expect(done.SessionsFailed).To(Equal(1))
		Expect(done.Errors).To(HaveLen(1))
		Expect(done.Errors[0]).To(HavePrefix(harnessID + "/bad: "))
		Expect(projectedTurns()).To(Equal(1), "the good session still derived")
	})

	It("refuses to execute a run another process holds", func() {
		seedSession("a", 1)
		run, _, err := driver.StartDeriveRun(ctx, "")
		Expect(err).NotTo(HaveOccurred())

		other, err := postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		defer other.Close()
		holder, err := other.DB().Acquire(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer holder.Release()
		var locked bool
		Expect(holder.QueryRow(ctx, "SELECT pg_try_advisory_lock($1, $2)",
			postgres.DeriveRunLockClass, postgres.DeriveRunLockKey(run.ID)).Scan(&locked)).To(Succeed())
		Expect(locked).To(BeTrue())

		_, err = driver.ExecuteDeriveRun(ctx, run.ID, "w")
		Expect(err).To(MatchError(derive.ErrRunBusy))

		_, err = holder.Exec(ctx, "SELECT pg_advisory_unlock($1, $2)",
			postgres.DeriveRunLockClass, postgres.DeriveRunLockKey(run.ID))
		Expect(err).NotTo(HaveOccurred())
		done, err := driver.ExecuteDeriveRun(ctx, run.ID, "w")
		Expect(err).NotTo(HaveOccurred())
		Expect(done.State).To(Equal(derive.RunCompleted))
	})

	It("reports an unknown run as not found", func() {
		_, err := driver.GetDeriveRun(ctx, newTestOrgID())
		Expect(err).To(MatchError(derive.ErrRunNotFound))
		_, err = driver.GetDeriveRun(ctx, "not-a-uuid")
		Expect(err).To(MatchError(derive.ErrRunNotFound))
		_, err = driver.CancelDeriveRun(ctx, newTestOrgID())
		Expect(err).To(MatchError(derive.ErrRunNotFound))
	})
})

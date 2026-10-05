package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver.DeleteSession", func() {
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

	// seed ingests a 2-node turn under the given identity and returns the
	// tapes-minted session UUID. A non-nil parentHarnessSessionID forks the
	// new session off that parent (shared harness_id). Ingest itself
	// persists nothing turn-shaped, so a derived span_turn row is planted
	// directly to give the session a dependent projection row the delete
	// must cascade through (span_turns/spans/span_links cascade on
	// session_id since 1781230000_span_model).
	seed := func(orgID, harnessSessionID, text string, parentHarnessSessionID *string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:                  orgID,
				AuthSubject:            "subject-delete",
				HarnessID:              "claude",
				HarnessSessionID:       harnessSessionID,
				ParentHarnessSessionID: parentHarnessSessionID,
			},
			Nodes: sessionFixture(text),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())

		_, err = pgDriver.DB().Exec(ctx, `
			INSERT INTO span_turns_20260615 (org_id, trace_id, session_id, started_at)
			VALUES ($1::uuid, $2, $3::uuid, NOW())`,
			orgID, "trace-"+harnessSessionID, res.SessionID)
		Expect(err).NotTo(HaveOccurred())
		return res.SessionID
	}

	spanTurnCount := func(sessionID string) int {
		var n int
		err := pgDriver.DB().QueryRow(ctx,
			"SELECT count(*) FROM span_turns_20260615 WHERE session_id = $1::uuid", sessionID).Scan(&n)
		Expect(err).NotTo(HaveOccurred())
		return n
	}

	It("deletes the session and reports it was removed", func() {
		orgID := newTestOrgID()
		id := seed(orgID, "sess-solo", "solo turn", nil)
		Expect(spanTurnCount(id)).To(BeNumerically(">", 0), "precondition: the session owns derived span turns")

		deleted, err := pgDriver.DeleteSession(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())

		rec, err := pgDriver.GetSessionRecord(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil(), "the session row is gone")
		Expect(spanTurnCount(id)).To(BeZero(), "the session's span turns cascade with it")
	})

	It("cascades to subagent child sessions and their span turns", func() {
		orgID := newTestOrgID()
		parentID := seed(orgID, "sess-parent", "parent turn", nil)
		parentHarness := "sess-parent"
		childID := seed(orgID, "sess-child", "child turn", &parentHarness)
		Expect(childID).NotTo(Equal(parentID))

		// Sanity: the child really is FK-linked to the parent.
		child, err := pgDriver.GetSessionRecord(ctx, orgID, childID)
		Expect(err).NotTo(HaveOccurred())
		Expect(child).NotTo(BeNil())
		Expect(child.ParentSessionID).To(Equal(parentID), "precondition: child forks off the parent")

		deleted, err := pgDriver.DeleteSession(ctx, orgID, parentID)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())

		gone, err := pgDriver.GetSessionRecord(ctx, orgID, childID)
		Expect(err).NotTo(HaveOccurred())
		Expect(gone).To(BeNil(), "deleting the parent cascades to the child session")
		Expect(spanTurnCount(parentID)).To(BeZero())
		Expect(spanTurnCount(childID)).To(BeZero())
	})

	It("leaves unrelated sessions in the same org untouched", func() {
		orgID := newTestOrgID()
		victimID := seed(orgID, "sess-victim", "victim turn", nil)
		survivorID := seed(orgID, "sess-survivor", "survivor turn", nil)

		deleted, err := pgDriver.DeleteSession(ctx, orgID, victimID)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())

		survivor, err := pgDriver.GetSessionRecord(ctx, orgID, survivorID)
		Expect(err).NotTo(HaveOccurred())
		Expect(survivor).NotTo(BeNil(), "a sibling session must not be swept up by the delete")
		Expect(spanTurnCount(survivorID)).To(BeNumerically(">", 0))
	})

	It("reports false for an absent id and never crosses orgs", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()
		idA := seed(orgA, "sess-a", "org A turn", nil)

		// A random valid UUID that does not exist.
		missing, err := pgDriver.DeleteSession(ctx, orgA, newTestOrgID())
		Expect(err).NotTo(HaveOccurred())
		Expect(missing).To(BeFalse())

		// orgB cannot delete orgA's session even with the right id.
		crossOrg, err := pgDriver.DeleteSession(ctx, orgB, idA)
		Expect(err).NotTo(HaveOccurred())
		Expect(crossOrg).To(BeFalse(), "the delete is org-scoped")

		stillThere, err := pgDriver.GetSessionRecord(ctx, orgA, idA)
		Expect(err).NotTo(HaveOccurred())
		Expect(stillThere).NotTo(BeNil(), "org A's session survives org B's delete attempt")
	})

	It("treats a malformed id as a no-op, not an error", func() {
		orgID := newTestOrgID()
		deleted, err := pgDriver.DeleteSession(ctx, orgID, "not-a-uuid")
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeFalse())
	})
})

var _ = Describe("Driver.DeleteSession captured data", func() {
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
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE raw_turn_attribution_corrections, derive_queue, raw_turns RESTART IDENTITY CASCADE")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// putTurn captures one derivable wire turn under the given harness
	// session and returns its raw turn id. PutRawTurn marks the session
	// derive-dirty, exactly as live capture does.
	putTurn := func(org, harness, harnessSessionID, requestID string) int64 {
		inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: org, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: harness,
			HarnessID: harness, HarnessSessionID: harnessSessionID, RequestID: requestID,
			RawRequest: json.RawMessage(fmt.Sprintf(
				`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":%q}]}`, requestID)),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]},"stop_reason":"end_turn"}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
		return rawTurnIDForRequest(ctx, driver, org, requestID)
	}

	// insertSession plants the session identity ingest would write, optionally
	// as a subagent child of parentID.
	insertSession := func(org, harness, harnessSessionID string, parentID *string) string {
		id := newTestOrgID()
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id,
			                      parent_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, $5, NOW(), NOW())`,
			id, org, harness, harnessSessionID, parentID)
		Expect(err).NotTo(HaveOccurred())
		return id
	}

	// correct appends an attribution correction moving a raw turn onto key.
	correct := func(rawTurnID int64, harness, harnessSessionID string) {
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO raw_turn_attribution_corrections
			    (org_id, raw_turn_id, harness_id, harness_session_id, reason)
			VALUES ($1, $2, $3, $4, 'test')`,
			orgID, rawTurnID, harness, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	}

	derive := func(harnessSessionID string) {
		_, err := driver.RederiveSessionLocked(ctx, "", orgID, harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	}

	rawTurnExists := func(id int64) bool {
		var n int
		Expect(driver.DB().QueryRow(ctx, "SELECT count(*) FROM raw_turns WHERE id = $1", id).Scan(&n)).To(Succeed())
		return n > 0
	}

	correctionsFor := func(id int64) int {
		var n int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT count(*) FROM raw_turn_attribution_corrections WHERE raw_turn_id = $1", id).Scan(&n)).To(Succeed())
		return n
	}

	sessionIDFor := func(org, harness, harnessSessionID string) string {
		var id string
		err := driver.DB().QueryRow(ctx, `
			SELECT id::text FROM sessions
			WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			org, harness, harnessSessionID).Scan(&id)
		if err != nil {
			Expect(err).To(MatchError(pgx.ErrNoRows))
			return ""
		}
		return id
	}

	// projectedRawTurns counts the distinct raw turns the session's spans are
	// derived from. Two unrelated single-message requests may fold into one
	// trace, so traces are not a per-turn count; span provenance is.
	projectedRawTurns := func(harnessSessionID string) int {
		var n int
		Expect(driver.DB().QueryRow(ctx, `
			SELECT count(DISTINCT sp.raw_turn_id)
			FROM spans_20260615 sp
			JOIN sessions s ON s.id = sp.session_id
			WHERE s.org_id = $1 AND s.harness_id = $2 AND s.harness_session_id = $3`,
			orgID, harnessID, harnessSessionID).Scan(&n)).To(Succeed())
		return n
	}

	deleteSession := func(id string) {
		deleted, err := driver.DeleteSession(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
	}

	It("removes a solo session's raw turns, their corrections and its derive mark", func() {
		first := putTurn(orgID, harnessID, "solo", "solo-1")
		second := putTurn(orgID, harnessID, "solo", "solo-2")
		// A correction that keeps the turn on its own session (a thread fix)
		// still references a deleted raw turn and must go with it.
		correct(second, harnessID, "solo")
		id := insertSession(orgID, harnessID, "solo", nil)
		derive("solo")
		Expect(projectedRawTurns("solo")).To(Equal(2),
			"precondition: both raw turns are projected")
		Expect(driver.MarkDeriveDirty(ctx, orgID, harnessID, "solo")).To(Succeed())

		deleteSession(id)

		Expect(sessionIDFor(orgID, harnessID, "solo")).To(BeEmpty())
		Expect(rawTurnExists(first)).To(BeFalse())
		Expect(rawTurnExists(second)).To(BeFalse())
		Expect(correctionsFor(second)).To(BeZero())
		mark, err := driver.GetDeriveDirty(ctx, orgID, harnessID, "solo")
		Expect(err).NotTo(HaveOccurred())
		Expect(mark).To(BeNil(), "nothing is left to derive, so the mark goes too")
		var spans int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT count(*) FROM span_turns_20260615 WHERE session_id = $1::uuid", id).Scan(&spans)).To(Succeed())
		Expect(spans).To(BeZero())
	})

	It("follows effective attribution: keeps turns corrected away and removes turns corrected in", func() {
		movedAway := putTurn(orgID, harnessID, "victim", "victim-moved-away")
		stays := putTurn(orgID, harnessID, "victim", "victim-stays")
		movedIn := putTurn(orgID, harnessID, "other", "other-moved-in")
		otherOwn := putTurn(orgID, harnessID, "other", "other-own")
		correct(movedAway, harnessID, "other")
		correct(movedIn, harnessID, "victim")
		victimID := insertSession(orgID, harnessID, "victim", nil)
		otherID := insertSession(orgID, harnessID, "other", nil)

		deleteSession(victimID)

		Expect(rawTurnExists(stays)).To(BeFalse())
		Expect(rawTurnExists(movedIn)).To(BeFalse(), "a turn corrected onto the victim belongs to it")
		Expect(correctionsFor(movedIn)).To(BeZero())
		Expect(rawTurnExists(movedAway)).To(BeTrue(), "a turn corrected onto another session belongs to that session")
		Expect(correctionsFor(movedAway)).To(Equal(1), "the surviving turn keeps its attribution")
		Expect(rawTurnExists(otherOwn)).To(BeTrue())
		Expect(sessionIDFor(orgID, harnessID, "other")).To(Equal(otherID))

		derive("other")
		Expect(projectedRawTurns("other")).To(Equal(2),
			"the other session still projects its own turn and the one corrected onto it")
	})

	It("judges attribution by the latest correction only, across a correction history", func() {
		// Corrected onto the victim, then back: belongs to "other" now.
		bounced := putTurn(orgID, harnessID, "other", "other-bounced")
		correct(bounced, harnessID, "victim")
		correct(bounced, harnessID, "other")
		// Corrected away from the victim, then back: belongs to the victim.
		returned := putTurn(orgID, harnessID, "victim", "victim-returned")
		correct(returned, harnessID, "other")
		correct(returned, harnessID, "victim")
		victimID := insertSession(orgID, harnessID, "victim", nil)
		insertSession(orgID, harnessID, "other", nil)

		deleteSession(victimID)

		Expect(rawTurnExists(bounced)).To(BeTrue(), "an earlier correction onto the victim does not claim the turn")
		Expect(correctionsFor(bounced)).To(Equal(2))
		Expect(rawTurnExists(returned)).To(BeFalse(), "the latest correction onto the victim claims the turn")
		Expect(correctionsFor(returned)).To(BeZero())
	})

	It("removes the raw turns of every subagent session in the subtree, and nothing outside it", func() {
		parentTurn := putTurn(orgID, harnessID, "parent", "parent-1")
		childTurn := putTurn(orgID, harnessID, "child", "child-1")
		grandchildTurn := putTurn(orgID, harnessID, "grandchild", "grandchild-1")
		siblingTurn := putTurn(orgID, harnessID, "sibling", "sibling-1")
		parentID := insertSession(orgID, harnessID, "parent", nil)
		childID := insertSession(orgID, harnessID, "child", &parentID)
		insertSession(orgID, harnessID, "grandchild", &childID)
		insertSession(orgID, harnessID, "sibling", nil)

		deleteSession(parentID)

		Expect(rawTurnExists(parentTurn)).To(BeFalse())
		Expect(rawTurnExists(childTurn)).To(BeFalse())
		Expect(rawTurnExists(grandchildTurn)).To(BeFalse())
		Expect(sessionIDFor(orgID, harnessID, "grandchild")).To(BeEmpty())
		Expect(rawTurnExists(siblingTurn)).To(BeTrue())
		Expect(sessionIDFor(orgID, harnessID, "sibling")).NotTo(BeEmpty())
	})

	It("deleting a subagent session leaves its parent's raw turns", func() {
		parentTurn := putTurn(orgID, harnessID, "parent", "parent-1")
		childTurn := putTurn(orgID, harnessID, "child", "child-1")
		parentID := insertSession(orgID, harnessID, "parent", nil)
		childID := insertSession(orgID, harnessID, "child", &parentID)

		deleteSession(childID)

		Expect(rawTurnExists(childTurn)).To(BeFalse())
		Expect(rawTurnExists(parentTurn)).To(BeTrue())
		Expect(sessionIDFor(orgID, harnessID, "parent")).To(Equal(parentID))
	})

	It("never touches another org's or another harness's turns under the same harness session id", func() {
		otherOrg := newTestOrgID()
		victim := putTurn(orgID, harnessID, "shared-id", "victim-1")
		otherHarness := putTurn(orgID, "codex", "shared-id", "codex-1")
		otherOrgTurn := putTurn(otherOrg, harnessID, "shared-id", "other-org-1")
		victimID := insertSession(orgID, harnessID, "shared-id", nil)
		insertSession(orgID, "codex", "shared-id", nil)
		insertSession(otherOrg, harnessID, "shared-id", nil)

		deleteSession(victimID)

		Expect(rawTurnExists(victim)).To(BeFalse())
		Expect(rawTurnExists(otherHarness)).To(BeTrue())
		Expect(rawTurnExists(otherOrgTurn)).To(BeTrue())
		Expect(sessionIDFor(orgID, "codex", "shared-id")).NotTo(BeEmpty())
		Expect(sessionIDFor(otherOrg, harnessID, "shared-id")).NotTo(BeEmpty())
	})

	It("is not brought back by a full re-derive or a sweep of the raw layer", func() {
		putTurn(orgID, harnessID, "gone", "gone-1")
		putTurn(orgID, harnessID, "gone", "gone-2")
		id := insertSession(orgID, harnessID, "gone", nil)
		derive("gone")

		deleteSession(id)

		_, err := driver.RederiveFromRaw(ctx, "")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.SweepDeriveDirty(ctx, time.Time{})
		Expect(err).NotTo(HaveOccurred())
		mark, err := driver.GetDeriveDirty(ctx, orgID, harnessID, "gone")
		Expect(err).NotTo(HaveOccurred())
		Expect(mark).To(BeNil(), "the sweep finds no raw turn to enqueue")
		derive("gone")

		Expect(sessionIDFor(orgID, harnessID, "gone")).To(BeEmpty())
		var spans int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT count(*) FROM span_turns_20260615 WHERE org_id = $1", orgID).Scan(&spans)).To(Succeed())
		Expect(spans).To(BeZero())
	})

	It("starts a new session holding only what is captured after the delete", func() {
		putTurn(orgID, harnessID, "continued", "before-1")
		putTurn(orgID, harnessID, "continued", "before-2")
		oldID := insertSession(orgID, harnessID, "continued", nil)
		derive("continued")
		Expect(projectedRawTurns("continued")).To(Equal(2))

		deleteSession(oldID)

		// The agent keeps running: its next turn is captured and ingest
		// recreates the session identity, as the proxy does on every turn.
		putTurn(orgID, harnessID, "continued", "after-1")
		newID := insertSession(orgID, harnessID, "continued", nil)
		derive("continued")

		Expect(newID).NotTo(Equal(oldID))
		Expect(projectedRawTurns("continued")).To(Equal(1),
			"the deleted turns stay deleted; only the new capture is projected")
	})

	It("waits for an in-flight derive of the session before deleting", func() {
		turn := putTurn(orgID, harnessID, "busy", "busy-1")
		id := insertSession(orgID, harnessID, "busy", nil)

		release, err := driver.AcquireDeriveSessionLock(ctx, orgID, harnessID, "busy")
		Expect(err).NotTo(HaveOccurred())
		done := make(chan error, 1)
		go func() {
			_, err := driver.DeleteSession(ctx, orgID, id)
			done <- err
		}()

		Consistently(done, 300*time.Millisecond).ShouldNot(Receive(), "the delete must not run under a derive")
		Expect(rawTurnExists(turn)).To(BeTrue())

		release()
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))
		Expect(rawTurnExists(turn)).To(BeFalse())
		Expect(sessionIDFor(orgID, harnessID, "busy")).To(BeEmpty())
	})

	It("removes the raw turns of a subagent session that attaches while the delete runs", func() {
		parentTurn := putTurn(orgID, harnessID, "parent", "parent-1")
		childTurn := putTurn(orgID, harnessID, "late-child", "late-child-1")
		parentID := insertSession(orgID, harnessID, "parent", nil)

		// Ingest of the child is mid-transaction when the delete starts: its
		// session row references the parent but has not committed, so the
		// delete's first read of the subtree cannot see it.
		attach, err := driver.DB().Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer attach.Rollback(ctx)
		_, err = attach.Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id,
			                      parent_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, 'late-child', $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, parentID)
		Expect(err).NotTo(HaveOccurred())

		done := make(chan error, 1)
		go func() {
			_, err := driver.DeleteSession(ctx, orgID, parentID)
			done <- err
		}()
		Consistently(done, 300*time.Millisecond).ShouldNot(Receive(),
			"the delete waits for the attaching child to settle")
		Expect(attach.Commit(ctx)).To(Succeed())
		Eventually(done, 5*time.Second).Should(Receive(BeNil()))

		Expect(sessionIDFor(orgID, harnessID, "late-child")).To(BeEmpty(), "the child cascades with its parent")
		Expect(rawTurnExists(parentTurn)).To(BeFalse())
		Expect(rawTurnExists(childTurn)).To(BeFalse(),
			"a child session the delete removes takes its raw turns with it")
		mark, err := driver.GetDeriveDirty(ctx, orgID, harnessID, "late-child")
		Expect(err).NotTo(HaveOccurred())
		Expect(mark).To(BeNil())
	})

	It("never strands a turn captured while the delete runs", func() {
		putTurn(orgID, harnessID, "racing", "racing-1")
		id := insertSession(orgID, harnessID, "racing", nil)

		// A capture for the session arrives after the delete removed the raw
		// turns but before it commits.
		captured := make(chan error, 1)
		restore := postgres.SetDeleteSessionAfterRawTurnsForTest(func() {
			go func() {
				_, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
					OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: harnessID,
					HarnessID: harnessID, HarnessSessionID: "racing", RequestID: "racing-late",
					RawRequest: json.RawMessage(`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":"late"}]}`),
					Response:   json.RawMessage(`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]},"stop_reason":"end_turn"}`),
				})
				captured <- err
			}()
			// Give the capture every chance to commit inside the delete.
			select {
			case err := <-captured:
				captured <- err
			case <-time.After(300 * time.Millisecond):
			}
		})
		defer restore()

		deleteSession(id)
		Eventually(captured, 5*time.Second).Should(Receive(BeNil()))

		late := rawTurnIDForRequest(ctx, driver, orgID, "racing-late")
		Expect(rawTurnExists(late)).To(BeTrue(), "the capture is ordered after the delete, so it is new capture")
		mark, err := driver.GetDeriveDirty(ctx, orgID, harnessID, "racing")
		Expect(err).NotTo(HaveOccurred())
		Expect(mark).NotTo(BeNil(), "the surviving turn keeps its derive mark")
	})
})

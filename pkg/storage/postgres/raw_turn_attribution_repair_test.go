package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

var _ = Describe("raw-turn attribution repair", func() {
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

	It("records an idempotent overlay, preserves raw bytes, and rebuilds source, target, thread, and parent attribution", func() {
		const (
			harnessID = "codex"
			sourceID  = "parent-session"
			targetID  = "child-session"
			parentID  = "root-session"
			threadID  = "child-thread"
		)

		sourceRowID := newTestOrgID()
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			sourceRowID, orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		rec := storage.RawTurnRecord{
			OrgID:            orgID,
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "codex",
			HarnessID:        harnessID,
			HarnessSessionID: sourceID,
			RequestID:        "repair-request-1",
			RawRequest: json.RawMessage(
				`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":"repair me"}]}`),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"repaired"}]},"stop_reason":"end_turn"}`),
			Meta: json.RawMessage(`{"thread_id":"wrong-thread","future":"keep-me"}`),
			SessionEnvelope: json.RawMessage(fmt.Sprintf(
				`{"org_id":%q,"auth_subject":"user-test","harness_id":%q,"harness_session_id":%q,"cwd":"/tmp/project","harness_metadata":{"originator":"Codex Desktop","paperProxyRequestId":"paper-proxy-1"}}`,
				orgID, harnessID, sourceID)),
		}
		inserted, err := driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())

		var rawTurnID int64
		var beforeRequest, beforeResponse, beforeMeta, beforeEnvelope, beforeHarness, beforeSession string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT id, raw_request::text, response::text, meta::text, session_envelope::text,
			       harness_id, harness_session_id
			FROM raw_turns WHERE org_id = $1 AND request_id = $2`, orgID, rec.RequestID).Scan(
			&rawTurnID, &beforeRequest, &beforeResponse, &beforeMeta, &beforeEnvelope,
			&beforeHarness, &beforeSession,
		)).To(Succeed())

		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(Equal(1))

		request := storage.RawTurnAttributionRepairRequest{
			OrgID:                  orgID,
			PaperProxyRequestID:    "paper-proxy-1",
			HarnessID:              harnessID,
			HarnessSessionID:       targetID,
			ThreadID:               threadID,
			ParentHarnessSessionID: &[]string{parentID}[0],
			Reason:                 "Codex child request matched exact thread and parent evidence",
		}
		result, err := driver.RepairRawTurnAttribution(ctx, "", request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Recorded).To(BeTrue())
		Expect(result.Previous.HarnessSessionID).To(Equal(sourceID))
		Expect(result.Effective.HarnessSessionID).To(Equal(targetID))
		Expect(result.Effective.ThreadID).To(Equal(threadID))

		var firstStartedAt, firstLastSeenAt time.Time
		Expect(driver.DB().QueryRow(ctx, `
			SELECT started_at, last_seen_at FROM sessions
			WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			orgID, harnessID, targetID).Scan(&firstStartedAt, &firstLastSeenAt)).To(Succeed())

		result, err = driver.RepairRawTurnAttribution(ctx, "", request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Recorded).To(BeFalse(), "repeating the same repair must not append another audit row")
		var retryStartedAt, retryLastSeenAt time.Time
		Expect(driver.DB().QueryRow(ctx, `
			SELECT started_at, last_seen_at FROM sessions
			WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			orgID, harnessID, targetID).Scan(&retryStartedAt, &retryLastSeenAt)).To(Succeed())
		Expect(retryStartedAt).To(Equal(firstStartedAt))
		Expect(retryLastSeenAt).To(Equal(firstLastSeenAt), "idempotent repair must not make the session look newly active")

		var corrections int
		Expect(driver.DB().QueryRow(ctx,
			"SELECT COUNT(*) FROM raw_turn_attribution_corrections WHERE org_id = $1 AND raw_turn_id = $2",
			orgID, rawTurnID).Scan(&corrections)).To(Succeed())
		Expect(corrections).To(Equal(1))

		var afterRequest, afterResponse, afterMeta, afterEnvelope, afterHarness, afterSession string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT raw_request::text, response::text, meta::text, session_envelope::text,
			       harness_id, harness_session_id
			FROM raw_turns WHERE org_id = $1 AND id = $2`, orgID, rawTurnID).Scan(
			&afterRequest, &afterResponse, &afterMeta, &afterEnvelope, &afterHarness, &afterSession,
		)).To(Succeed())
		Expect([]string{afterRequest, afterResponse, afterMeta, afterEnvelope, afterHarness, afterSession}).To(Equal(
			[]string{beforeRequest, beforeResponse, beforeMeta, beforeEnvelope, beforeHarness, beforeSession},
		), "repair must not rewrite immutable raw-turn columns")

		rawRows, err := driver.ListRawTurns(ctx, 0, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rawRows).To(HaveLen(1))
		Expect(rawRows[0].HarnessID).To(Equal(harnessID))
		Expect(rawRows[0].HarnessSessionID).To(Equal(targetID))
		Expect(rawRows[0].Meta).To(MatchJSON(`{"thread_id":"child-thread","future":"keep-me"}`))
		var effectiveEnvelope sessions.IngestEnvelope
		Expect(json.Unmarshal(rawRows[0].SessionEnvelope, &effectiveEnvelope)).To(Succeed())
		Expect(effectiveEnvelope.HarnessID).To(Equal(harnessID))
		Expect(effectiveEnvelope.HarnessSessionID).To(Equal(targetID))
		Expect(effectiveEnvelope.ParentHarnessSessionID).NotTo(BeNil())
		Expect(*effectiveEnvelope.ParentHarnessSessionID).To(Equal(parentID))
		Expect(effectiveEnvelope.Cwd).To(Equal("/tmp/project"), "unrepaired envelope fields must survive the overlay")

		var corpus bytes.Buffer
		Expect(derive.WriteCorpus(&corpus, rawRows)).To(Succeed())
		wire, transcripts, err := derive.LoadCorpus(&corpus)
		Expect(err).NotTo(HaveOccurred())
		Expect(transcripts).To(BeEmpty())
		Expect(wire).To(HaveLen(1))
		var replayEnvelope sessions.IngestEnvelope
		Expect(json.Unmarshal(wire[0].SessionEnvelope, &replayEnvelope)).To(Succeed())
		Expect(replayEnvelope.HarnessID).To(Equal(harnessID))
		Expect(replayEnvelope.HarnessSessionID).To(Equal(targetID))
		Expect(replayEnvelope.ParentHarnessSessionID).NotTo(BeNil())
		Expect(*replayEnvelope.ParentHarnessSessionID).To(Equal(parentID),
			"dump-corpus replay must carry the effective repaired parent")

		sessionRows, err := gensqlc.New(driver.DB()).ListRawTurnsBySession(ctx, gensqlc.ListRawTurnsBySessionParams{
			OrgID: mustUUID(orgID), HarnessSessionID: targetID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionRows).To(HaveLen(1))
		Expect(sessionRows[0].HarnessID).To(Equal(harnessID))
		Expect(sessionRows[0].HarnessSessionID).To(Equal(targetID))
		Expect(sessionRows[0].Meta).To(MatchJSON(`{"thread_id":"child-thread","future":"keep-me"}`))
		Expect(sessionRows[0].SessionEnvelope).To(MatchJSON(rawRows[0].SessionEnvelope))

		headers, err := driver.ListRawTurnHeaders(ctx, orgID, harnessID, targetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(headers).To(HaveLen(1))
		Expect(headers[0].Meta).To(MatchJSON(`{"thread_id":"child-thread","future":"keep-me"}`))

		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(BeZero(),
			"moving the only raw turn must clear the old projection")
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, targetID)).To(Equal(1))
		source, err := driver.GetSessionRecordByHarness(ctx, orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).To(BeNil(), "moving the final effective turn must remove the ghost source identity")
		listed, err := driver.ListSessionRecords(ctx, orgID, storage.SessionListOpts{Limit: 10})
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).NotTo(ContainElement(HaveField("HarnessSessionID", sourceID)))

		var gotThread string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT sp.thread_id
			FROM spans_20260615 sp
			JOIN sessions s ON s.id = sp.session_id
			WHERE s.org_id = $1 AND s.harness_id = $2 AND s.harness_session_id = $3
			  AND sp.kind = 'llm' AND sp.raw_turn_id = $4`,
			orgID, harnessID, targetID, rawTurnID).Scan(&gotThread)).To(Succeed())
		Expect(gotThread).To(Equal(threadID))

		var gotParent string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT parent.harness_session_id
			FROM sessions child
			JOIN sessions parent ON parent.id = child.parent_session_id
			WHERE child.org_id = $1 AND child.harness_id = $2 AND child.harness_session_id = $3`,
			orgID, harnessID, targetID).Scan(&gotParent)).To(Succeed())
		Expect(gotParent).To(Equal(parentID))
	})

	It("synthesizes an effective envelope when the immutable raw row had none", func() {
		const (
			harnessID = "codex"
			sourceID  = "envelope-less-source"
			targetID  = "envelope-less-target"
		)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "codex",
			HarnessID: harnessID, HarnessSessionID: sourceID, RequestID: "repair-envelope-less",
			RawRequest: json.RawMessage(
				`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":"repair envelope"}]}`),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"repaired"}]},"stop_reason":"end_turn"}`),
			Meta: json.RawMessage(`{"thread_id":"wrong-thread"}`),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())

		rawTurnID := rawTurnIDForRequest(ctx, driver, orgID, "repair-envelope-less")
		_, err = driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: rawTurnID, HarnessID: harnessID,
			HarnessSessionID: targetID, ThreadID: "correct-thread", Reason: "exact identity evidence",
		})
		Expect(err).NotTo(HaveOccurred())

		rows, err := driver.ListRawTurns(ctx, 0, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].SessionEnvelope).To(MatchJSON(
			`{"harness_id":"codex","harness_session_id":"envelope-less-target"}`))
		var storedEnvelope *string
		Expect(driver.DB().QueryRow(ctx,
			"SELECT session_envelope::text FROM raw_turns WHERE org_id = $1 AND id = $2",
			orgID, rawTurnID).Scan(&storedEnvelope)).To(Succeed())
		Expect(storedEnvelope).To(BeNil(), "read-time repair must not materialize an envelope in raw_turns")
	})

	It("expands target liveness across out-of-order repairs without erasing its auth subject", func() {
		const (
			harnessID = "codex"
			targetID  = "ordered-target"
			earlyID   = "ordered-source-early"
			lateID    = "ordered-source-late"
		)
		early := time.Date(2026, time.July, 10, 8, 0, 0, 0, time.UTC)
		middle := early.Add(2 * time.Hour)
		late := early.Add(4 * time.Hour)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (
				id, org_id, auth_subject, harness_id, harness_session_id,
				started_at, last_seen_at
			) VALUES
				($1, $2, 'source-early', $3, $4, $5, $5),
				($6, $2, 'source-late', $3, $7, $8, $8),
				($9, $2, 'target-subject', $3, $10, $11, $11)`,
			newTestOrgID(), orgID, harnessID, earlyID, early,
			newTestOrgID(), lateID, late,
			newTestOrgID(), targetID, middle)
		Expect(err).NotTo(HaveOccurred())

		earlyTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, earlyID, "ordered-early")
		lateTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, lateID, "ordered-late")
		_, err = driver.DB().Exec(ctx,
			"UPDATE raw_turns SET received_at = $1 WHERE org_id = $2 AND id = $3",
			early, orgID, earlyTurnID)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx,
			"UPDATE raw_turns SET received_at = $1 WHERE org_id = $2 AND id = $3",
			late, orgID, lateTurnID)
		Expect(err).NotTo(HaveOccurred())

		repair := func(rawTurnID int64) {
			_, err := driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
				OrgID: orgID, RawTurnID: rawTurnID, HarnessID: harnessID,
				HarnessSessionID: targetID, ThreadID: "target-thread", Reason: "ordered repair evidence",
			})
			Expect(err).NotTo(HaveOccurred())
		}
		repair(lateTurnID)
		repair(earlyTurnID)

		var startedAt, lastSeenAt time.Time
		var authSubject string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT started_at, last_seen_at, auth_subject
			FROM sessions
			WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			orgID, harnessID, targetID).Scan(&startedAt, &lastSeenAt, &authSubject)).To(Succeed())
		Expect(startedAt).To(BeTemporally("==", early))
		Expect(lastSeenAt).To(BeTemporally("==", late))
		Expect(authSubject).To(Equal("target-subject"),
			"a repair without envelope auth must preserve the target's existing subject")

		repair(earlyTurnID)
		var retryStartedAt, retryLastSeenAt time.Time
		Expect(driver.DB().QueryRow(ctx, `
			SELECT started_at, last_seen_at
			FROM sessions
			WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			orgID, harnessID, targetID).Scan(&retryStartedAt, &retryLastSeenAt)).To(Succeed())
		Expect(retryStartedAt).To(BeTemporally("==", early))
		Expect(retryLastSeenAt).To(BeTemporally("==", late))
	})

	It("keeps an empty repair source that still anchors child lineage", func() {
		const (
			harnessID = "codex"
			sourceID  = "placeholder-parent"
			childID   = "placeholder-child"
			targetID  = "placeholder-target"
		)
		sourceRowID := newTestOrgID()
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (
				id, org_id, auth_subject, harness_id, harness_session_id,
				parent_session_id, started_at, last_seen_at
			) VALUES
				($1, $2, 'parent-subject', $3, $4, NULL, NOW(), NOW()),
				($5, $2, 'child-subject', $3, $6, $1, NOW(), NOW())`,
			sourceRowID, orgID, harnessID, sourceID, newTestOrgID(), childID)
		Expect(err).NotTo(HaveOccurred())
		rawTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, sourceID, "placeholder-parent-turn")

		_, err = driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: rawTurnID, HarnessID: harnessID,
			HarnessSessionID: targetID, ThreadID: "target-thread", Reason: "child lineage evidence",
		})
		Expect(err).NotTo(HaveOccurred())

		source, err := driver.GetSessionRecordByHarness(ctx, orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).NotTo(BeNil(), "a referenced zero-turn parent remains a legitimate placeholder")
		var childParentID string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT parent_session_id::text FROM sessions
			WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			orgID, harnessID, childID).Scan(&childParentID)).To(Succeed())
		Expect(childParentID).To(Equal(sourceRowID))
	})

	It("keeps a repair source while another effective raw turn remains", func() {
		const (
			harnessID = "codex"
			sourceID  = "partially-moved-source"
			targetID  = "partially-moved-target"
		)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (
				id, org_id, auth_subject, harness_id, harness_session_id,
				started_at, last_seen_at
			) VALUES ($1, $2, 'source-subject', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		movedTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, sourceID, "partially-moved")
		putAttributionRepairTurn(ctx, driver, orgID, harnessID, sourceID, "stays-on-source")
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		_, err = driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: movedTurnID, HarnessID: harnessID,
			HarnessSessionID: targetID, ThreadID: "target-thread", Reason: "single-turn evidence",
		})
		Expect(err).NotTo(HaveOccurred())

		source, err := driver.GetSessionRecordByHarness(ctx, orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).NotTo(BeNil())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(Equal(1))
	})

	It("rejects missing rows and invalid replacement fields without writing an audit record", func() {
		_, err := driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: 999, HarnessID: "codex", HarnessSessionID: "child", Reason: "evidence",
		})
		Expect(err).To(MatchError(storage.ErrRawTurnNotFound))

		_, err = driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: 999, HarnessID: "", HarnessSessionID: "child", Reason: "",
		})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("harness_id")))

		self := "child"
		_, err = driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: 999, HarnessID: "codex", HarnessSessionID: self,
			ParentHarnessSessionID: &self, Reason: "invalid lineage",
		})
		Expect(err).To(MatchError(ContainSubstring("cannot parent itself")))

		var corrections int
		Expect(driver.DB().QueryRow(ctx, "SELECT COUNT(*) FROM raw_turn_attribution_corrections").Scan(&corrections)).To(Succeed())
		Expect(corrections).To(BeZero())
	})

	It("reports pending projections and converges via the derive queue when a synchronous rederive fails", func() {
		const (
			harnessID = "codex"
			sourceID  = "pending-source"
			targetID  = "pending-target"
		)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		rawTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, sourceID, "pending-request")
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(Equal(1))

		// Fail ONLY the second (effective-session) rebuild: the previous
		// session's projection has already been rebuilt when the failure
		// hits, so the resulting state is genuinely partial.
		restore := postgres.SetRepairRederiveForTest(func(
			d *postgres.Driver, ctx context.Context, project, org, hid, hsid string,
		) (*derive.RederiveReport, error) {
			if hsid == targetID {
				return nil, errors.New("injected rederive failure")
			}
			return d.RederiveSession(ctx, project, org, hid, hsid)
		})
		defer restore()

		result, err := driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: rawTurnID, HarnessID: harnessID,
			HarnessSessionID: targetID, ThreadID: "pending-thread", Reason: "pending evidence",
		})
		Expect(err).To(MatchError(storage.ErrRepairProjectionsPending))
		Expect(err.Error()).To(ContainSubstring("injected rederive failure"))
		Expect(result.Recorded).To(BeTrue(), "the correction committed before the rebuild failed")
		Expect(result.Effective.HarnessSessionID).To(Equal(targetID))
		Expect(result.ProjectionsPending).To(ConsistOf(
			storage.RepairPendingSession{HarnessID: harnessID, HarnessSessionID: targetID}))

		// The partial state is real: the correction is effective at read
		// time, the old projection is already cleared, and the target's is
		// stale (empty) until the queue converges it.
		rows, err := driver.ListRawTurns(ctx, 0, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].HarnessSessionID).To(Equal(targetID))
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(BeZero())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, targetID)).To(BeZero())

		// The stale session is queued for the worker...
		entry, err := driver.GetDeriveDirty(ctx, orgID, harnessID, targetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(entry).NotTo(BeNil(), "a failed synchronous rebuild must leave the session derive-dirty")

		// ...and once the transient failure clears, the queue path converges
		// exactly the way the worker does: rederive under the session lock,
		// then clear the entry.
		restore()
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, entry.HarnessID, entry.HarnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		cleared, err := driver.ClearDeriveDirty(ctx, *entry)
		Expect(err).NotTo(HaveOccurred())
		Expect(cleared).To(BeTrue())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, targetID)).To(Equal(1))
	})

	It("returns the applied result with source_cleanup_pending when only the source cleanup fails", func() {
		const (
			harnessID = "codex"
			sourceID  = "cleanup-source"
			targetID  = "cleanup-target"
		)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		rawTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, sourceID, "cleanup-request")
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		restore := postgres.SetRepairSourceCleanupForTest(func(
			*postgres.Driver, context.Context, pgtype.UUID, string, string,
		) error {
			return errors.New("injected cleanup failure")
		})
		defer restore()

		result, err := driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: rawTurnID, HarnessID: harnessID,
			HarnessSessionID: targetID, ThreadID: "cleanup-thread", Reason: "cleanup evidence",
		})
		Expect(err).NotTo(HaveOccurred(),
			"a cosmetic cleanup failure must not surface an already-applied repair as an error")
		Expect(result.Recorded).To(BeTrue())
		Expect(result.Effective.HarnessSessionID).To(Equal(targetID))
		Expect(result.SourceCleanupPending).To(BeTrue())
		Expect(result.ProjectionsPending).To(BeEmpty(),
			"both projections rebuilt; only the leftover source row is outstanding")

		// The repair genuinely applied: raw reads and both projections agree,
		// and only the emptied source row is left behind.
		rows, err := driver.ListRawTurns(ctx, 0, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].HarnessSessionID).To(Equal(targetID))
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(BeZero())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, targetID)).To(Equal(1))
		source, err := driver.GetSessionRecordByHarness(ctx, orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(source).NotTo(BeNil(), "the skipped cleanup leaves the empty source row in place")
	})

	It("keeps the pending contract and joins the cleanup error when cleanup and the effective rederive both fail", func() {
		const (
			harnessID = "codex"
			sourceID  = "combined-source"
			targetID  = "combined-target"
		)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		rawTurnID := putAttributionRepairTurn(ctx, driver, orgID, harnessID, sourceID, "combined-request")
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		restoreRederive := postgres.SetRepairRederiveForTest(func(
			d *postgres.Driver, ctx context.Context, project, org, hid, hsid string,
		) (*derive.RederiveReport, error) {
			if hsid == targetID {
				return nil, errors.New("injected rederive failure")
			}
			return d.RederiveSession(ctx, project, org, hid, hsid)
		})
		defer restoreRederive()
		restoreCleanup := postgres.SetRepairSourceCleanupForTest(func(
			*postgres.Driver, context.Context, pgtype.UUID, string, string,
		) error {
			return errors.New("injected cleanup failure")
		})
		defer restoreCleanup()

		result, err := driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
			OrgID: orgID, RawTurnID: rawTurnID, HarnessID: harnessID,
			HarnessSessionID: targetID, ThreadID: "combined-thread", Reason: "combined evidence",
		})
		Expect(err).To(MatchError(storage.ErrRepairProjectionsPending))
		Expect(err.Error()).To(ContainSubstring("injected rederive failure"))
		Expect(err.Error()).To(ContainSubstring("injected cleanup failure"),
			"the cleanup failure must stay joined into the pending error, not lost")
		Expect(result.Recorded).To(BeTrue())
		Expect(result.SourceCleanupPending).To(BeTrue())
		Expect(result.ProjectionsPending).To(ConsistOf(
			storage.RepairPendingSession{HarnessID: harnessID, HarnessSessionID: targetID}))

		entry, err := driver.GetDeriveDirty(ctx, orgID, harnessID, targetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(entry).NotTo(BeNil(), "the stale projection must remain queued for the derive worker")
	})

	It("serializes a whole-org rederive with attribution repair", func() {
		const (
			harnessID = "codex"
			sourceID  = "race-source"
			targetID  = "race-target"
		)
		_, err := driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, $2, 'user-test', $3, $4, NOW(), NOW())`,
			newTestOrgID(), orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		rec := storage.RawTurnRecord{
			OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "codex",
			HarnessID: harnessID, HarnessSessionID: sourceID, RequestID: "repair-race-request",
			RawRequest: json.RawMessage(`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":"race"}]}`),
			Response:   json.RawMessage(`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"serialized"}]},"stop_reason":"end_turn"}`),
			Meta:       json.RawMessage(`{"thread_id":"wrong-thread"}`),
			SessionEnvelope: json.RawMessage(fmt.Sprintf(
				`{"org_id":%q,"auth_subject":"user-test","harness_id":%q,"harness_session_id":%q}`,
				orgID, harnessID, sourceID)),
		}
		inserted, err := driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
		_, err = driver.RederiveSessionLocked(ctx, "", orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())

		release, acquired, err := driver.TryDeriveSessionLock(ctx, orgID, harnessID, sourceID)
		Expect(err).NotTo(HaveOccurred())
		Expect(acquired).To(BeTrue())
		var releaseOnce sync.Once
		releaseHeldLock := func() { releaseOnce.Do(release) }
		DeferCleanup(releaseHeldLock)

		rederiveDone := make(chan error, 1)
		go func() {
			_, err := driver.RederiveFromRaw(ctx, "")
			rederiveDone <- err
		}()
		Consistently(rederiveDone, 200*time.Millisecond).ShouldNot(Receive(),
			"whole-org rederive must wait for the same session lock as repair and the worker")

		repairDone := make(chan error, 1)
		go func() {
			_, err := driver.RepairRawTurnAttribution(ctx, "", storage.RawTurnAttributionRepairRequest{
				OrgID: orgID, RawTurnID: rawTurnIDForRequest(ctx, driver, orgID, rec.RequestID),
				HarnessID: harnessID, HarnessSessionID: targetID, ThreadID: "correct-thread", Reason: "race evidence",
			})
			repairDone <- err
		}()
		releaseHeldLock()

		Eventually(rederiveDone).Should(Receive(BeNil()))
		Eventually(repairDone).Should(Receive(BeNil()))
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, sourceID)).To(BeZero())
		Expect(spanTurnsForHarnessSession(ctx, driver, orgID, harnessID, targetID)).To(Equal(1))
		var gotThread string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT sp.thread_id FROM spans_20260615 sp
			JOIN sessions s ON s.id = sp.session_id
			WHERE s.org_id = $1 AND s.harness_id = $2 AND s.harness_session_id = $3
			  AND sp.kind = 'llm'`, orgID, harnessID, targetID).Scan(&gotThread)).To(Succeed())
		Expect(gotThread).To(Equal("correct-thread"))
	})
})

func rawTurnIDForRequest(ctx context.Context, driver *postgres.Driver, orgID, requestID string) int64 {
	var id int64
	Expect(driver.DB().QueryRow(ctx,
		"SELECT id FROM raw_turns WHERE org_id = $1 AND request_id = $2", orgID, requestID).Scan(&id)).To(Succeed())
	return id
}

func putAttributionRepairTurn(
	ctx context.Context,
	driver *postgres.Driver,
	orgID, harnessID, harnessSessionID, requestID string,
) int64 {
	inserted, err := driver.PutRawTurn(ctx, storage.RawTurnRecord{
		OrgID: orgID, Source: storage.RawTurnSourceWire, Provider: "anthropic", AgentName: "codex",
		HarnessID: harnessID, HarnessSessionID: harnessSessionID, RequestID: requestID,
		RawRequest: json.RawMessage(
			`{"model":"claude-test","max_tokens":4096,"messages":[{"role":"user","content":"repair"}]}`),
		Response: json.RawMessage(
			`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"repaired"}]},"stop_reason":"end_turn"}`),
		Meta: json.RawMessage(`{"thread_id":"source-thread"}`),
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(inserted).To(BeTrue())
	return rawTurnIDForRequest(ctx, driver, orgID, requestID)
}

func spanTurnsForHarnessSession(ctx context.Context, driver *postgres.Driver, orgID, harnessID, harnessSessionID string) int {
	var count int
	Expect(driver.DB().QueryRow(ctx, `
		SELECT COUNT(*)
		FROM span_turns_20260615 st
		JOIN sessions s ON s.id = st.session_id
		WHERE s.org_id = $1 AND s.harness_id = $2 AND s.harness_session_id = $3`,
		orgID, harnessID, harnessSessionID).Scan(&count)).To(Succeed())
	return count
}

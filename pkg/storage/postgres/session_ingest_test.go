package postgres_test

import (
	"context"
	"encoding/hex"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// sessionFixture is a tiny helper to materialize a 2-node turn chain
// (one user message + one assistant response) suitable for IngestTurn
// requests. The text argument seeds the user message so successive
// fixtures within the same spec produce distinct hashes.
func sessionFixture(text string) []*merkle.Node {
	userBucket := merkle.Bucket{
		Type:     "message",
		Role:     "user",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
	user := merkle.NewNode(userBucket, nil)

	respBucket := merkle.Bucket{
		Type:     "message",
		Role:     "assistant",
		Content:  []llm.ContentBlock{{Type: "text", Text: "ok: " + text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
	resp := merkle.NewNode(respBucket, user, merkle.NodeOptions{
		StopReason: "stop",
		Usage: &llm.Usage{
			PromptTokens:     12,
			CompletionTokens: 8,
			TotalTokens:      20,
		},
	})
	return []*merkle.Node{user, resp}
}

func newTestOrgID() string {
	return uuid.New().String()
}

// Driver.IngestTurn no longer persists merkle nodes and no longer bumps the
// per-turn token/turn/cost counters — node persistence is retired (the merkle
// chain is in-memory only) and the session rollups are owned by the
// derive-time span fold (FoldSessionRollupsFromSpans). What IngestTurn still
// does, and what these specs assert, is: UPSERT the sessions row keyed by the
// envelope's natural key (or a synthetic harness_session_id from the turn's
// Merkle root), resolve the optional fork-parent FK (placeholder-inserting the
// parent when its own first turn hasn't landed yet), and fold derived_status
// from the in-memory turn chain.
var _ = Describe("Driver.IngestTurn", func() {
	var (
		driver   storage.Driver
		ingester storage.SessionIngester
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).ToNot(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		pgDriver, ok := driver.(*postgres.Driver)
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

	It("UPSERTs a sessions row keyed by the envelope's natural key", func() {
		orgID := newTestOrgID()
		envelope := &sessions.IngestEnvelope{
			OrgID:            orgID,
			AuthSubject:      "subject-a",
			HarnessID:        "claude",
			HarnessSessionID: "harness-happy",
		}

		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      envelope,
			Nodes:        sessionFixture("happy path"),
			InputTokens:  12,
			OutputTokens: 8,
			CostUSD:      0,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())

		// The sessions row exists with the resolved natural key. Token/turn
		// counters are NOT asserted here: ingest no longer maintains them
		// (the derive-time span fold owns those rollups).
		pgDriver := driver.(*postgres.Driver)
		var (
			harnessID        string
			harnessSessionID string
		)
		err = pgDriver.DB().QueryRow(ctx, `
			SELECT harness_id, harness_session_id
			  FROM sessions
			 WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			mustUUID(orgID), "claude", "harness-happy").Scan(&harnessID, &harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(harnessID).To(Equal("claude"))
		Expect(harnessSessionID).To(Equal("harness-happy"))
	})

	It("derives status 'completed' for an assistant leaf with a terminal stop_reason", func() {
		orgID := newTestOrgID()
		env := &sessions.IngestEnvelope{OrgID: orgID, AuthSubject: "s", HarnessID: "claude", HarnessSessionID: "hs-complete"}

		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{Session: env, Nodes: sessionFixture("done")})
		Expect(err).NotTo(HaveOccurred())

		var status string
		var toolErrors int
		var hasGit bool
		pg := driver.(*postgres.Driver)
		Expect(pg.DB().QueryRow(ctx, `SELECT derived_status, tool_error_count, has_git_activity FROM sessions WHERE org_id=$1 AND harness_id=$2 AND harness_session_id=$3`,
			mustUUID(orgID), "claude", "hs-complete").Scan(&status, &toolErrors, &hasGit)).To(Succeed())
		Expect(status).To(Equal(sessions.StatusCompleted))
		Expect(toolErrors).To(Equal(0))
		Expect(hasGit).To(BeFalse())
	})

	It("derives 'completed' via git activity even when the leaf is a non-terminal tool_result (PCC-515)", func() {
		orgID := newTestOrgID()
		env := &sessions.IngestEnvelope{OrgID: orgID, AuthSubject: "s", HarnessID: "claude", HarnessSessionID: "hs-git"}

		user := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "ship it"}}}, nil)
		asst := merkle.NewNode(merkle.Bucket{Type: "message", Role: "assistant", Content: []llm.ContentBlock{{Type: "tool_use", ToolName: "Bash", ToolInput: map[string]any{"command": "git commit -m wip"}}}}, user, merkle.NodeOptions{StopReason: "tool_use"})
		// Leaf is the tool_result (user role, no terminal stop_reason): the
		// leaf-only SQL classifier would call this abandoned, while the
		// chain-aware classifier sees the git commit and returns completed.
		toolRes := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: []llm.ContentBlock{{Type: "tool_result", ToolOutput: "ok"}}}, asst)

		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{Session: env, Nodes: []*merkle.Node{user, asst, toolRes}})
		Expect(err).NotTo(HaveOccurred())

		var status string
		var hasGit bool
		pg := driver.(*postgres.Driver)
		Expect(pg.DB().QueryRow(ctx, `SELECT derived_status, has_git_activity FROM sessions WHERE org_id=$1 AND harness_id=$2 AND harness_session_id=$3`,
			mustUUID(orgID), "claude", "hs-git").Scan(&status, &hasGit)).To(Succeed())
		Expect(status).To(Equal(sessions.StatusCompleted))
		Expect(hasGit).To(BeTrue())
	})

	It("derives 'failed' when the session ends on an unrecovered tool_result error", func() {
		orgID := newTestOrgID()
		env := &sessions.IngestEnvelope{OrgID: orgID, AuthSubject: "s", HarnessID: "claude", HarnessSessionID: "hs-err"}

		user := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "do it"}}}, nil)
		asst := merkle.NewNode(merkle.Bucket{Type: "message", Role: "assistant", Content: []llm.ContentBlock{{Type: "tool_use", ToolName: "Bash", ToolInput: map[string]any{"command": "ls"}}}}, user, merkle.NodeOptions{StopReason: "tool_use"})
		toolRes := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: []llm.ContentBlock{{Type: "tool_result", IsError: true, ToolOutput: "boom"}}}, asst)

		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{Session: env, Nodes: []*merkle.Node{user, asst, toolRes}})
		Expect(err).NotTo(HaveOccurred())

		var status string
		var toolErrors int
		pg := driver.(*postgres.Driver)
		Expect(pg.DB().QueryRow(ctx, `SELECT derived_status, tool_error_count FROM sessions WHERE org_id=$1 AND harness_id=$2 AND harness_session_id=$3`,
			mustUUID(orgID), "claude", "hs-err").Scan(&status, &toolErrors)).To(Succeed())
		Expect(status).To(Equal(sessions.StatusFailed))
		Expect(toolErrors).To(Equal(1))
	})

	It("folds derived_status from the in-memory chain at ingest time", func() {
		// derived_status is computed and written on every IngestTurn from the
		// in-memory turn chain (it is not a span-fold output), so a freshly
		// ingested session already carries the correct status with no separate
		// backfill pass.
		orgID := newTestOrgID()
		env := &sessions.IngestEnvelope{OrgID: orgID, AuthSubject: "s", HarnessID: "claude", HarnessSessionID: "hs-status-fold"}
		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{Session: env, Nodes: sessionFixture("status fold")})
		Expect(err).NotTo(HaveOccurred())

		pg := driver.(*postgres.Driver)
		var status string
		Expect(pg.DB().QueryRow(ctx, `SELECT derived_status FROM sessions WHERE org_id=$1 AND harness_id=$2 AND harness_session_id=$3`,
			mustUUID(orgID), "claude", "hs-status-fold").Scan(&status)).To(Succeed())
		Expect(status).To(Equal(sessions.StatusCompleted))
	})

	It("is idempotent across a retried envelope: one row, same session id", func() {
		orgID := newTestOrgID()
		envelope := &sessions.IngestEnvelope{
			OrgID:            orgID,
			AuthSubject:      "subject-b",
			HarnessID:        "claude",
			HarnessSessionID: "harness-retry",
		}
		nodes := sessionFixture("retry me")

		req := storage.IngestTurnRequest{
			Session:      envelope,
			Nodes:        nodes,
			InputTokens:  10,
			OutputTokens: 4,
		}
		res1, err := ingester.IngestTurn(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		res2, err := ingester.IngestTurn(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.SessionID).To(Equal(res1.SessionID), "retry must resolve to the same sessions row")

		pgDriver := driver.(*postgres.Driver)
		var rowCount int
		Expect(pgDriver.DB().QueryRow(ctx, `SELECT COUNT(*) FROM sessions WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), "harness-retry").Scan(&rowCount)).To(Succeed())
		Expect(rowCount).To(Equal(1))
	})

	It("FKs to an existing parent session when ParentHarnessSessionID resolves", func() {
		orgID := newTestOrgID()
		parentKey := "parent-session"
		parentEnv := &sessions.IngestEnvelope{
			OrgID:            orgID,
			AuthSubject:      "subject-c",
			HarnessID:        "claude",
			HarnessSessionID: parentKey,
		}
		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      parentEnv,
			Nodes:        sessionFixture("parent turn"),
			InputTokens:  3,
			OutputTokens: 2,
		})
		Expect(err).NotTo(HaveOccurred())

		childEnv := &sessions.IngestEnvelope{
			OrgID:                  orgID,
			AuthSubject:            "subject-c",
			HarnessID:              "claude",
			HarnessSessionID:       "child-session",
			ParentHarnessSessionID: &parentKey,
		}
		childRes, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      childEnv,
			Nodes:        sessionFixture("child turn"),
			InputTokens:  4,
			OutputTokens: 3,
		})
		Expect(err).NotTo(HaveOccurred())

		pgDriver := driver.(*postgres.Driver)
		var parentSessionID pgtype.UUID
		err = pgDriver.DB().QueryRow(ctx, `SELECT parent_session_id FROM sessions WHERE id = $1`, mustUUID(childRes.SessionID)).Scan(&parentSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(parentSessionID.Valid).To(BeTrue())

		var parentRowID pgtype.UUID
		err = pgDriver.DB().QueryRow(ctx, `SELECT id FROM sessions WHERE org_id = $1 AND harness_session_id = $2`, mustUUID(orgID), parentKey).Scan(&parentRowID)
		Expect(err).NotTo(HaveOccurred())
		Expect(parentSessionID.Bytes).To(Equal(parentRowID.Bytes))
	})

	It("placeholder-inserts a missing parent and FKs the child to it", func() {
		orgID := newTestOrgID()
		parentKey := "ghost-parent"
		childEnv := &sessions.IngestEnvelope{
			OrgID:                  orgID,
			AuthSubject:            "subject-d",
			HarnessID:              "claude",
			HarnessSessionID:       "child-of-ghost",
			ParentHarnessSessionID: &parentKey,
		}

		childRes, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      childEnv,
			Nodes:        sessionFixture("orphan child"),
			InputTokens:  1,
			OutputTokens: 1,
		})
		Expect(err).NotTo(HaveOccurred())

		pgDriver := driver.(*postgres.Driver)

		// Parent placeholder row exists.
		var parentRowID pgtype.UUID
		Expect(pgDriver.DB().QueryRow(ctx, `SELECT id FROM sessions WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), parentKey).Scan(&parentRowID)).To(Succeed())

		// Child FK references the placeholder's id.
		var childParentID pgtype.UUID
		Expect(pgDriver.DB().QueryRow(ctx, `SELECT parent_session_id FROM sessions WHERE id = $1`, mustUUID(childRes.SessionID)).Scan(&childParentID)).To(Succeed())
		Expect(childParentID.Valid).To(BeTrue())
		Expect(childParentID.Bytes).To(Equal(parentRowID.Bytes))
	})

	It("derives a synthetic harness_session_id from the root hash when the envelope is nil", func() {
		nodes := sessionFixture("synthetic")
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      nil,
			Nodes:        nodes,
			InputTokens:  2,
			OutputTokens: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())

		pgDriver := driver.(*postgres.Driver)
		var harnessID, harnessSessionID string
		err = pgDriver.DB().QueryRow(ctx, `SELECT harness_id, harness_session_id FROM sessions WHERE id = $1`, mustUUID(res.SessionID)).Scan(&harnessID, &harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(harnessID).To(Equal("unknown"))

		// First 16 hex chars of the root (= first input node) hash.
		Expect(nodes[0].Hash).NotTo(BeEmpty())
		Expect(harnessSessionID).To(Equal(nodes[0].Hash[:16]))
		_, decodeErr := hex.DecodeString(harnessSessionID)
		Expect(decodeErr).NotTo(HaveOccurred())
	})

	It("backfills mutable fields (auth_subject, name, cwd, harness_version) when the parent's first real turn lands after a child placeholder", func() {
		orgID := newTestOrgID()
		parentKey := "ghost-parent-backfill"

		// 1. Child ingests first with a parent hint. The parent doesn't
		//    exist yet, so resolveParentSessionID inserts a placeholder.
		//    The placeholder row carries the *child's* auth_subject
		//    because InsertSessionPlaceholder has no knowledge of who
		//    the parent really is.
		childAuthSubject := "child-subject"
		childEnv := &sessions.IngestEnvelope{
			OrgID:                  orgID,
			AuthSubject:            childAuthSubject,
			HarnessID:              "claude",
			HarnessSessionID:       "child-of-pending",
			ParentHarnessSessionID: &parentKey,
		}
		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      childEnv,
			Nodes:        sessionFixture("child first"),
			InputTokens:  1,
			OutputTokens: 1,
		})
		Expect(err).NotTo(HaveOccurred())

		pgDriver := driver.(*postgres.Driver)

		// Sanity: placeholder row exists with the child's auth_subject and no
		// name/cwd/version yet (this is the state we must back-fill).
		var (
			placeholderAuth    string
			placeholderName    pgtype.Text
			placeholderCwd     pgtype.Text
			placeholderVersion pgtype.Text
		)
		err = pgDriver.DB().QueryRow(ctx, `
			SELECT auth_subject, name, cwd, harness_version
			  FROM sessions
			 WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), parentKey,
		).Scan(&placeholderAuth, &placeholderName, &placeholderCwd, &placeholderVersion)
		Expect(err).NotTo(HaveOccurred())
		Expect(placeholderAuth).To(Equal(childAuthSubject), "placeholder should temporarily carry the child's auth_subject")
		Expect(placeholderName.Valid).To(BeFalse(), "placeholder has no name yet")
		Expect(placeholderCwd.Valid).To(BeFalse(), "placeholder has no cwd yet")
		Expect(placeholderVersion.Valid).To(BeFalse(), "placeholder has no harness_version yet")

		// 2. Parent's first real turn lands. UpsertSession must overwrite
		//    auth_subject (reclaiming attribution) and merge the remaining
		//    mutable fields.
		parentAuthSubject := "parent-subject"
		parentEnv := &sessions.IngestEnvelope{
			OrgID:            orgID,
			AuthSubject:      parentAuthSubject,
			HarnessID:        "claude",
			HarnessSessionID: parentKey,
			Name:             "parent name",
			Cwd:              "/parent/cwd",
			HarnessVersion:   "1.2.3",
		}
		parentRes, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      parentEnv,
			Nodes:        sessionFixture("parent backfills"),
			InputTokens:  7,
			OutputTokens: 5,
		})
		Expect(err).NotTo(HaveOccurred())

		// 3. Assert: same row (the placeholder's id is preserved), with
		//    every mutable field now reflecting the parent's authoritative
		//    values.
		var (
			finalAuth    string
			finalName    pgtype.Text
			finalCwd     pgtype.Text
			finalVersion pgtype.Text
		)
		err = pgDriver.DB().QueryRow(ctx, `
			SELECT auth_subject, name, cwd, harness_version
			  FROM sessions
			 WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), parentKey,
		).Scan(&finalAuth, &finalName, &finalCwd, &finalVersion)
		Expect(err).NotTo(HaveOccurred())
		Expect(finalAuth).To(Equal(parentAuthSubject), "parent's real upsert must overwrite the child-borrowed auth_subject")
		Expect(finalName.Valid).To(BeTrue())
		Expect(finalName.String).To(Equal("parent name"))
		Expect(finalCwd.Valid).To(BeTrue())
		Expect(finalCwd.String).To(Equal("/parent/cwd"))
		Expect(finalVersion.Valid).To(BeTrue())
		Expect(finalVersion.String).To(Equal("1.2.3"))

		// 4. The sessions row id is the placeholder's id (preserved by the
		//    ON CONFLICT path), so the child's parent_session_id FK still
		//    resolves.
		var parentRowID pgtype.UUID
		Expect(pgDriver.DB().QueryRow(ctx, `SELECT id FROM sessions WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), parentKey).Scan(&parentRowID)).To(Succeed())
		Expect(uuidString(parentRowID)).To(Equal(parentRes.SessionID))

		var childParentID pgtype.UUID
		Expect(pgDriver.DB().QueryRow(ctx, `SELECT parent_session_id FROM sessions WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), "child-of-pending").Scan(&childParentID)).To(Succeed())
		Expect(childParentID.Valid).To(BeTrue())
		Expect(childParentID.Bytes).To(Equal(parentRowID.Bytes))
	})
})

// mustUUID converts a canonical UUID string to a pgtype.UUID, failing
// the surrounding spec if the input is not a valid UUID. Used to build
// query args inline in spec assertions.
func mustUUID(s string) pgtype.UUID {
	parsed, err := uuid.Parse(s)
	Expect(err).NotTo(HaveOccurred())
	return pgtype.UUID{Bytes: parsed, Valid: true}
}

// uuidString renders a pgtype.UUID as its canonical 36-char hyphenated
// form, or "" when Valid is false. Lets specs cross-check IDs returned
// from IngestTurn against rows queried back via the pgx connection.
func uuidString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	var u uuid.UUID
	copy(u[:], id.Bytes[:])
	return u.String()
}

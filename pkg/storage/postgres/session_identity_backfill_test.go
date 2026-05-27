package postgres_test

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// identityBucket builds a deterministic assistant bucket. Identical text
// yields an identical content hash regardless of which org ingests it —
// that is the multi-org collision the composite (org_id, hash) PK exists
// to keep separate.
func identityBucket(text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     "assistant",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

var _ = Describe("Driver.SessionIdentityByHash", func() {
	var (
		driver   storage.Driver
		ingester storage.SessionIngester
		pg       *postgres.Driver
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).ToNot(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pg, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pg.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
		_, err = pg.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		ingester, ok = driver.(storage.SessionIngester)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionIngester")
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	It("returns the attached session's harness identity for a node that has a session", func() {
		orgID := newTestOrgID()
		env := &sessions.IngestEnvelope{
			OrgID:            orgID,
			AuthSubject:      "subj",
			HarnessID:        "claude",
			HarnessSessionID: "session-with-identity",
		}
		nodes := sessionFixture("identity present")
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session:      env,
			Nodes:        nodes,
			InputTokens:  1,
			OutputTokens: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.NewNodes).To(HaveLen(2))

		// Query identity for the leaf (assistant) node's hash, scoped to org.
		identity, err := pg.SessionIdentityByHash(ctx, orgID, nodes[1].Hash)
		Expect(err).NotTo(HaveOccurred())
		Expect(identity).NotTo(BeNil())
		Expect(identity.HarnessID).To(Equal("claude"))
		Expect(identity.HarnessSessionID).To(Equal("session-with-identity"))
	})

	It("returns (nil, nil) for a node hash whose session_id is NULL", func() {
		// Driver.Put writes a node with the nil-org sentinel and no
		// session_id, exactly the legacy shape.
		node := merkle.NewNode(identityBucket("orphan node"), nil)
		_, err := driver.Put(ctx, node)
		Expect(err).NotTo(HaveOccurred())

		// Sanity: the row exists but its session_id is NULL.
		var sid pgtype.UUID
		Expect(pg.DB().QueryRow(ctx, `SELECT session_id FROM nodes WHERE hash = $1`, node.Hash).Scan(&sid)).To(Succeed())
		Expect(sid.Valid).To(BeFalse(), "node should have a NULL session_id")

		// Driver.Put lands on the nil-org sentinel bucket; query that org
		// (empty string maps to the sentinel).
		identity, err := pg.SessionIdentityByHash(ctx, "", node.Hash)
		Expect(err).NotTo(HaveOccurred())
		Expect(identity).To(BeNil())
	})

	It("returns (nil, nil) for a hash that exists in no node at all", func() {
		identity, err := pg.SessionIdentityByHash(ctx, "", "0000000000000000000000000000000000000000000000000000000000000000")
		Expect(err).NotTo(HaveOccurred())
		Expect(identity).To(BeNil())
	})

	// MULTI-ORG INVARIANT: two orgs ingest identical content, so both own a
	// node with the SAME hash (distinguished only by org_id in the composite
	// PK), but each node belongs to that org's own session with a DIFFERENT
	// harness_session_id. Resolving identity for one org's node must return
	// THAT org's harness identity, never the other org's.
	It("returns the querying org's identity, not the other org's, when two orgs share an identical node hash", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()

		// Both turns carry IDENTICAL content so the assistant node hash
		// collides across the two orgs.
		makeNodes := func() []*merkle.Node {
			userBucket := merkle.Bucket{
				Type:     "message",
				Role:     "user",
				Content:  []llm.ContentBlock{{Type: "text", Text: "shared content"}},
				Model:    "test-model",
				Provider: "test-provider",
			}
			user := merkle.NewNode(userBucket, nil)
			resp := merkle.NewNode(identityBucket("shared content"), user, merkle.NodeOptions{StopReason: "stop"})
			return []*merkle.Node{user, resp}
		}

		nodesA := makeNodes()
		nodesB := makeNodes()
		// Confirm the precondition: the leaf hashes are equal across orgs.
		Expect(nodesA[1].Hash).To(Equal(nodesB[1].Hash), "test precondition: identical content must hash identically")
		sharedHash := nodesA[1].Hash

		// Ingest org B's turn FIRST, then org A's. The lookup is scoped by
		// org_id, so insert order must not matter: querying either org's id
		// for the shared hash must return THAT org's identity. (Order is
		// deliberately reversed from the assertion below to catch a "first
		// physical row wins" regression — an unscoped query seeded B-first
		// would wrongly resolve org A's query to org B.)
		_, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgB,
				AuthSubject:      "subj-b",
				HarnessID:        "claude",
				HarnessSessionID: "org-b-session",
			},
			Nodes:        nodesB,
			InputTokens:  1,
			OutputTokens: 1,
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgA,
				AuthSubject:      "subj-a",
				HarnessID:        "claude",
				HarnessSessionID: "org-a-session",
			},
			Nodes:        nodesA,
			InputTokens:  1,
			OutputTokens: 1,
		})
		Expect(err).NotTo(HaveOccurred())

		// Sanity: two distinct node rows exist for the same hash, one per org,
		// each FKing to a different session.
		var rowCount int
		Expect(pg.DB().QueryRow(ctx, `SELECT COUNT(*) FROM nodes WHERE hash = $1`, sharedHash).Scan(&rowCount)).To(Succeed())
		Expect(rowCount).To(Equal(2), "each org owns its own copy of the identical-content node")

		// The spec's invariant: the org_id passed to the lookup selects which
		// org's copy of the shared-hash node — and therefore which session's
		// identity — is returned. The assertions are symmetric so neither org
		// can win by physical row order: org A's query must yield org A's
		// session, org B's must yield org B's.
		idA, err := pg.SessionIdentityByHash(ctx, orgA, sharedHash)
		Expect(err).NotTo(HaveOccurred())
		Expect(idA).NotTo(BeNil())
		Expect(idA.HarnessSessionID).To(Equal("org-a-session"),
			"identity for org A's query must be org A's, not org B's (got %q)", idA.HarnessSessionID)

		idB, err := pg.SessionIdentityByHash(ctx, orgB, sharedHash)
		Expect(err).NotTo(HaveOccurred())
		Expect(idB).NotTo(BeNil())
		Expect(idB.HarnessSessionID).To(Equal("org-b-session"),
			"identity for org B's query must be org B's, not org A's (got %q)", idB.HarnessSessionID)
	})
})

var _ = Describe("Driver.BackfillSession", func() {
	var (
		driver     storage.Driver
		backfiller storage.SessionBackfiller
		pg         *postgres.Driver
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).ToNot(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pg, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pg.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
		_, err = pg.DB().Exec(ctx, "TRUNCATE TABLE sessions CASCADE")
		Expect(err).NotTo(HaveOccurred())

		backfiller, ok = driver.(storage.SessionBackfiller)
		Expect(ok).To(BeTrue(), "postgres driver must satisfy SessionBackfiller")
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// insertOrgNode writes a node row for a specific org WITHOUT a session_id,
	// mirroring the legacy pre-session shape that backfill is meant to link.
	insertOrgNode := func(orgID string, hash, text string) {
		_, err := pg.DB().Exec(ctx, `
INSERT INTO nodes (org_id, hash, bucket, content, role, created_at)
VALUES ($1, $2, $3::jsonb, $4::jsonb, 'assistant', now())`,
			mustUUID(orgID), hash, `{"role":"assistant"}`, `[{"type":"text","text":"`+text+`"}]`)
		Expect(err).NotTo(HaveOccurred())
	}

	sessionIDOfNode := func(orgID, hash string) pgtype.UUID {
		var sid pgtype.UUID
		Expect(pg.DB().QueryRow(ctx, `SELECT session_id FROM nodes WHERE org_id = $1 AND hash = $2`,
			mustUUID(orgID), hash).Scan(&sid)).To(Succeed())
		return sid
	}

	It("upserts the session row and stamps session_id onto the org's NULL-session nodes; NodesLinked counts the stamped rows", func() {
		orgID := newTestOrgID()
		h1 := "aaaa000000000000000000000000000000000000000000000000000000000001"
		h2 := "aaaa000000000000000000000000000000000000000000000000000000000002"
		insertOrgNode(orgID, h1, "node one")
		insertOrgNode(orgID, h2, "node two")

		res, err := backfiller.BackfillSession(ctx, storage.SessionBackfillRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "backfill-subj",
				HarnessID:        "claude",
				HarnessSessionID: "backfilled-session",
			},
			NodeHashes:   []string{h1, h2},
			InputTokens:  100,
			OutputTokens: 50,
			TurnCount:    2,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		Expect(res.NodesLinked).To(Equal(2))

		// The session row exists on its natural key.
		var (
			harnessID, harnessSessionID string
			turnCount                   int32
		)
		Expect(pg.DB().QueryRow(ctx, `
			SELECT harness_id, harness_session_id, turn_count
			  FROM sessions WHERE org_id = $1 AND harness_id = $2 AND harness_session_id = $3`,
			mustUUID(orgID), "claude", "backfilled-session").Scan(&harnessID, &harnessSessionID, &turnCount)).To(Succeed())
		Expect(harnessID).To(Equal("claude"))
		Expect(harnessSessionID).To(Equal("backfilled-session"))
		Expect(turnCount).To(Equal(int32(2)))

		// Both nodes now carry the session_id FK.
		Expect(sessionIDOfNode(orgID, h1).Valid).To(BeTrue())
		Expect(sessionIDOfNode(orgID, h2).Valid).To(BeTrue())
	})

	It("only stamps nodes belonging to the envelope's org; another org's identical-hash node stays NULL", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()
		sharedHash := "bbbb000000000000000000000000000000000000000000000000000000000001"

		// Both orgs own a node with the SAME hash (composite PK keeps them
		// distinct). Neither has a session yet.
		insertOrgNode(orgA, sharedHash, "shared")
		insertOrgNode(orgB, sharedHash, "shared")

		res, err := backfiller.BackfillSession(ctx, storage.SessionBackfillRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgA,
				AuthSubject:      "subj-a",
				HarnessID:        "claude",
				HarnessSessionID: "org-a-backfill",
			},
			NodeHashes: []string{sharedHash},
			TurnCount:  1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.NodesLinked).To(Equal(1), "must stamp only org A's copy")

		// Org A's node is stamped; org B's identical-hash node is untouched.
		Expect(sessionIDOfNode(orgA, sharedHash).Valid).To(BeTrue(), "org A's node should be linked")
		Expect(sessionIDOfNode(orgB, sharedHash).Valid).To(BeFalse(), "org B's node must remain NULL")
	})

	It("is idempotent: re-running the same backfill does not double-link and reports zero additional links", func() {
		orgID := newTestOrgID()
		h1 := "cccc000000000000000000000000000000000000000000000000000000000001"
		insertOrgNode(orgID, h1, "idem node")

		req := storage.SessionBackfillRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subj",
				HarnessID:        "claude",
				HarnessSessionID: "idem-session",
			},
			NodeHashes: []string{h1},
			TurnCount:  1,
		}

		res1, err := backfiller.BackfillSession(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.NodesLinked).To(Equal(1))
		firstSessionID := sessionIDOfNode(orgID, h1)
		Expect(firstSessionID.Valid).To(BeTrue())

		res2, err := backfiller.BackfillSession(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.NodesLinked).To(Equal(0), "second run must not re-link an already-linked node")

		// Still exactly one session row on the natural key.
		var rowCount int
		Expect(pg.DB().QueryRow(ctx, `SELECT COUNT(*) FROM sessions WHERE org_id = $1 AND harness_session_id = $2`,
			mustUUID(orgID), "idem-session").Scan(&rowCount)).To(Succeed())
		Expect(rowCount).To(Equal(1))

		// The node's session_id is unchanged.
		Expect(sessionIDOfNode(orgID, h1).Bytes).To(Equal(firstSessionID.Bytes))
	})

	It("does not silently reassign nodes that already have a session_id", func() {
		orgID := newTestOrgID()
		h1 := "dddd000000000000000000000000000000000000000000000000000000000001"
		insertOrgNode(orgID, h1, "already linked")

		// First backfill links the node to session ONE.
		res1, err := backfiller.BackfillSession(ctx, storage.SessionBackfillRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subj",
				HarnessID:        "claude",
				HarnessSessionID: "session-one",
			},
			NodeHashes: []string{h1},
			TurnCount:  1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res1.NodesLinked).To(Equal(1))
		linkedSessionID := sessionIDOfNode(orgID, h1)
		Expect(linkedSessionID.Valid).To(BeTrue())

		// A DIFFERENT session now attempts to backfill the SAME node hash.
		res2, err := backfiller.BackfillSession(ctx, storage.SessionBackfillRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subj",
				HarnessID:        "claude",
				HarnessSessionID: "session-two",
			},
			NodeHashes: []string{h1},
			TurnCount:  1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res2.NodesLinked).To(Equal(0), "node already has a session_id and must not be reassigned")

		// The node still points at session ONE.
		Expect(sessionIDOfNode(orgID, h1).Bytes).To(Equal(linkedSessionID.Bytes),
			"node's session_id must still reference the original session")
	})
})

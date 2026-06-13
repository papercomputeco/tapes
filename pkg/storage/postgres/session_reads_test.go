package postgres_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver.GetSessionRecordByHarness", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ingester storage.SessionIngester
		ctx      context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
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

	// seedSession ingests a 2-node turn for the given org and harness
	// identity, returning the tapes-minted session UUID. The text seeds
	// the user message so it becomes the session's preview.
	seedSession := func(orgID, harnessID, harnessSessionID, text string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subject-reads",
				HarnessID:        harnessID,
				HarnessSessionID: harnessSessionID,
			},
			Nodes:        sessionFixture(text),
			InputTokens:  12,
			OutputTokens: 8,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	It("returns the matching record with preview for an exact org-scoped natural key", func() {
		orgID := newTestOrgID()
		sessionID := seedSession(orgID, "claude", "harness-exact", "preview text for exact match")

		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, "claude", "harness-exact")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())

		Expect(rec.ID).To(Equal(sessionID))
		Expect(rec.HarnessID).To(Equal("claude"))
		Expect(rec.HarnessSessionID).To(Equal("harness-exact"))
		Expect(rec.TurnCount).To(Equal(1))
		Expect(rec.TotalInputTokens).To(Equal(int64(12)))
		Expect(rec.TotalOutputTokens).To(Equal(int64(8)))
		Expect(rec.Preview).To(Equal("preview text for exact match"))

		// Parity with the list path: the single filtered row must
		// carry the same field population as a ListSessionRecords row,
		// including Preview attached via getSessionPreviews.
		listed, err := pgDriver.ListSessionRecords(ctx, orgID, "", 10, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(HaveLen(1))
		Expect(listed[0].ID).To(Equal(rec.ID))
		Expect(listed[0].Preview).To(Equal(rec.Preview))
	})

	It("returns nil without error when no row matches the natural key", func() {
		orgID := newTestOrgID()
		// Seed an unrelated session so the miss exercises the index
		// against real rows rather than an empty table.
		seedSession(orgID, "claude", "harness-present", "some other session")

		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, "claude", "harness-absent")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil())
	})

	It("does not return a session with the same harness identity under a different org", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()

		// Identical (harness_id, harness_session_id) pair seeded under
		// two different orgs; the unique index is only unique per-org.
		idA := seedSession(orgA, "claude", "shared-harness-session", "org A turn")
		idB := seedSession(orgB, "claude", "shared-harness-session", "org B turn")
		Expect(idA).NotTo(Equal(idB))

		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgA, "claude", "shared-harness-session")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.ID).To(Equal(idA))
		Expect(rec.ID).NotTo(Equal(idB), "org B's session UUID must never surface for org A")
		Expect(rec.Preview).To(Equal("org A turn"))
	})

	It("filters the paged list by auth_subject within an org", func() {
		// Given two sessions in one org captured for different users
		orgID := newTestOrgID()
		seedFor := func(subject, harnessSession, text string) string {
			res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
				Session: &sessions.IngestEnvelope{
					OrgID:            orgID,
					AuthSubject:      subject,
					HarnessID:        "claude",
					HarnessSessionID: harnessSession,
				},
				Nodes:        sessionFixture(text),
				InputTokens:  12,
				OutputTokens: 8,
			})
			Expect(err).NotTo(HaveOccurred())
			return res.SessionID
		}
		idAlice := seedFor("user_alice", "sess-alice", "alice turn")
		_ = seedFor("user_bob", "sess-bob", "bob turn")

		// When listing with alice's subject
		mine, err := pgDriver.ListSessionRecords(ctx, orgID, "user_alice", 10, nil, nil)
		Expect(err).NotTo(HaveOccurred())

		// Then only alice's session returns, carrying the subject
		Expect(mine).To(HaveLen(1))
		Expect(mine[0].ID).To(Equal(idAlice))
		Expect(mine[0].AuthSubject).To(Equal("user_alice"))

		// And the unfiltered list still returns both users' sessions
		all, err := pgDriver.ListSessionRecords(ctx, orgID, "", 10, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(all).To(HaveLen(2))
	})

	It("does not match case-folded, trimmed, or prefix variants of the harness ids", func() {
		orgID := newTestOrgID()
		seedSession(orgID, "Claude-Code", "Sess-ABC-123", "variant matching")

		// Sanity anchor: the exact stored triple hits.
		rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, "Claude-Code", "Sess-ABC-123")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())

		variants := []struct {
			desc             string
			harnessID        string
			harnessSessionID string
		}{
			{"case-folded harness_id", "claude-code", "Sess-ABC-123"},
			{"case-folded harness_session_id", "Claude-Code", "sess-abc-123"},
			{"case-folded both", "CLAUDE-CODE", "SESS-ABC-123"},
			{"whitespace-padded harness_id", " Claude-Code ", "Sess-ABC-123"},
			{"whitespace-padded harness_session_id", "Claude-Code", " Sess-ABC-123 "},
			{"prefix of harness_id", "Claude", "Sess-ABC-123"},
			{"prefix of harness_session_id", "Claude-Code", "Sess-ABC"},
		}
		for _, v := range variants {
			rec, err := pgDriver.GetSessionRecordByHarness(ctx, orgID, v.harnessID, v.harnessSessionID)
			Expect(err).NotTo(HaveOccurred(), v.desc)
			Expect(rec).To(BeNil(), "%s must not match the stored natural key", v.desc)
		}
	})
})

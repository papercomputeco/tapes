package postgres_test

import (
	"context"

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

	// seed ingests a 2-node turn under the given identity and returns the
	// tapes-minted session UUID. A non-nil parentHarnessSessionID forks the
	// new session off that parent (shared harness_id).
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
		return res.SessionID
	}

	nodeCount := func(sessionID string) int {
		var n int
		err := pgDriver.DB().QueryRow(ctx,
			"SELECT count(*) FROM nodes WHERE session_id = $1::uuid", sessionID).Scan(&n)
		Expect(err).NotTo(HaveOccurred())
		return n
	}

	It("deletes the session and reports it was removed", func() {
		orgID := newTestOrgID()
		id := seed(orgID, "sess-solo", "solo turn", nil)
		Expect(nodeCount(id)).To(BeNumerically(">", 0), "precondition: the session owns nodes")

		deleted, err := pgDriver.DeleteSession(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())

		rec, err := pgDriver.GetSessionRecord(ctx, orgID, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil(), "the session row is gone")
		Expect(nodeCount(id)).To(BeZero(), "the session's nodes cascade with it")
	})

	It("cascades to subagent child sessions and their nodes", func() {
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
		Expect(nodeCount(parentID)).To(BeZero())
		Expect(nodeCount(childID)).To(BeZero())
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
		Expect(nodeCount(survivorID)).To(BeNumerically(">", 0))
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

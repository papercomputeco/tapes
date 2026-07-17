package postgres_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// ptr is a small helper to take the address of a string literal inline in
// table/spec bodies (Name *string arguments to UpdateSessionName).
func ptr(s string) *string { return &s }

var _ = Describe("Driver.UpdateSessionName", func() {
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

	// seedSession ingests a 2-node turn for the given org, returning the
	// tapes-minted session UUID. Mirrors the sibling read/ingest suites'
	// seeding helper.
	seedSession := func(orgID, harnessSessionID, text string) string {
		res, err := ingester.IngestTurn(ctx, storage.IngestTurnRequest{
			Session: &sessions.IngestEnvelope{
				OrgID:            orgID,
				AuthSubject:      "subject-update",
				HarnessID:        "claude",
				HarnessSessionID: harnessSessionID,
			},
			Nodes: sessionFixture(text),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.SessionID).NotTo(BeEmpty())
		return res.SessionID
	}

	// setDerivedTitle seeds a derived_title directly since it's populated by
	// the title-gen/derive pipeline, not by IngestTurn.
	setDerivedTitle := func(id, title string) {
		_, err := pgDriver.DB().Exec(ctx,
			"UPDATE sessions SET derived_title = $1 WHERE id = $2", title, mustUUID(id))
		Expect(err).NotTo(HaveOccurred())
	}

	It("updates name and returns rowsAffected=1 when the org matches the session (CC-2)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-org-match", "original text")

		rows, err := pgDriver.UpdateSessionName(ctx, orgA, id, ptr("My corrected title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		rec, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.Name).To(Equal("My corrected title"))
	})

	It("does not update the row and returns rowsAffected=0 for a cross-org id (CC-2, EST-7)", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()
		id := seedSession(orgA, "harness-cross-org", "orgA original text")

		rows, err := pgDriver.UpdateSessionName(ctx, orgB, id, ptr("hijacked title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(0)))

		// The row is untouched: reading it back under the true owning org
		// must not reflect orgB's attempted write.
		rec, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.Name).NotTo(Equal("hijacked title"))

		// And orgB can't even read the row (org-scoped read), reinforcing
		// that the update predicate matches the read predicate.
		recForOrgB, err := pgDriver.GetSessionRecord(ctx, orgB, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(recForOrgB).To(BeNil())
	})

	It("returns rowsAffected=0 for an unknown id (EST-7 not-found path)", func() {
		orgA := newTestOrgID()
		// A syntactically valid but never-seeded UUID.
		unknownID := "00000000-0000-0000-0000-000000000000"

		rows, err := pgDriver.UpdateSessionName(ctx, orgA, unknownID, ptr("does not matter"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(0)))
	})

	It("clears name to NULL when passed nil, leaving derived_title untouched (EST-4)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-clear", "clear me")
		setDerivedTitle(id, "auto-generated title")

		// Give the row a manual name first so there's something to clear.
		rows, err := pgDriver.UpdateSessionName(ctx, orgA, id, ptr("manual title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		rec, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Name).To(Equal("manual title"))

		// Now clear it: name=nil must NULL the column. GetSessionRecord's
		// display-precedence falls back to derived_title when name is
		// NULL, so assert against the raw column via SQL to isolate the
		// storage-layer contract from the read-path's display fallback.
		rows, err = pgDriver.UpdateSessionName(ctx, orgA, id, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		var namePg *string
		var derivedTitlePg *string
		Expect(pgDriver.DB().QueryRow(ctx,
			"SELECT name, derived_title FROM sessions WHERE id = $1", mustUUID(id),
		).Scan(&namePg, &derivedTitlePg)).To(Succeed())
		Expect(namePg).To(BeNil(), "name column must be NULL after clearing")
		Expect(derivedTitlePg).NotTo(BeNil())
		Expect(*derivedTitlePg).To(Equal("auto-generated title"), "clearing name must never touch derived_title")
	})

	It("changes only name and leaves derived_title (and other fields) unchanged (CC-1, EST-5)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-only-name", "only name changes")
		setDerivedTitle(id, "unchanged derived title")

		before, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(before).NotTo(BeNil())

		rows, err := pgDriver.UpdateSessionName(ctx, orgA, id, ptr("brand new manual name"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		// Raw column check: derived_title must be byte-for-byte the same
		// row as before the update (CC-1: never write derived_title).
		var derivedTitlePg *string
		Expect(pgDriver.DB().QueryRow(ctx,
			"SELECT derived_title FROM sessions WHERE id = $1", mustUUID(id),
		).Scan(&derivedTitlePg)).To(Succeed())
		Expect(derivedTitlePg).NotTo(BeNil())
		Expect(*derivedTitlePg).To(Equal("unchanged derived title"))

		after, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(after).NotTo(BeNil())

		// Other session fields untouched by the name-only mutation.
		Expect(after.HarnessID).To(Equal(before.HarnessID))
		Expect(after.HarnessSessionID).To(Equal(before.HarnessSessionID))
		Expect(after.TurnCount).To(Equal(before.TurnCount))
		Expect(after.TotalInputTokens).To(Equal(before.TotalInputTokens))
		Expect(after.TotalOutputTokens).To(Equal(before.TotalOutputTokens))
		Expect(after.AuthSubject).To(Equal(before.AuthSubject))
		Expect(after.StartedAt).To(Equal(before.StartedAt))
	})

	It("a user-set name takes display precedence over an existing derived_title (EST-2, CC-4 carve-out)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-precedence-carveout", "precedence carve-out text")
		setDerivedTitle(id, "auto-generated title")

		// Sanity: with only a derived_title and no user name, the read path
		// must fall back to it.
		before, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(before).NotTo(BeNil())
		Expect(before.Name).To(Equal("auto-generated title"))

		rows, err := pgDriver.UpdateSessionName(ctx, orgA, id, ptr("user chosen title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		after, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(after).NotTo(BeNil())
		Expect(after.Name).To(Equal("user chosen title"),
			"a non-empty user name must win over derived_title in the display precedence")
	})
})

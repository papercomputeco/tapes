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
// table/spec bodies (name *string arguments to UpdateSessionDisplayName).
func ptr(s string) *string { return &s }

var _ = Describe("Driver.UpdateSessionDisplayName", func() {
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
	// seeding helper. The envelope carries no Name, so the sessions.name
	// column is NULL — the user title lives only in display_name.
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

	It("sets display_name and returns rowsAffected=1 when the org matches the session (CC-2)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-org-match", "original text")

		rows, err := pgDriver.UpdateSessionDisplayName(ctx, orgA, id, ptr("My corrected title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		rec, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.DisplayName).To(Equal("My corrected title"))
	})

	It("does not update the row and returns rowsAffected=0 for a cross-org id (CC-2, EST-7)", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()
		id := seedSession(orgA, "harness-cross-org", "orgA original text")

		rows, err := pgDriver.UpdateSessionDisplayName(ctx, orgB, id, ptr("hijacked title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(0)))

		// The row is untouched: reading it back under the true owning org
		// must not reflect orgB's attempted write.
		rec, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.DisplayName).NotTo(Equal("hijacked title"))

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

		rows, err := pgDriver.UpdateSessionDisplayName(ctx, orgA, unknownID, ptr("does not matter"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(0)))
	})

	It("clears display_name to NULL when passed nil, leaving derived_title untouched (EST-4)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-clear", "clear me")
		setDerivedTitle(id, "auto-generated title")

		// Give the row a user title first so there's something to clear.
		rows, err := pgDriver.UpdateSessionDisplayName(ctx, orgA, id, ptr("manual title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		rec, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.DisplayName).To(Equal("manual title"))

		// Now clear it: name=nil must NULL the display_name column. Assert
		// against the raw columns via SQL to isolate the storage-layer
		// contract from the read-path's display-title resolution.
		rows, err = pgDriver.UpdateSessionDisplayName(ctx, orgA, id, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		var displayNamePg *string
		var derivedTitlePg *string
		Expect(pgDriver.DB().QueryRow(ctx,
			"SELECT display_name, derived_title FROM sessions WHERE id = $1", mustUUID(id),
		).Scan(&displayNamePg, &derivedTitlePg)).To(Succeed())
		Expect(displayNamePg).To(BeNil(), "display_name column must be NULL after clearing")
		Expect(derivedTitlePg).NotTo(BeNil())
		Expect(*derivedTitlePg).To(Equal("auto-generated title"), "clearing display_name must never touch derived_title")
	})

	It("changes only display_name and leaves name, derived_title (and other fields) unchanged (CC-1, EST-5)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-only-name", "only name changes")
		setDerivedTitle(id, "unchanged derived title")

		before, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(before).NotTo(BeNil())

		rows, err := pgDriver.UpdateSessionDisplayName(ctx, orgA, id, ptr("brand new manual name"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		// Raw column check: neither the harness name nor derived_title is
		// touched by a display_name write (CC-1: never write derived_title,
		// and the durable-title fix must not disturb the harness slug).
		var namePg *string
		var derivedTitlePg *string
		Expect(pgDriver.DB().QueryRow(ctx,
			"SELECT name, derived_title FROM sessions WHERE id = $1", mustUUID(id),
		).Scan(&namePg, &derivedTitlePg)).To(Succeed())
		Expect(namePg).To(BeNil(), "harness name column stays NULL (envelope carried no name)")
		Expect(derivedTitlePg).NotTo(BeNil())
		Expect(*derivedTitlePg).To(Equal("unchanged derived title"))

		after, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(after).NotTo(BeNil())
		Expect(after.DisplayName).To(Equal("brand new manual name"))

		// Other session fields untouched by the display_name-only mutation.
		Expect(after.HarnessID).To(Equal(before.HarnessID))
		Expect(after.HarnessSessionID).To(Equal(before.HarnessSessionID))
		Expect(after.TurnCount).To(Equal(before.TurnCount))
		Expect(after.TotalInputTokens).To(Equal(before.TotalInputTokens))
		Expect(after.TotalOutputTokens).To(Equal(before.TotalOutputTokens))
		Expect(after.AuthSubject).To(Equal(before.AuthSubject))
		Expect(after.StartedAt).To(Equal(before.StartedAt))
	})

	It("stores display_name independently of name and derived_title (PCC-970)", func() {
		orgA := newTestOrgID()
		id := seedSession(orgA, "harness-independent-columns", "independent columns text")
		setDerivedTitle(id, "auto-generated title")

		// Sanity: with no user title and a NULL name column, the read
		// record's Name coalesces to derived_title, and DisplayName is empty.
		before, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(before).NotTo(BeNil())
		Expect(before.Name).To(Equal("auto-generated title"))
		Expect(before.DisplayName).To(BeEmpty())
		// DerivedTitle is exposed raw and never inherits the name fallback.
		Expect(before.DerivedTitle).To(Equal("auto-generated title"))

		rows, err := pgDriver.UpdateSessionDisplayName(ctx, orgA, id, ptr("user chosen title"))
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(Equal(int64(1)))

		after, err := pgDriver.GetSessionRecord(ctx, orgA, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(after).NotTo(BeNil())
		// The user title lands in display_name — a separate axis. The API's
		// resolveDisplayTitle prefers it; the storage columns stay distinct.
		Expect(after.DisplayName).To(Equal("user chosen title"),
			"the user title is stored in display_name")
		Expect(after.Name).To(Equal("auto-generated title"),
			"the harness/coalesced name column is untouched by a display_name write")
		Expect(after.DerivedTitle).To(Equal("auto-generated title"),
			"DerivedTitle stays the raw folded title, independent of the user title")
	})
})

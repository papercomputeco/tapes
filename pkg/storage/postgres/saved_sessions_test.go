package postgres_test

import (
	"context"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver saved sessions persistence", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
		ctx      context.Context
		orgID    string
	)

	// insertSession seeds a minimal sessions row the FK can point at.
	insertSession := func(id string) {
		// $1 is passed twice (id, then harness_session_id): reusing the
		// literal "$1" placeholder for both a UUID and a TEXT column trips
		// Postgres's extended-protocol type inference ("inconsistent types
		// deduced for parameter $1", SQLSTATE 42P08), so each column gets
		// its own placeholder bound to the same Go value instead.
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id,
				harness_session_id, started_at, last_seen_at, harness_metadata)
			VALUES ($1, $2, 'user_owner', 'claude', $3, now(), now(), '{}')`,
			id, orgID, id)
		Expect(err).NotTo(HaveOccurred())
	}

	// insertSessionInOrg seeds a minimal sessions row owned by an explicit
	// org id, distinct from the outer orgID, so cross-org ownership checks
	// have a real row to point at.
	insertSessionInOrg := func(id, owningOrgID string) {
		_, err := pgDriver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id,
				harness_session_id, started_at, last_seen_at, harness_metadata)
			VALUES ($1, $2, 'user_owner', 'claude', $3, now(), now(), '{}')`,
			id, owningOrgID, id)
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		ctx = context.Background()
		orgID = uuid.NewString()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		var ok bool
		pgDriver, ok = driver.(*postgres.Driver)
		Expect(ok).To(BeTrue())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE saved_sessions")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	It("saves, lists, and unsaves an org-wide marker", func() {
		sid := uuid.NewString()
		insertSession(sid)

		rec, err := pgDriver.SaveSession(ctx, orgID, sid, "user_alice")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.SessionID).To(Equal(sid))
		Expect(rec.SavedBy).To(Equal("user_alice"))
		Expect(rec.SavedAt).NotTo(BeZero())

		list, err := pgDriver.ListSavedSessions(ctx, orgID)
		Expect(err).NotTo(HaveOccurred())
		Expect(list).To(HaveLen(1))
		Expect(list[0].SessionID).To(Equal(sid))

		deleted, err := pgDriver.UnsaveSession(ctx, orgID, sid)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())

		deleted, err = pgDriver.UnsaveSession(ctx, orgID, sid)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeFalse(), "second unsave is an idempotent no-op")
	})

	It("keeps the first saver's attribution on re-save", func() {
		sid := uuid.NewString()
		insertSession(sid)

		first, err := pgDriver.SaveSession(ctx, orgID, sid, "user_alice")
		Expect(err).NotTo(HaveOccurred())

		second, err := pgDriver.SaveSession(ctx, orgID, sid, "user_bob")
		Expect(err).NotTo(HaveOccurred())
		Expect(second.SavedBy).To(Equal("user_alice"), "first saver wins")
		Expect(second.SavedAt).To(Equal(first.SavedAt))
	})

	It("returns nil for a session that does not exist in the org", func() {
		rec, err := pgDriver.SaveSession(ctx, orgID, uuid.NewString(), "user_alice")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil())

		rec, err = pgDriver.SaveSession(ctx, orgID, "not-a-uuid", "user_alice")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil())
	})

	It("returns nil for a session that exists but belongs to a different org", func() {
		otherOrgID := uuid.NewString()
		sid := uuid.NewString()
		insertSessionInOrg(sid, otherOrgID)

		rec, err := pgDriver.SaveSession(ctx, orgID, sid, "user_alice")
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).To(BeNil())

		list, err := pgDriver.ListSavedSessions(ctx, orgID)
		Expect(err).NotTo(HaveOccurred())
		Expect(list).To(BeEmpty())
	})

	It("scopes markers to the org", func() {
		sid := uuid.NewString()
		insertSession(sid)
		_, err := pgDriver.SaveSession(ctx, orgID, sid, "user_alice")
		Expect(err).NotTo(HaveOccurred())

		otherOrg, err := pgDriver.ListSavedSessions(ctx, uuid.NewString())
		Expect(err).NotTo(HaveOccurred())
		Expect(otherOrg).To(BeEmpty())
	})
})

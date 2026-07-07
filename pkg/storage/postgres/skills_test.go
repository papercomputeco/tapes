package postgres_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver skills persistence", func() {
	var (
		driver   storage.Driver
		pgDriver *postgres.Driver
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
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE skills")
		Expect(err).NotTo(HaveOccurred())
		_, err = pgDriver.DB().Exec(ctx, "TRUNCATE TABLE skill_versions")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// newRec mints a fresh opaque id per call — skills are keyed on id now, and
	// slug is a non-unique cosmetic label.
	newRec := func() storage.SkillRecord {
		now := time.Now().UTC().Truncate(time.Microsecond)
		return storage.SkillRecord{
			ID:                      uuid.NewString(),
			Slug:                    "debug-react-hooks",
			Name:                    "Debug React Hooks",
			Description:             "Diagnoses hook issues",
			Type:                    "workflow",
			Version:                 "0.1.0",
			Visibility:              "private",
			Tags:                    []string{"react", "debugging"},
			Content:                 "# body",
			IsAIGenerated:           true,
			GeneratedFromSessionIDs: []string{"sess-1", "sess-2"},
			AuthorSubject:           "user-123",
			CreatedAt:               now,
			UpdatedAt:               now,
		}
	}

	It("round-trips an upserted skill", func() {
		org := newTestOrgID()
		rec := newRec()
		saved, err := pgDriver.UpsertSkill(ctx, org, rec)
		Expect(err).NotTo(HaveOccurred())
		Expect(saved.ID).To(Equal(rec.ID))
		Expect(saved.Slug).To(Equal("debug-react-hooks"))

		got, err := pgDriver.GetSkill(ctx, org, rec.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got.Name).To(Equal("Debug React Hooks"))
		Expect(got.Tags).To(Equal([]string{"react", "debugging"}))
		Expect(got.GeneratedFromSessionIDs).To(Equal([]string{"sess-1", "sess-2"}))
		Expect(got.IsAIGenerated).To(BeTrue())
	})

	It("overwrites mutable fields on re-upsert and preserves created_at", func() {
		org := newTestOrgID()
		first := newRec()
		_, err := pgDriver.UpsertSkill(ctx, org, first)
		Expect(err).NotTo(HaveOccurred())

		second := first // same id -> update
		second.Description = "Updated description"
		second.UpdatedAt = first.UpdatedAt.Add(time.Hour)
		saved, err := pgDriver.UpsertSkill(ctx, org, second)
		Expect(err).NotTo(HaveOccurred())
		Expect(saved.Description).To(Equal("Updated description"))
		Expect(saved.CreatedAt).To(BeTemporally("~", first.CreatedAt, time.Second))
	})

	It("scopes reads to the org", func() {
		orgA := newTestOrgID()
		orgB := newTestOrgID()
		rec := newRec()
		_, err := pgDriver.UpsertSkill(ctx, orgA, rec)
		Expect(err).NotTo(HaveOccurred())

		got, err := pgDriver.GetSkill(ctx, orgB, rec.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil(), "org B must not see org A's skill")
	})

	It("returns nil for a missing id", func() {
		got, err := pgDriver.GetSkill(ctx, newTestOrgID(), uuid.NewString())
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil())
	})

	It("round-trips the author subject", func() {
		org := newTestOrgID()
		rec := newRec()
		_, err := pgDriver.UpsertSkill(ctx, org, rec)
		Expect(err).NotTo(HaveOccurred())
		got, err := pgDriver.GetSkill(ctx, org, rec.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.AuthorSubject).To(Equal("user-123"))
	})

	It("lists skills for the org", func() {
		org := newTestOrgID()
		_, err := pgDriver.UpsertSkill(ctx, org, newRec())
		Expect(err).NotTo(HaveOccurred())
		second := newRec()
		second.Slug = "another-skill"
		_, err = pgDriver.UpsertSkill(ctx, org, second)
		Expect(err).NotTo(HaveOccurred())

		list, err := pgDriver.ListSkills(ctx, org, storage.SkillListOpts{})
		Expect(err).NotTo(HaveOccurred())
		Expect(list).To(HaveLen(2))
		Expect(pgDriver.ListSkills(ctx, newTestOrgID(), storage.SkillListOpts{})).To(BeEmpty(), "scoped to the org")
	})

	It("filters the list by search query and author scope", func() {
		org := newTestOrgID()
		mine := newRec()
		mine.Name = "Profile API latency"
		mine.AuthorSubject = "user-me"
		_, err := pgDriver.UpsertSkill(ctx, org, mine)
		Expect(err).NotTo(HaveOccurred())
		theirs := newRec()
		theirs.Name = "Write release notes"
		theirs.AuthorSubject = "user-other"
		_, err = pgDriver.UpsertSkill(ctx, org, theirs)
		Expect(err).NotTo(HaveOccurred())

		byQuery, err := pgDriver.ListSkills(ctx, org, storage.SkillListOpts{Query: "latency"})
		Expect(err).NotTo(HaveOccurred())
		Expect(byQuery).To(HaveLen(1))
		Expect(byQuery[0].Name).To(Equal("Profile API latency"))

		mineOnly, err := pgDriver.ListSkills(ctx, org, storage.SkillListOpts{Author: "user-me"})
		Expect(err).NotTo(HaveOccurred())
		Expect(mineOnly).To(HaveLen(1))

		counts, err := pgDriver.CountSkills(ctx, org, "", "user-me")
		Expect(err).NotTo(HaveOccurred())
		Expect(counts.Total).To(Equal(int64(2)))
		Expect(counts.Mine).To(Equal(int64(1)))
	})

	It("orders the list by most-downloaded when asked", func() {
		org := newTestOrgID()
		// UpsertSkill deliberately never writes download_count (it's a real
		// usage signal, moved only by IncrementSkillDownloads), so downloads
		// must be bumped through that path. Setting rec.DownloadCount here
		// was silently dropped, leaving both rows at 0 — the downloads sort
		// then fell through to the random-UUID id tiebreak and this spec
		// passed on a coin flip.
		low := newRec()
		low.Name = "Low"
		_, err := pgDriver.UpsertSkill(ctx, org, low)
		Expect(err).NotTo(HaveOccurred())
		Expect(pgDriver.IncrementSkillDownloads(ctx, org, low.ID)).To(Succeed())
		high := newRec()
		high.Name = "High"
		_, err = pgDriver.UpsertSkill(ctx, org, high)
		Expect(err).NotTo(HaveOccurred())
		Expect(pgDriver.IncrementSkillDownloads(ctx, org, high.ID)).To(Succeed())
		Expect(pgDriver.IncrementSkillDownloads(ctx, org, high.ID)).To(Succeed())

		list, err := pgDriver.ListSkills(ctx, org, storage.SkillListOpts{Sort: storage.SkillSortDownloads})
		Expect(err).NotTo(HaveOccurred())
		Expect(list).To(HaveLen(2))
		Expect(list[0].Name).To(Equal("High"), "most-downloaded first")
		Expect(list[0].DownloadCount).To(Equal(int64(2)))
	})

	It("lists skills generated from a session", func() {
		org := newTestOrgID()
		match := newRec()
		match.GeneratedFromSessionIDs = []string{"sess-a", "sess-b"}
		_, err := pgDriver.UpsertSkill(ctx, org, match)
		Expect(err).NotTo(HaveOccurred())
		other := newRec()
		other.GeneratedFromSessionIDs = []string{"sess-c"}
		_, err = pgDriver.UpsertSkill(ctx, org, other)
		Expect(err).NotTo(HaveOccurred())

		got, err := pgDriver.ListSkillsBySession(ctx, org, "sess-b")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(1))
		Expect(got[0].ID).To(Equal(match.ID))
	})

	It("appends and lists immutable versions with a monotonic number", func() {
		org := newTestOrgID()
		rec := newRec()
		_, err := pgDriver.UpsertSkill(ctx, org, rec)
		Expect(err).NotTo(HaveOccurred())

		n, err := pgDriver.NextSkillVersionNumber(ctx, org, rec.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(1))

		now := time.Now().UTC().Truncate(time.Microsecond)
		v1, err := pgDriver.CreateSkillVersion(ctx, org, storage.SkillVersionRecord{
			SkillID: rec.ID, VersionNumber: 1, Semver: "0.1.0",
			Changelog: "first", Content: "# v1", AuthorSubject: "user-123", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(v1.VersionNumber).To(Equal(1))

		n, err = pgDriver.NextSkillVersionNumber(ctx, org, rec.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(2))

		_, err = pgDriver.CreateSkillVersion(ctx, org, storage.SkillVersionRecord{
			SkillID: rec.ID, VersionNumber: 2, Semver: "0.1.1",
			Changelog: "second", Content: "# v2", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		vers, err := pgDriver.ListSkillVersions(ctx, org, rec.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(vers).To(HaveLen(2))
		Expect(vers[0].VersionNumber).To(Equal(2), "newest first")
	})
})

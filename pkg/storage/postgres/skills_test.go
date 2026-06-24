package postgres_test

import (
	"context"
	"time"

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
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	newRec := func() storage.SkillRecord {
		now := time.Now().UTC().Truncate(time.Microsecond)
		return storage.SkillRecord{
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
			CreatedAt:               now,
			UpdatedAt:               now,
		}
	}

	It("round-trips an upserted skill", func() {
		org := newTestOrgID()
		saved, err := pgDriver.UpsertSkill(ctx, org, newRec())
		Expect(err).NotTo(HaveOccurred())
		Expect(saved.Slug).To(Equal("debug-react-hooks"))

		got, err := pgDriver.GetSkill(ctx, org, "debug-react-hooks")
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

		second := newRec()
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
		_, err := pgDriver.UpsertSkill(ctx, orgA, newRec())
		Expect(err).NotTo(HaveOccurred())

		got, err := pgDriver.GetSkill(ctx, orgB, "debug-react-hooks")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil(), "org B must not see org A's skill")
	})

	It("returns nil for a missing slug", func() {
		got, err := pgDriver.GetSkill(ctx, newTestOrgID(), "nope")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil())
	})
})

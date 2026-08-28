package postgres_test

// A filter claim is only as good as core's ability to actually read the
// published view it names. These specs pin the probe that decides that: one
// WHERE FALSE round trip, made with the driver's own role, that must fail
// for a missing view, a missing claim-declared column, and a revoked SELECT
// alike — the three ways a deployment-owned view can be broken underneath an
// admitted claim.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("published view probe [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
	)

	parseView := func(qualified string) storage.PublishedViewName {
		GinkgoHelper()
		view, err := storage.ParsePublishedViewName(qualified)
		Expect(err).NotTo(HaveOccurred())
		return view
	}
	parseColumn := func(name string) storage.PublishedColumnName {
		GinkgoHelper()
		column, err := storage.ParsePublishedColumnName(name)
		Expect(err).NotTo(HaveOccurred())
		return column
	}
	pgCode := func(err error) string {
		GinkgoHelper()
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue(), "the probe must surface the database's own verdict: %v", err)
		return pgErr.Code
	}

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		// The fixture view mirrors the published-view contract shape: the
		// two contract columns plus one claim-declarable value column.
		_, err = driver.DB().Exec(ctx, `DROP SCHEMA IF EXISTS probepub_v1 CASCADE`)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, `CREATE SCHEMA probepub_v1`)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, `
			CREATE VIEW probepub_v1.attachments AS
			SELECT 'session'::text AS primitive_type, ''::text AS primitive_id, ''::text AS value
			WHERE FALSE`)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, err := driver.DB().Exec(context.Background(), `DROP SCHEMA IF EXISTS probepub_v1 CASCADE`)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("marks a probe the store never answered as transient", func() {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()

		err := driver.ProbePublishedView(expired,
			parseView("probepub_v1.attachments"), parseColumn("value"))
		Expect(err).To(HaveOccurred())
		var transient *storage.TransientProbeError
		Expect(errors.As(err, &transient)).To(BeTrue(),
			"a deadline is a failure to ask, not the store's verdict — callers must not cache it")
	})

	It("accepts a view the driver's role can read with the claim-declared column", func() {
		Expect(driver.ProbePublishedView(ctx,
			parseView("probepub_v1.attachments"), parseColumn("value"))).To(Succeed())
	})

	It("rejects a view that does not exist", func() {
		err := driver.ProbePublishedView(ctx,
			parseView("probepub_v1.missing"), parseColumn("value"))
		Expect(err).To(HaveOccurred())
		Expect(pgCode(err)).To(Equal("42P01"), "undefined_table is the database's word for a missing view")
		var transient *storage.TransientProbeError
		Expect(errors.As(err, &transient)).To(BeFalse(),
			"the store answered: this is a definitive verdict, never a transient failure")
		Expect(err.Error()).To(ContainSubstring("probepub_v1.missing"),
			"the error must name the view an operator has to go create")
	})

	It("rejects a view missing the claim-declared value column", func() {
		err := driver.ProbePublishedView(ctx,
			parseView("probepub_v1.attachments"), parseColumn("absent"))
		Expect(err).To(HaveOccurred())
		Expect(pgCode(err)).To(Equal("42703"),
			"a view without the declared match column cannot serve the claim")
	})

	It("rejects a view the probing role cannot SELECT from", func() {
		// A second driver connects as a role that can reach the schema but
		// was never granted SELECT on the view — the shape of a deployment
		// that created the view but forgot the grant. The role needs just
		// enough on public for the driver's migration check to read the
		// already-applied version table.
		const restrictedRole = "tapes_probe_denied"
		dropRole := fmt.Sprintf(`
			DO $$ BEGIN
				IF EXISTS (SELECT FROM pg_roles WHERE rolname = '%[1]s') THEN
					EXECUTE 'DROP OWNED BY %[1]s';
					EXECUTE 'DROP ROLE %[1]s';
				END IF;
			END $$;`, restrictedRole)
		_, err := driver.DB().Exec(ctx, dropRole)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, fmt.Sprintf(
			`CREATE ROLE %s LOGIN PASSWORD 'denied'`, restrictedRole))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, err := driver.DB().Exec(context.Background(), dropRole)
			Expect(err).NotTo(HaveOccurred())
		})
		for _, grant := range []string{
			`GRANT USAGE ON SCHEMA public TO ` + restrictedRole,
			`GRANT SELECT ON public.schema_migrations TO ` + restrictedRole,
			`GRANT USAGE ON SCHEMA probepub_v1 TO ` + restrictedRole,
		} {
			_, err = driver.DB().Exec(ctx, grant)
			Expect(err).NotTo(HaveOccurred())
		}

		config, err := pgx.ParseConfig(testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		restrictedDSN := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=denied sslmode=disable",
			config.Host, config.Port, config.Database, restrictedRole)
		restricted, err := postgres.NewDriver(ctx, restrictedDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(restricted.Close)

		err = restricted.ProbePublishedView(ctx,
			parseView("probepub_v1.attachments"), parseColumn("value"))
		Expect(err).To(HaveOccurred())
		Expect(pgCode(err)).To(Equal("42501"),
			"insufficient_privilege: the view exists but this role cannot read it")
	})
})

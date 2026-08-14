package postgres_test

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("span stats aggregate [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
		orgID  pgtype.UUID
	)

	const orgKey = "00000000-0000-4000-8000-0000000057a7"

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE span_turns_20260615 CASCADE")
		Expect(err).NotTo(HaveOccurred())

		Expect(orgID.Scan(orgKey)).To(Succeed())
	})

	It("sums tool calls from the turn rollups, not the spans table", func() {
		// Two turns whose rollups carry tool counts, with no rows in
		// spans_20260615 at all. The aggregate must believe the rollup:
		// scanning the wide spans table per request is what made
		// /v1/stats slow (PCC-936).
		seq, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())

		a := postgres.NewSpanTurnParamsForTest(orgID, "trace-tools-a", pgtype.UUID{}, "first", seq, postgres.FidelityRaw)
		a.ToolCalls = 3
		Expect(driver.SpanTurnUpsertForTest(ctx, a)).To(Succeed())

		b := postgres.NewSpanTurnParamsForTest(orgID, "trace-tools-b", pgtype.UUID{}, "second", seq, postgres.FidelityRaw)
		b.ToolCalls = 2
		Expect(driver.SpanTurnUpsertForTest(ctx, b)).To(Succeed())

		stats, err := driver.AggregateSpanStats(ctx, orgKey, nil, nil, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.TurnCount).To(Equal(2))
		Expect(stats.ToolCalls).To(Equal(5))
	})
})

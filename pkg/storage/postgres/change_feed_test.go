package postgres_test

// The change-feed contract: content_hash tells a real change from an
// idempotent rewrite, derive_seq advances only on the former, and fidelity
// records whether a row can be re-derived from stored bytes.

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("fidelity rollup", func() {
	It("reports the worst tier present", func() {
		// A trace is only as re-derivable as its least-recoverable span: one
		// dropped payload means the trace cannot be faithfully re-derived,
		// however many of its siblings can.
		Expect(postgres.RollupFidelityForTest([]string{
			postgres.FidelityRaw, postgres.FidelityDegraded, postgres.FidelityRaw,
		})).To(Equal(postgres.FidelityDegraded))

		Expect(postgres.RollupFidelityForTest([]string{
			postgres.FidelityRaw, postgres.FidelityReduced,
		})).To(Equal(postgres.FidelityReduced))

		Expect(postgres.RollupFidelityForTest([]string{
			postgres.FidelityRaw, postgres.FidelityRaw,
		})).To(Equal(postgres.FidelityRaw))
	})

	It("stays unbacked when nothing is backed", func() {
		// No raw turns behind any span is not a gap — there were never wire
		// bytes to keep — so it must not read as degraded.
		Expect(postgres.RollupFidelityForTest(nil)).To(Equal(postgres.FidelityUnbacked))
		Expect(postgres.RollupFidelityForTest([]string{
			postgres.FidelityUnbacked, postgres.FidelityUnbacked,
		})).To(Equal(postgres.FidelityUnbacked))
	})

	It("lets a backed span outrank an unbacked sibling", func() {
		Expect(postgres.RollupFidelityForTest([]string{
			postgres.FidelityUnbacked, postgres.FidelityRaw,
		})).To(Equal(postgres.FidelityRaw))
	})
})

var _ = Describe("span fidelity resolution", func() {
	tiers := map[int64]string{
		7: postgres.FidelityRaw,
		9: postgres.FidelityDegraded,
	}

	It("maps a known raw turn to its tier", func() {
		Expect(postgres.SpanFidelityForTest(tiers, 7)).To(Equal(postgres.FidelityRaw))
		Expect(postgres.SpanFidelityForTest(tiers, 9)).To(Equal(postgres.FidelityDegraded))
	})

	It("treats no raw turn, and a vanished one, as unbacked", func() {
		Expect(postgres.SpanFidelityForTest(tiers, 0)).To(Equal(postgres.FidelityUnbacked))
		// A raw row pruned between derive and write is not evidence about the
		// bytes, so it must not be reported as reduced or degraded.
		Expect(postgres.SpanFidelityForTest(tiers, 404)).To(Equal(postgres.FidelityUnbacked))
	})
})

var _ = Describe("content hashing", func() {
	base := func() gensqlcSpanTurn {
		return gensqlcSpanTurn{prompt: "hello", fidelity: postgres.FidelityRaw}
	}

	It("is stable across identical content", func() {
		a := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, base().prompt, 1, base().fidelity)
		b := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, base().prompt, 999, base().fidelity)
		// derive_seq differs and must not participate: the hash is what
		// decides whether the sequence advances, so including it would make
		// the row change every pass by construction.
		Expect(postgres.SpanTurnContentHashForTest(a)).To(Equal(postgres.SpanTurnContentHashForTest(b)))
	})

	It("changes when content changes", func() {
		a := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, "hello", 1, postgres.FidelityRaw)
		b := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, "hello!", 1, postgres.FidelityRaw)
		Expect(postgres.SpanTurnContentHashForTest(a)).NotTo(Equal(postgres.SpanTurnContentHashForTest(b)))
	})

	It("changes when only fidelity changes", func() {
		// A row whose bytes went from stored to dropped has changed in the
		// only way a fidelity-tracking consumer cares about, even though
		// nothing else moved.
		a := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, "hello", 1, postgres.FidelityRaw)
		b := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, "hello", 1, postgres.FidelityDegraded)
		Expect(postgres.SpanTurnContentHashForTest(a)).NotTo(Equal(postgres.SpanTurnContentHashForTest(b)))
	})

	It("does not confuse a field-boundary shift for identical content", func() {
		// Length-prefixing is what stops ("ab","c") hashing like ("a","bc").
		// Without it a change feed reports "unchanged" for a real edit, which
		// is worse than having no feed at all.
		a := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, "ab", 1, "c")
		b := postgres.NewSpanTurnParamsForTest(pgtype.UUID{}, "t1", pgtype.UUID{}, "a", 1, "bc")
		Expect(postgres.SpanTurnContentHashForTest(a)).NotTo(Equal(postgres.SpanTurnContentHashForTest(b)))
	})
})

// gensqlcSpanTurn is a tiny local holder so the specs above read as content
// rather than as parameter plumbing.
type gensqlcSpanTurn struct {
	prompt   string
	fidelity string
}

var _ = Describe("derive_seq cursor [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
		orgID  pgtype.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE span_turns_20260615 CASCADE")
		Expect(err).NotTo(HaveOccurred())

		Expect(orgID.Scan("00000000-0000-4000-8000-00000000feed")).To(Succeed())
	})

	// readRow returns the stored cursor and hash for a trace.
	readRow := func(traceID string) (int64, string, string) {
		var seq int64
		var hash, fidelity string
		Expect(driver.DB().QueryRow(ctx,
			`SELECT derive_seq, content_hash, fidelity FROM span_turns_20260615
			 WHERE org_id = $1 AND trace_id = $2`, orgID, traceID,
		).Scan(&seq, &hash, &fidelity)).To(Succeed())
		return seq, hash, fidelity
	}

	It("holds the cursor steady when a re-derive rewrites identical content", func() {
		seq1, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.SpanTurnUpsertForTest(ctx,
			postgres.NewSpanTurnParamsForTest(orgID, "trace-steady", pgtype.UUID{}, "hello", seq1, postgres.FidelityRaw),
		)).To(Succeed())
		firstSeq, firstHash, _ := readRow("trace-steady")
		Expect(firstSeq).To(Equal(seq1))

		// A second pass over unchanged raw. Every row is rewritten in place;
		// none of them changed. A consumer polling the cursor must see
		// nothing, or it re-reads the whole session after every derive.
		seq2, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(seq2).To(BeNumerically(">", seq1))
		Expect(driver.SpanTurnUpsertForTest(ctx,
			postgres.NewSpanTurnParamsForTest(orgID, "trace-steady", pgtype.UUID{}, "hello", seq2, postgres.FidelityRaw),
		)).To(Succeed())

		secondSeq, secondHash, _ := readRow("trace-steady")
		Expect(secondHash).To(Equal(firstHash))
		Expect(secondSeq).To(Equal(firstSeq), "cursor must not advance on an idempotent rewrite")
	})

	It("advances the cursor when the content actually changes", func() {
		seq1, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.SpanTurnUpsertForTest(ctx,
			postgres.NewSpanTurnParamsForTest(orgID, "trace-moving", pgtype.UUID{}, "hello", seq1, postgres.FidelityRaw),
		)).To(Succeed())
		firstSeq, firstHash, _ := readRow("trace-moving")

		seq2, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.SpanTurnUpsertForTest(ctx,
			postgres.NewSpanTurnParamsForTest(orgID, "trace-moving", pgtype.UUID{}, "hello, world", seq2, postgres.FidelityRaw),
		)).To(Succeed())

		secondSeq, secondHash, _ := readRow("trace-moving")
		Expect(secondHash).NotTo(Equal(firstHash))
		Expect(secondSeq).To(Equal(seq2))
		Expect(secondSeq).To(BeNumerically(">", firstSeq))
	})

	It("advances the cursor when only the fidelity tier changes", func() {
		seq1, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.SpanTurnUpsertForTest(ctx,
			postgres.NewSpanTurnParamsForTest(orgID, "trace-fidelity", pgtype.UUID{}, "hello", seq1, postgres.FidelityRaw),
		)).To(Succeed())
		firstSeq, _, firstFidelity := readRow("trace-fidelity")
		Expect(firstFidelity).To(Equal(postgres.FidelityRaw))

		// The same projection, now backed by a turn whose bytes were dropped.
		seq2, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.SpanTurnUpsertForTest(ctx,
			postgres.NewSpanTurnParamsForTest(orgID, "trace-fidelity", pgtype.UUID{}, "hello", seq2, postgres.FidelityDegraded),
		)).To(Succeed())

		secondSeq, _, secondFidelity := readRow("trace-fidelity")
		Expect(secondFidelity).To(Equal(postgres.FidelityDegraded))
		Expect(secondSeq).To(BeNumerically(">", firstSeq),
			"a provenance change is a change a consumer must see")
	})

	It("hands out a strictly increasing cursor", func() {
		a, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		b, err := driver.NextDeriveSeqForTest(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(BeNumerically(">", a))
	})
})

var _ = Describe("change feed under concurrent derives [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
		orgID  pgtype.UUID
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE span_turns_20260615 CASCADE")
		Expect(err).NotTo(HaveOccurred())

		Expect(orgID.Scan("00000000-0000-4000-8000-0000000feed2")).To(Succeed())
	})

	// This is the failure the cursor's design exists to prevent, and it is
	// invisible to a single-writer test: two derive passes overlap, the one
	// that took the LOWER cursor value commits LAST, and a consumer that has
	// already checkpointed past it never sees it again.
	//
	// The rows are written with raw SQL rather than the upsert helper because
	// the point is to control transaction boundaries precisely — two open
	// transactions committing in a chosen order — which the helper, running on
	// the pool, cannot express.
	It("never lets a late-committing pass slip behind a consumer's cursor", func() {
		insert := `INSERT INTO span_turns_20260615
		             (org_id, trace_id, started_at, content_hash, derive_seq, fidelity)
		           VALUES ($1, $2, now(), $3, $4, '')`

		// Pass A opens first, so it takes the lower cursor value.
		txA, err := driver.DB().Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = txA.Rollback(ctx) }()

		var seqA int64
		Expect(txA.QueryRow(ctx, `SELECT pg_current_xact_id()::text::bigint`).Scan(&seqA)).To(Succeed())
		_, err = txA.Exec(ctx, insert, orgID, "trace-late-commit", "hash-a", seqA)
		Expect(err).NotTo(HaveOccurred())

		// Pass B opens second — higher cursor — and commits first.
		txB, err := driver.DB().Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = txB.Rollback(ctx) }()

		var seqB int64
		Expect(txB.QueryRow(ctx, `SELECT pg_current_xact_id()::text::bigint`).Scan(&seqB)).To(Succeed())
		Expect(seqB).To(BeNumerically(">", seqA), "B must hold the higher cursor for this to be the case under test")
		_, err = txB.Exec(ctx, insert, orgID, "trace-early-commit", "hash-b", seqB)
		Expect(err).NotTo(HaveOccurred())
		Expect(txB.Commit(ctx)).To(Succeed())

		// A is still open. An unbounded poll is where the data loss starts: it
		// sees B, and a consumer checkpointing on it advances past A's cursor
		// while A has not committed. Assert the naive read really does expose
		// that, so this test fails if the hazard it guards ever stops existing.
		var naive int64
		Expect(driver.DB().QueryRow(ctx,
			`SELECT count(*) FROM span_turns_20260615 WHERE org_id = $1 AND derive_seq > 0`,
			orgID,
		).Scan(&naive)).To(Succeed())
		Expect(naive).To(Equal(int64(1)), "an unbounded poll sees B mid-flight — the loss this bound prevents")

		// The shipped read withholds B instead, because a pass older than it is
		// still in flight.
		rows, err := driver.ListChangedSpanTurnsForTest(ctx, orgID, 0, 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(BeEmpty(), "B must be withheld while an older pass is still open")

		Expect(txA.Commit(ctx)).To(Succeed())

		// Both are now committed, and both are delivered — in cursor order,
		// with A first despite committing second.
		rows, err = driver.ListChangedSpanTurnsForTest(ctx, orgID, 0, 100)
		Expect(err).NotTo(HaveOccurred())
		traces := make([]string, 0, len(rows))
		for _, r := range rows {
			traces = append(traces, r.TraceID)
		}
		Expect(traces).To(Equal([]string{"trace-late-commit", "trace-early-commit"}),
			"both passes must be delivered, oldest cursor first")
	})
})

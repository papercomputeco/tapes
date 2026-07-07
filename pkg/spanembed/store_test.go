package spanembed_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

func testPostgresDSN() (string, error) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		return "", errors.New("TEST_POSTGRES_DSN is not set; run postgres integration tests via `dagger call test` so the Dagger Postgres service is available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("connect to test postgres: %w", err)
	}
	defer conn.Close(context.Background())

	if err := conn.Ping(ctx); err != nil {
		return "", fmt.Errorf("ping test postgres at TEST_POSTGRES_DSN: %w", err)
	}

	return dsn, nil
}

// Store integration specs run against the Dagger Postgres service.
// They use their own embedding table and a unique org per run, insert
// span rows with NULL session_id (so concurrent suites' TRUNCATE
// sessions CASCADE cannot reap them), and clean up everything they
// created.
var _ = Describe("Store", func() {
	const tableName = "span_embeddings_store_test"

	var (
		ctx    context.Context
		dsn    string
		pool   *pgxpool.Pool
		org    string
		traceA string
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		dsn, err = testPostgresDSN()
		Expect(err).NotTo(HaveOccurred())

		// Open the tapes driver once so migrations (spans_20260615, span_turns_20260615)
		// are applied, then keep a plain pool for the store under test.
		driver, err := postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Close()).To(Succeed())

		pool, err = pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		// EnsureSchema fail-fasts when pgvector is absent (production leaves
		// installation to database provisioning), so the suite provisions its
		// own test database. Without this the specs only pass when Ginkgo's
		// randomized order happens to run the chunked-layout spec (which
		// installs the extension inline) first — a fresh database otherwise
		// fails with "vector extension is not installed".
		_, err = pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
		Expect(err).NotTo(HaveOccurred())

		org = uuid.NewString()
		traceA = "trc_" + uuid.NewString()
	})

	AfterEach(func() {
		_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{tableName}.Sanitize())
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{tableName + "_failures"}.Sanitize())
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, "DELETE FROM spans_20260615 WHERE org_id = $1", org)
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, "DELETE FROM span_turns_20260615 WHERE org_id = $1", org)
		Expect(err).NotTo(HaveOccurred())
		pool.Close()
	})

	insertSpan := func(spanID, kind, callKind string, input, output string) {
		_, err := pool.Exec(ctx, `
			INSERT INTO span_turns_20260615 (org_id, trace_id, user_prompt, started_at)
			VALUES ($1, $2, 'find the turn', now())
			ON CONFLICT (org_id, trace_id) DO NOTHING
		`, org, traceA)
		Expect(err).NotTo(HaveOccurred())

		_, err = pool.Exec(ctx, `
			INSERT INTO spans_20260615 (org_id, trace_id, span_id, kind, call_kind, started_at, input, output)
			VALUES ($1, $2, $3, $4, $5, now(), NULLIF($6, '')::jsonb, NULLIF($7, '')::jsonb)
		`, org, traceA, spanID, kind, callKind, input, output)
		Expect(err).NotTo(HaveOccurred())
	}

	newStore := func(dims uint) *spanembed.Store {
		store, err := spanembed.NewStore(pool, spanembed.StoreConfig{
			TableName:  tableName,
			Dimensions: dims,
			OrgID:      org,
		}, logger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return store
	}

	countRows := func(table, spanID string) int {
		var n int
		Expect(pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE span_id = $1`, pgx.Identifier{table}.Sanitize()),
			spanID,
		).Scan(&n)).To(Succeed())
		return n
	}

	Describe("EnsureSchema", func() {
		It("refuses to reuse a table whose dimensions differ from the configuration", func() {
			Expect(newStore(3).EnsureSchema(ctx)).To(Succeed())
			err := newStore(4).EnsureSchema(ctx)
			Expect(err).To(MatchError(ContainSubstring("stores vector(3) embeddings but 4 dimensions are configured")))
		})

		It("reuses an existing table when the dimensions match", func() {
			Expect(newStore(3).EnsureSchema(ctx)).To(Succeed())
			Expect(newStore(3).EnsureSchema(ctx)).To(Succeed())
		})
	})

	Describe("candidate selection and search round trip", func() {
		It("lists only main llm spans_20260615, upserts by identity, prunes orphans, and searches with turn context", func() {
			input := `[{"type":"text","text":"how do I configure retry backoff"}]`
			output := `[{"type":"text","text":"set max-poll-backoff"}]`
			insertSpan("llm_main", "llm", "main", input, output)
			insertSpan("llm_shadow", "llm", "offshoot:permission-check", input, output)
			insertSpan("tool_1", "tool", "", input, output)

			store := newStore(3)
			Expect(store.EnsureSchema(ctx)).To(Succeed())

			candidates, err := store.ListCandidates(ctx, spanembed.Key{}, 100)
			Expect(err).NotTo(HaveOccurred())
			Expect(candidates).To(HaveLen(1))
			Expect(candidates[0].SpanID).To(Equal("llm_main"))
			Expect(candidates[0].ExistingHash).To(BeEmpty())

			rec := spanembed.ChunkRecord{
				OrgID:       org,
				TraceID:     traceA,
				SpanID:      "llm_main",
				Model:       "test-model",
				ContentHash: spanembed.ContentHash("x"),
				Embeddings:  [][]float32{{1, 0, 0}},
			}
			Expect(store.UpsertSpanChunks(ctx, rec)).To(Succeed())
			// Idempotent by identity: a second write replaces in place.
			rec.Embeddings = [][]float32{{0, 1, 0}}
			Expect(store.UpsertSpanChunks(ctx, rec)).To(Succeed())

			candidates, err = store.ListCandidates(ctx, spanembed.Key{}, 100)
			Expect(err).NotTo(HaveOccurred())
			Expect(candidates).To(HaveLen(1))
			Expect(candidates[0].ExistingHash).To(Equal(rec.ContentHash))
			Expect(candidates[0].ExistingModel).To(Equal("test-model"))

			hits, err := store.Search(ctx, org, []float32{0, 1, 0}, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(hits).To(HaveLen(1))
			Expect(hits[0].TraceID).To(Equal(traceA))
			Expect(hits[0].SpanID).To(Equal("llm_main"))
			Expect(hits[0].UserPrompt).To(Equal("find the turn"))
			Expect(hits[0].Snippet).To(ContainSubstring("retry backoff"))
			Expect(hits[0].Score).To(BeNumerically("~", 1.0, 1e-4))

			// Pruning: drop the span, the embedding follows.
			_, err = pool.Exec(ctx, "DELETE FROM spans_20260615 WHERE org_id = $1 AND span_id = 'llm_main'", org)
			Expect(err).NotTo(HaveOccurred())
			pruned, err := store.PruneOrphans(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pruned).To(Equal(int64(1)))
		})
	})

	Describe("chunked layout migration", func() {
		It("adds chunk_idx and a chunked primary key to a pre-chunking table in place", func() {
			// Recreate the original one-row-per-span schema by hand, with a row in it.
			_, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
			Expect(err).NotTo(HaveOccurred())
			_, err = pool.Exec(ctx, fmt.Sprintf(`
				CREATE TABLE %s (
					org_id UUID NOT NULL, trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
					session_id UUID, model TEXT NOT NULL, content_hash TEXT NOT NULL,
					embedded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
					embedding vector(3) NOT NULL,
					PRIMARY KEY (org_id, trace_id, span_id)
				)`, pgx.Identifier{tableName}.Sanitize()))
			Expect(err).NotTo(HaveOccurred())
			_, err = pool.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (org_id, trace_id, span_id, model, content_hash, embedding)
				VALUES ($1, $2, 'llm_main', 'test-model', 'h', '[1,0,0]')`, pgx.Identifier{tableName}.Sanitize()),
				org, traceA)
			Expect(err).NotTo(HaveOccurred())

			Expect(newStore(3).EnsureSchema(ctx)).To(Succeed())

			// The pre-existing row becomes chunk 0.
			var chunkIdx int
			Expect(pool.QueryRow(ctx,
				fmt.Sprintf(`SELECT chunk_idx FROM %s WHERE span_id = 'llm_main'`, pgx.Identifier{tableName}.Sanitize()),
			).Scan(&chunkIdx)).To(Succeed())
			Expect(chunkIdx).To(BeZero())

			// The new primary key admits a second chunk for the same span.
			Expect(newStore(3).UpsertSpanChunks(ctx, spanembed.ChunkRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_main", Model: "test-model",
				ContentHash: spanembed.ContentHash("x"),
				Embeddings:  [][]float32{{1, 0, 0}, {0, 1, 0}},
			})).To(Succeed())
			Expect(countRows(tableName, "llm_main")).To(Equal(2))
		})
	})

	Describe("chunked writes and failure records", func() {
		BeforeEach(func() {
			Expect(newStore(3).EnsureSchema(ctx)).To(Succeed())
		})

		It("stores one row per chunk and prunes rows left by a shorter re-chunk", func() {
			store := newStore(3)
			rec := spanembed.ChunkRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_main", Model: "m", ContentHash: "h",
				Embeddings: [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}},
			}
			Expect(store.UpsertSpanChunks(ctx, rec)).To(Succeed())
			Expect(countRows(tableName, "llm_main")).To(Equal(3))

			rec.Embeddings = [][]float32{{1, 0, 0}, {0, 1, 0}}
			Expect(store.UpsertSpanChunks(ctx, rec)).To(Succeed())
			Expect(countRows(tableName, "llm_main")).To(Equal(2))
		})

		It("records a failure with accruing attempts and clears it on a later success", func() {
			store := newStore(3)
			fail := spanembed.FailureRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_big", Model: "m",
				ContentHash: "h", Reason: "oversize", TokenCount: 9523,
			}
			Expect(store.RecordFailure(ctx, fail)).To(Succeed())
			Expect(store.RecordFailure(ctx, fail)).To(Succeed())

			var attempts, tokens int
			Expect(pool.QueryRow(ctx,
				fmt.Sprintf(`SELECT attempts, token_count FROM %s WHERE span_id = 'llm_big'`, pgx.Identifier{tableName + "_failures"}.Sanitize()),
			).Scan(&attempts, &tokens)).To(Succeed())
			Expect(attempts).To(Equal(2))
			Expect(tokens).To(Equal(9523))

			Expect(store.UpsertSpanChunks(ctx, spanembed.ChunkRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_big", Model: "m",
				ContentHash: "h2", Embeddings: [][]float32{{1, 0, 0}},
			})).To(Succeed())
			Expect(countRows(tableName+"_failures", "llm_big")).To(BeZero())
		})

		It("returns one hit per span scored by the span's best-matching chunk", func() {
			insertSpan("llm_a", "llm", "main", `[{"type":"text","text":"alpha content"}]`, "")
			insertSpan("llm_b", "llm", "main", `[{"type":"text","text":"beta content"}]`, "")
			store := newStore(3)

			// Span A carries three chunks; one of them is the exact query vector.
			Expect(store.UpsertSpanChunks(ctx, spanembed.ChunkRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_a", Model: "m", ContentHash: "ha",
				Embeddings: [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}},
			})).To(Succeed())
			Expect(store.UpsertSpanChunks(ctx, spanembed.ChunkRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_b", Model: "m", ContentHash: "hb",
				Embeddings: [][]float32{{0.9, 0.1, 0}},
			})).To(Succeed())

			hits, err := store.Search(ctx, org, []float32{0, 1, 0}, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(hits).To(HaveLen(2)) // span A appears once despite three chunks
			Expect([]string{hits[0].SpanID, hits[1].SpanID}).To(ConsistOf("llm_a", "llm_b"))
			Expect(hits[0].SpanID).To(Equal("llm_a")) // best score first
			Expect(hits[0].Score).To(BeNumerically("~", 1.0, 1e-4))
		})

		It("prunes orphaned failure rows when the span no longer exists", func() {
			store := newStore(3)
			Expect(store.RecordFailure(ctx, spanembed.FailureRecord{
				OrgID: org, TraceID: traceA, SpanID: "llm_absent", Model: "m",
				ContentHash: "h", Reason: "oversize",
			})).To(Succeed())
			pruned, err := store.PruneOrphans(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pruned).To(Equal(int64(1)))
		})
	})

	Describe("reads before any embed pass", func() {
		It("returns ErrNotInitialized when the table does not exist", func() {
			store := newStore(3)
			_, err := store.Search(ctx, org, []float32{1, 0, 0}, 5)
			Expect(err).To(MatchError(spanembed.ErrNotInitialized))
		})

		It("prunes nothing without erroring when the table does not exist", func() {
			pruned, err := newStore(3).PruneOrphans(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pruned).To(BeZero())
		})
	})
})

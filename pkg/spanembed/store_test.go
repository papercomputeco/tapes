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

		// Open the tapes driver once so migrations (spans, span_turns)
		// are applied, then keep a plain pool for the store under test.
		driver, err := postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Close()).To(Succeed())

		pool, err = pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		org = uuid.NewString()
		traceA = "trc_" + uuid.NewString()
	})

	AfterEach(func() {
		_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{tableName}.Sanitize())
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, "DELETE FROM spans WHERE org_id = $1", org)
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, "DELETE FROM span_turns WHERE org_id = $1", org)
		Expect(err).NotTo(HaveOccurred())
		pool.Close()
	})

	insertSpan := func(spanID, kind, callKind string, input, output string) {
		_, err := pool.Exec(ctx, `
			INSERT INTO span_turns (org_id, trace_id, user_prompt, started_at)
			VALUES ($1, $2, 'find the turn', now())
			ON CONFLICT (org_id, trace_id) DO NOTHING
		`, org, traceA)
		Expect(err).NotTo(HaveOccurred())

		_, err = pool.Exec(ctx, `
			INSERT INTO spans (org_id, trace_id, span_id, kind, call_kind, started_at, input, output)
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
		It("lists only main llm spans, upserts by identity, prunes orphans, and searches with turn context", func() {
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

			rec := spanembed.Record{
				OrgID:       org,
				TraceID:     traceA,
				SpanID:      "llm_main",
				Model:       "test-model",
				ContentHash: spanembed.ContentHash("x"),
				Embedding:   []float32{1, 0, 0},
			}
			Expect(store.Upsert(ctx, rec)).To(Succeed())
			// Idempotent by identity: a second write replaces in place.
			rec.Embedding = []float32{0, 1, 0}
			Expect(store.Upsert(ctx, rec)).To(Succeed())

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
			_, err = pool.Exec(ctx, "DELETE FROM spans WHERE org_id = $1 AND span_id = 'llm_main'", org)
			Expect(err).NotTo(HaveOccurred())
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

package spanembed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvectorgo "github.com/pgvector/pgvector-go"
)

const (
	// DefaultTableName is the span-embedding table. It lives in the
	// same database as the spans projection — candidate selection and
	// search both join against spans/span_turns.
	DefaultTableName = "span_embeddings"

	// maxVectorDimensions is the upper bound pgvector supports for the
	// vector type.
	maxVectorDimensions = 16000
)

// ErrNotInitialized is returned by reads when the embedding table does
// not exist yet — i.e. no embed pass has ever run against this store.
var ErrNotInitialized = errors.New("span embeddings not initialized: run the embed pass (tapes dev embed-spans or tapes serve derive-worker --embed-spans)")

// StoreConfig configures a span-embedding store.
type StoreConfig struct {
	// TableName defaults to DefaultTableName.
	TableName string

	// Dimensions is the embedding dimensionality. The table's vector
	// column is created with exactly this size and EnsureSchema
	// fail-fasts when an existing table disagrees — model and dims are
	// a deliberate, explicit pairing (e.g. text-embedding-3-large@1024
	// in cloud, embeddinggemma@768 on a local/dev deployment).
	Dimensions uint

	// OrgID optionally scopes candidate listing and orphan pruning to
	// one tenant. Empty embeds every org.
	OrgID string
}

// Store reads and writes span embeddings in the tapes Postgres
// database. The write path (EnsureSchema, Upsert, PruneOrphans)
// belongs to the derive-side embed pass; the read path (Search) backs
// the API's span search.
type Store struct {
	pool   *pgxpool.Pool
	table  pgx.Identifier
	dims   uint
	orgID  pgtype.UUID
	scoped bool
	logger *slog.Logger
}

// NewStore wraps an existing connection pool. It performs no IO:
// writers must call EnsureSchema before upserting, readers may query
// immediately and receive ErrNotInitialized until a writer has run.
func NewStore(pool *pgxpool.Pool, cfg StoreConfig, log *slog.Logger) (*Store, error) {
	if pool == nil {
		return nil, errors.New("span embedding store requires a connection pool")
	}
	if cfg.Dimensions == 0 {
		return nil, errors.New("span embedding dimensions cannot be 0, must be configured")
	}
	if cfg.Dimensions > maxVectorDimensions {
		return nil, fmt.Errorf("span embedding dimensions cannot exceed %d", maxVectorDimensions)
	}
	table := cfg.TableName
	if table == "" {
		table = DefaultTableName
	}
	s := &Store{
		pool:   pool,
		table:  pgx.Identifier{table},
		dims:   cfg.Dimensions,
		logger: log,
	}
	if cfg.OrgID != "" {
		parsed, err := uuid.Parse(cfg.OrgID)
		if err != nil {
			return nil, fmt.Errorf("parse org id: %w", err)
		}
		s.orgID = pgtype.UUID{Bytes: parsed, Valid: true}
		s.scoped = true
	}
	return s, nil
}

// EnsureSchema creates the embedding table and its HNSW index sized to
// the configured dimensions, or fail-fasts when an existing table was
// created with different dimensions. pgvector cannot resize a column
// in place, so a dims change requires re-embedding into a new (or
// dropped) table — the error says so instead of letting every
// subsequent insert fail.
func (s *Store) EnsureSchema(ctx context.Context) error {
	table := s.table.Sanitize()

	// The pgvector extension must be installed by database
	// provisioning; runtime connections commonly cannot create
	// extensions in managed Postgres.
	var hasVector bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')`).Scan(&hasVector); err != nil {
		return fmt.Errorf("checking vector extension: %w", err)
	}
	if !hasVector {
		return errors.New("vector extension is not installed in this database")
	}

	var existingDims int32
	err := s.pool.QueryRow(ctx, `
		SELECT a.atttypmod
		FROM pg_attribute a
		WHERE a.attrelid = to_regclass($1)
		  AND a.attname = 'embedding'
	`, table).Scan(&existingDims)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Table does not exist yet; create it below.
	case err != nil:
		return fmt.Errorf("checking existing embedding dimensions: %w", err)
	case existingDims != int32(s.dims): // #nosec G115 -- NewStore bounds dimensions to maxVectorDimensions
		return fmt.Errorf(
			"existing table %s stores vector(%d) embeddings but %d dimensions are configured; re-embed into a new table or drop the old one",
			table, existingDims, s.dims,
		)
	}

	// No FK onto spans: the projection's down-migrations and prunes
	// must never be blocked by the embedding sidecar table. Orphans
	// are reaped by PruneOrphans instead.
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			org_id       UUID NOT NULL,
			trace_id     TEXT NOT NULL,
			span_id      TEXT NOT NULL,
			session_id   UUID,
			model        TEXT NOT NULL,
			content_hash TEXT NOT NULL,
			embedded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			embedding    vector(%d) NOT NULL,
			PRIMARY KEY (org_id, trace_id, span_id)
		)
	`, table, s.dims)
	if _, err := s.pool.Exec(ctx, createTable); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	indexName := pgx.Identifier{s.table[0] + "_embedding_idx"}.Sanitize()
	createIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s
		ON %s
		USING hnsw (embedding vector_cosine_ops)
	`, indexName, table)
	if _, err := s.pool.Exec(ctx, createIndex); err != nil {
		return fmt.Errorf("creating index: %w", err)
	}

	s.logger.Info("span embedding schema ready",
		"table", table,
		"dimensions", s.dims,
	)
	return nil
}

// ListCandidates pages every main llm span (keyset-ordered) joined
// with its current embedding row. The caller decides per candidate
// whether an embed is due — content hashing happens in Go, so the
// query stays a cheap indexable join.
func (s *Store) ListCandidates(ctx context.Context, after Key, limit int) ([]Candidate, error) {
	table := s.table.Sanitize()
	query := fmt.Sprintf(`
		SELECT s.org_id, s.trace_id, s.span_id, s.session_id, s.input, s.output,
		       COALESCE(e.content_hash, ''), COALESCE(e.model, '')
		FROM spans s
		LEFT JOIN %s e
		  ON e.org_id = s.org_id AND e.trace_id = s.trace_id AND e.span_id = s.span_id
		WHERE s.kind = 'llm' AND s.call_kind = 'main'
		  AND (s.org_id, s.trace_id, s.span_id) > (@org, @trace, @span)
		  AND (NOT @scoped::boolean OR s.org_id = @scope_org)
		ORDER BY s.org_id, s.trace_id, s.span_id
		LIMIT @page
	`, table)

	afterOrg := pgtype.UUID{Valid: true} // nil UUID sorts first
	if after.OrgID != "" {
		parsed, err := uuid.Parse(after.OrgID)
		if err != nil {
			return nil, fmt.Errorf("parse cursor org id: %w", err)
		}
		afterOrg = pgtype.UUID{Bytes: parsed, Valid: true}
	}

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{
		"org":       afterOrg,
		"trace":     after.TraceID,
		"span":      after.SpanID,
		"scoped":    s.scoped,
		"scope_org": s.orgID,
		"page":      limit,
	})
	if err != nil {
		return nil, fmt.Errorf("listing embed candidates: %w", err)
	}
	defer rows.Close()

	var out []Candidate
	for rows.Next() {
		var (
			c         Candidate
			org       pgtype.UUID
			session   pgtype.UUID
			inp, outp []byte
			hash, mdl string
		)
		if err := rows.Scan(&org, &c.TraceID, &c.SpanID, &session, &inp, &outp, &hash, &mdl); err != nil {
			return nil, fmt.Errorf("scanning embed candidate: %w", err)
		}
		c.OrgID = uuid.UUID(org.Bytes).String()
		if session.Valid {
			c.SessionID = uuid.UUID(session.Bytes).String()
		}
		c.Input = inp
		c.Output = outp
		c.ExistingHash = hash
		c.ExistingModel = mdl
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating embed candidates: %w", err)
	}
	return out, nil
}

// Upsert writes one embedding keyed by span identity. Re-derives keep
// identities stable, so re-embedding overwrites in place.
func (s *Store) Upsert(ctx context.Context, rec Record) error {
	table := s.table.Sanitize()
	query := fmt.Sprintf(`
		INSERT INTO %s (org_id, trace_id, span_id, session_id, model, content_hash, embedding, embedded_at)
		VALUES (@org, @trace, @span, @session, @model, @hash, @embedding, now())
		ON CONFLICT (org_id, trace_id, span_id) DO UPDATE SET
			session_id   = EXCLUDED.session_id,
			model        = EXCLUDED.model,
			content_hash = EXCLUDED.content_hash,
			embedding    = EXCLUDED.embedding,
			embedded_at  = now()
	`, table)

	org, err := uuid.Parse(rec.OrgID)
	if err != nil {
		return fmt.Errorf("parse org id: %w", err)
	}
	session := pgtype.UUID{}
	if rec.SessionID != "" {
		parsed, err := uuid.Parse(rec.SessionID)
		if err != nil {
			return fmt.Errorf("parse session id: %w", err)
		}
		session = pgtype.UUID{Bytes: parsed, Valid: true}
	}

	if _, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"org":       pgtype.UUID{Bytes: org, Valid: true},
		"trace":     rec.TraceID,
		"span":      rec.SpanID,
		"session":   session,
		"model":     rec.Model,
		"hash":      rec.ContentHash,
		"embedding": pgvectorgo.NewVector(rec.Embedding),
	}); err != nil {
		return fmt.Errorf("upserting span embedding %s/%s: %w", rec.TraceID, rec.SpanID, err)
	}
	return nil
}

// PruneOrphans removes embeddings whose span no longer exists as a
// main llm span — pruned by a re-derive, or reclassified out of the
// embeddable set.
func (s *Store) PruneOrphans(ctx context.Context) (int64, error) {
	table := s.table.Sanitize()
	query := fmt.Sprintf(`
		DELETE FROM %s e
		WHERE (NOT @scoped::boolean OR e.org_id = @scope_org)
		  AND NOT EXISTS (
			SELECT 1 FROM spans s
			WHERE s.org_id = e.org_id AND s.trace_id = e.trace_id AND s.span_id = e.span_id
			  AND s.kind = 'llm' AND s.call_kind = 'main'
		)
	`, table)
	tag, err := s.pool.Exec(ctx, query, pgx.NamedArgs{
		"scoped":    s.scoped,
		"scope_org": s.orgID,
	})
	if err != nil {
		if isUndefinedTable(err) {
			return 0, nil // nothing embedded yet, nothing to prune
		}
		return 0, fmt.Errorf("pruning orphaned span embeddings: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Search returns the topK spans most similar to the query embedding,
// joined with their trace context (turn prompt and span payloads for
// the snippet). Scoped to one org — search is a tenant-facing read.
func (s *Store) Search(ctx context.Context, orgID string, embedding []float32, topK int) ([]Hit, error) {
	if topK <= 0 {
		topK = 5
	}
	org, err := uuid.Parse(orgID)
	if err != nil {
		return nil, fmt.Errorf("parse org id: %w", err)
	}

	table := s.table.Sanitize()
	query := fmt.Sprintf(`
		SELECT e.trace_id, e.span_id, e.session_id,
		       1 - (e.embedding <=> @embedding) AS score,
		       t.user_prompt, s.model, s.started_at, s.input, s.output
		FROM %s e
		JOIN spans s
		  ON s.org_id = e.org_id AND s.trace_id = e.trace_id AND s.span_id = e.span_id
		JOIN span_turns t
		  ON t.org_id = e.org_id AND t.trace_id = e.trace_id
		WHERE e.org_id = @org
		ORDER BY e.embedding <=> @embedding
		LIMIT @topk
	`, table)

	rows, err := s.pool.Query(ctx, query, pgx.NamedArgs{
		"org":       pgtype.UUID{Bytes: org, Valid: true},
		"embedding": pgvectorgo.NewVector(embedding),
		"topk":      topK,
	})
	if err != nil {
		if isUndefinedTable(err) {
			return nil, ErrNotInitialized
		}
		return nil, fmt.Errorf("querying span embeddings: %w", err)
	}
	defer rows.Close()

	var hits []Hit
	for rows.Next() {
		var (
			h         Hit
			session   pgtype.UUID
			startedAt pgtype.Timestamptz
			inp, outp []byte
		)
		if err := rows.Scan(&h.TraceID, &h.SpanID, &session, &h.Score, &h.UserPrompt, &h.Model, &startedAt, &inp, &outp); err != nil {
			return nil, fmt.Errorf("scanning span search hit: %w", err)
		}
		if session.Valid {
			h.SessionID = uuid.UUID(session.Bytes).String()
		}
		h.StartedAt = startedAt.Time
		h.Snippet = snippet(RenderSpanText(inp, outp))
		hits = append(hits, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating span search hits: %w", err)
	}

	s.logger.Debug("span embedding search", "hits", len(hits))
	return hits, nil
}

// snippetRunes bounds the matched-span preview a search hit carries.
const snippetRunes = 280

// snippet truncates rendered span text for the search hit payload.
func snippet(text string) string {
	runes := []rune(text)
	if len(runes) <= snippetRunes {
		return text
	}
	return string(runes[:snippetRunes]) + "…"
}

// isUndefinedTable reports whether err is Postgres undefined_table —
// the read-before-any-embed-pass case.
func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

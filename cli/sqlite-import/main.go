package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	_ "github.com/mattn/go-sqlite3"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvectorgo "github.com/pgvector/pgvector-go"

	"github.com/papercomputeco/tapes/pkg/config"
)

const (
	defaultSQLitePath  = "~/.tapes/tapes.sqlite"
	defaultPostgresDSN = "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable"
	defaultBatchSize   = 1000
)

type config struct {
	sqlitePath  string
	postgresDSN string
	batchSize   int
	dryRun      bool
}

type nodeRow struct {
	hash                     string
	bucket                   []byte
	typeName                 sql.NullString
	role                     sql.NullString
	content                  []byte
	model                    sql.NullString
	provider                 sql.NullString
	agentName                sql.NullString
	stopReason               sql.NullString
	promptTokens             sql.NullInt64
	completionTokens         sql.NullInt64
	totalTokens              sql.NullInt64
	cacheCreationInputTokens sql.NullInt64
	cacheReadInputTokens     sql.NullInt64
	totalDurationNS          sql.NullInt64
	promptDurationNS         sql.NullInt64
	project                  sql.NullString
	createdAtRaw             string
	parentHash               sql.NullString
}

type stats struct {
	total    int64
	inserted int64
	skipped  int64
}

type vectorStats struct {
	attempted     bool
	total         int64
	inserted      int64
	skipped       int64
	postgresCount int64
}

func main() {
	cfg := parseFlags()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.sqlitePath, "sqlite-path", defaultSQLitePath, "Path to the legacy tapes SQLite database")
	flag.StringVar(&cfg.postgresDSN, "postgres-dsn", defaultPostgresDSN, "PostgreSQL DSN for the local tapes instance")
	flag.IntVar(&cfg.batchSize, "batch-size", defaultBatchSize, "Number of rows to insert per PostgreSQL batch")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "Validate connectivity and schema without writing rows")
	flag.Parse()

	if cfg.batchSize <= 0 {
		log.Fatal("--batch-size must be > 0")
	}

	return cfg
}

func run(ctx context.Context, cfg config) error {
	sqlitePath, err := expandPath(cfg.sqlitePath)
	if err != nil {
		return fmt.Errorf("expand sqlite path: %w", err)
	}
	cfg.sqlitePath = sqlitePath

	log.Printf("source sqlite: %s", cfg.sqlitePath)
	log.Printf("target postgres: %s", config.RedactDSN(cfg.postgresDSN))

	sqlite_vec.Auto()

	sqliteDB, err := sql.Open("sqlite3", cfg.sqlitePath)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer sqliteDB.Close()

	if err := sqliteDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite: %w", err)
	}

	pgPool, err := pgxpool.New(ctx, cfg.postgresDSN)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pgPool.Close()

	if err := pgPool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	total, err := sqliteCount(ctx, sqliteDB, `SELECT COUNT(*) FROM nodes`)
	if err != nil {
		return fmt.Errorf("count sqlite nodes: %w", err)
	}
	if total == 0 {
		log.Printf("sqlite database is empty; nothing to import")
		return nil
	}

	dangling, err := sqliteCount(ctx, sqliteDB, `
		SELECT COUNT(*)
		FROM nodes n
		WHERE n.parent_hash IS NOT NULL
		  AND n.parent_hash != ''
		  AND NOT EXISTS (
			SELECT 1 FROM nodes p WHERE p.hash = n.parent_hash
		  )`)
	if err != nil {
		return fmt.Errorf("check dangling sqlite parents: %w", err)
	}

	importable, err := sqliteCount(ctx, sqliteDB, importableCountQuery)
	if err != nil {
		return fmt.Errorf("count importable sqlite nodes: %w", err)
	}
	skipped := total - importable

	log.Printf("sqlite nodes: total=%d importable=%d skipped=%d", total, importable, skipped)
	if dangling > 0 {
		log.Printf("warning: found %d rows with missing parent references in sqlite; orphaned rows/subtrees will be skipped", dangling)
	}
	if skipped > 0 {
		log.Printf("warning: skipping %d sqlite rows that are not reachable from a valid root", skipped)
	}

	if cfg.dryRun {
		log.Printf("node dry run successful")
		_, err := importVectors(ctx, sqliteDB, pgPool, cfg.batchSize, true)
		if err != nil {
			return err
		}
		log.Printf("dry run successful")
		return nil
	}

	rows, err := sqliteDB.QueryContext(ctx, orderedNodesQuery)
	if err != nil {
		return fmt.Errorf("query sqlite nodes: %w", err)
	}
	defer rows.Close()

	st, err := importNodes(ctx, pgPool, rows, importable, cfg)
	if err != nil {
		return err
	}

	postgresCount, err := postgresCount(ctx, pgPool, `SELECT COUNT(*) FROM nodes`)
	if err != nil {
		return fmt.Errorf("count postgres nodes: %w", err)
	}

	log.Printf("node import complete: total=%d inserted=%d skipped=%d postgres_nodes=%d", st.total, st.inserted, st.skipped, postgresCount)

	vectorStats, err := importVectors(ctx, sqliteDB, pgPool, cfg.batchSize, cfg.dryRun)
	if err != nil {
		return err
	}
	if vectorStats.attempted {
		log.Printf("vector import complete: total=%d inserted=%d skipped=%d postgres_vectors=%d", vectorStats.total, vectorStats.inserted, vectorStats.skipped, vectorStats.postgresCount)
	}

	return nil
}

const importableCountQuery = `
WITH RECURSIVE ordered AS (
	SELECT hash, 0 AS depth
	FROM nodes
	WHERE parent_hash IS NULL OR parent_hash = ''

	UNION ALL

	SELECT child.hash, ordered.depth + 1
	FROM nodes child
	JOIN ordered ON child.parent_hash = ordered.hash
)
SELECT COUNT(*) FROM ordered
`

const orderedNodesQuery = `
WITH RECURSIVE ordered AS (
	SELECT hash, 0 AS depth
	FROM nodes
	WHERE parent_hash IS NULL OR parent_hash = ''

	UNION ALL

	SELECT child.hash, ordered.depth + 1
	FROM nodes child
	JOIN ordered ON child.parent_hash = ordered.hash
)
SELECT
	n.hash,
	n.bucket,
	n.type,
	n.role,
	n.content,
	n.model,
	n.provider,
	n.agent_name,
	n.stop_reason,
	n.prompt_tokens,
	n.completion_tokens,
	n.total_tokens,
	n.cache_creation_input_tokens,
	n.cache_read_input_tokens,
	n.total_duration_ns,
	n.prompt_duration_ns,
	n.project,
	CAST(n.created_at AS TEXT) AS created_at,
	n.parent_hash
FROM nodes n
JOIN ordered o ON o.hash = n.hash
ORDER BY o.depth ASC, n.created_at ASC, n.hash ASC
`

func importNodes(ctx context.Context, pool *pgxpool.Pool, rows *sql.Rows, total int64, cfg config) (stats, error) {
	var (
		st        stats
		batch     []nodeRow
		processed int64
		startedAt = time.Now()
	)
	st.total = total

	for rows.Next() {
		row, err := scanNodeRow(rows)
		if err != nil {
			return st, fmt.Errorf("scan sqlite row: %w", err)
		}
		batch = append(batch, row)
		processed++

		if len(batch) >= cfg.batchSize {
			inserted, skipped, err := flushBatch(ctx, pool, batch)
			if err != nil {
				return st, err
			}
			st.inserted += inserted
			st.skipped += skipped
			log.Printf("progress: processed=%d/%d inserted=%d skipped=%d elapsed=%s", processed, total, st.inserted, st.skipped, time.Since(startedAt).Round(time.Second))
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("iterate sqlite rows: %w", err)
	}

	if len(batch) > 0 {
		inserted, skipped, err := flushBatch(ctx, pool, batch)
		if err != nil {
			return st, err
		}
		st.inserted += inserted
		st.skipped += skipped
		log.Printf("progress: processed=%d/%d inserted=%d skipped=%d elapsed=%s", processed, total, st.inserted, st.skipped, time.Since(startedAt).Round(time.Second))
	}

	if processed != total {
		return st, fmt.Errorf("ordered sqlite walk reached %d of %d importable nodes", processed, total)
	}

	return st, nil
}

func scanNodeRow(rows *sql.Rows) (nodeRow, error) {
	var row nodeRow
	if err := rows.Scan(
		&row.hash,
		&row.bucket,
		&row.typeName,
		&row.role,
		&row.content,
		&row.model,
		&row.provider,
		&row.agentName,
		&row.stopReason,
		&row.promptTokens,
		&row.completionTokens,
		&row.totalTokens,
		&row.cacheCreationInputTokens,
		&row.cacheReadInputTokens,
		&row.totalDurationNS,
		&row.promptDurationNS,
		&row.project,
		&row.createdAtRaw,
		&row.parentHash,
	); err != nil {
		return nodeRow{}, err
	}
	return row, nil
}

func flushBatch(ctx context.Context, pool *pgxpool.Pool, batch []nodeRow) (int64, int64, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("begin postgres tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	pgBatch := &pgx.Batch{}
	for _, row := range batch {
		args, err := nodeInsertArgs(row)
		if err != nil {
			return 0, 0, err
		}
		pgBatch.Queue(insertNodeSQL, args...)
	}

	results := tx.SendBatch(ctx, pgBatch)

	var inserted int64
	for range batch {
		ct, err := results.Exec()
		if err != nil {
			_ = results.Close()
			_ = tx.Rollback(ctx)
			log.Printf("warning: postgres batch insert failed; retrying %d rows individually: %v", len(batch), err)
			return flushBatchIndividually(ctx, pool, batch)
		}
		inserted += ct.RowsAffected()
	}
	if err := results.Close(); err != nil {
		return 0, 0, fmt.Errorf("close postgres batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("commit postgres tx: %w", err)
	}

	return inserted, int64(len(batch)) - inserted, nil
}

func flushBatchIndividually(ctx context.Context, pool *pgxpool.Pool, batch []nodeRow) (int64, int64, error) {
	var inserted int64
	var skipped int64

	for _, row := range batch {
		args, err := nodeInsertArgs(row)
		if err != nil {
			return 0, 0, err
		}

		ct, err := pool.Exec(ctx, insertNodeSQL, args...)
		if err != nil {
			reason, ok := classifySkippableInsertError(err)
			if !ok {
				return 0, 0, fmt.Errorf("insert postgres row %s: %w", row.hash, err)
			}
			log.Printf("warning: skipping row hash=%s parent=%s reason=%s", row.hash, printableParentHash(row.parentHash), reason)
			skipped++
			continue
		}

		inserted += ct.RowsAffected()
		if ct.RowsAffected() == 0 {
			skipped++
		}
	}

	return inserted, skipped, nil
}

func nodeInsertArgs(row nodeRow) ([]any, error) {
	createdAt, err := parseSQLiteTime(row.createdAtRaw)
	if err != nil {
		return nil, fmt.Errorf("parse created_at for %s: %w", row.hash, err)
	}

	return []any{
		row.hash,
		nilIfEmptyBytes(row.bucket),
		nullStringValue(row.typeName),
		nullStringValue(row.role),
		nilIfEmptyBytes(row.content),
		nullStringValue(row.model),
		nullStringValue(row.provider),
		nullStringValue(row.agentName),
		nullStringValue(row.stopReason),
		nullInt64Value(row.promptTokens),
		nullInt64Value(row.completionTokens),
		nullInt64Value(row.totalTokens),
		nullInt64Value(row.cacheCreationInputTokens),
		nullInt64Value(row.cacheReadInputTokens),
		nullInt64Value(row.totalDurationNS),
		nullInt64Value(row.promptDurationNS),
		nullStringValue(row.project),
		createdAt.UTC(),
		nullStringValue(normalizeParentHash(row.parentHash)),
	}, nil
}

func classifySkippableInsertError(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}

	switch pgErr.Code {
	case "22P05":
		return "postgres rejected JSON content (unsupported Unicode escape sequence)", true
	case "23503":
		return "parent row is missing (ancestor skipped or absent)", true
	default:
		return "", false
	}
}

func printableParentHash(parent sql.NullString) string {
	if !parent.Valid || strings.TrimSpace(parent.String) == "" {
		return "<root>"
	}
	return parent.String
}

const insertNodeSQL = `
INSERT INTO nodes (
	hash,
	bucket,
	type,
	role,
	content,
	model,
	provider,
	agent_name,
	stop_reason,
	prompt_tokens,
	completion_tokens,
	total_tokens,
	cache_creation_input_tokens,
	cache_read_input_tokens,
	total_duration_ns,
	prompt_duration_ns,
	project,
	created_at,
	parent_hash
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8, $9,
	$10, $11, $12, $13, $14, $15, $16, $17, $18, $19
)
ON CONFLICT (hash) DO NOTHING
`

const importVectorsQuery = `
SELECT d.doc_id, d.hash, e.embedding
FROM vec_embeddings e
INNER JOIN vec_documents d ON d.rowid = e.rowid
ORDER BY d.rowid ASC
`

func importVectors(ctx context.Context, sqliteDB *sql.DB, pgPool *pgxpool.Pool, batchSize int, dryRun bool) (vectorStats, error) {
	var st vectorStats

	hasDocs, err := sqliteTableExists(ctx, sqliteDB, "vec_documents")
	if err != nil {
		return st, fmt.Errorf("check sqlite vec_documents table: %w", err)
	}
	hasEmbeddings, err := sqliteTableExists(ctx, sqliteDB, "vec_embeddings")
	if err != nil {
		return st, fmt.Errorf("check sqlite vec_embeddings table: %w", err)
	}
	if !hasDocs || !hasEmbeddings {
		log.Printf("vector import skipped: sqlite vector tables not present")
		return st, nil
	}

	st.attempted = true

	var vecVersion string
	if err := sqliteDB.QueryRowContext(ctx, `SELECT vec_version()`).Scan(&vecVersion); err != nil {
		return st, fmt.Errorf("sqlite-vec not available while importing vectors: %w", err)
	}
	log.Printf("sqlite-vec detected: version=%s", vecVersion)

	vectorTotal, err := sqliteCount(ctx, sqliteDB, `SELECT COUNT(*) FROM vec_documents`)
	if err != nil {
		return st, fmt.Errorf("count sqlite vectors: %w", err)
	}
	if vectorTotal == 0 {
		log.Printf("vector import skipped: sqlite vector tables are empty")
		return st, nil
	}
	st.total = vectorTotal

	sourceDims, err := sqliteVectorDimensions(ctx, sqliteDB)
	if err != nil {
		return st, fmt.Errorf("detect sqlite vector dimensions: %w", err)
	}

	pgDims, err := postgresVectorDimensions(ctx, pgPool, "tapes_embeddings")
	if err != nil {
		return st, fmt.Errorf("detect postgres vector dimensions: %w", err)
	}
	if pgDims == 0 {
		log.Printf("warning: vector import skipped because PostgreSQL table %q does not exist", "tapes_embeddings")
		return st, nil
	}
	if pgDims != sourceDims {
		log.Printf("warning: vector import skipped because dimensions do not match: sqlite=%d postgres=%d", sourceDims, pgDims)
		return st, nil
	}

	log.Printf("sqlite vectors: total=%d dimensions=%d target_table=%s", vectorTotal, sourceDims, "tapes_embeddings")

	if dryRun {
		log.Printf("vector dry run successful")
		return st, nil
	}

	rows, err := sqliteDB.QueryContext(ctx, importVectorsQuery)
	if err != nil {
		return st, fmt.Errorf("query sqlite vectors: %w", err)
	}
	defer rows.Close()

	var (
		processed int64
		batchDocs []vectorDoc
		startedAt = time.Now()
	)

	for rows.Next() {
		var (
			id       string
			hash     string
			embBytes []byte
		)
		if err := rows.Scan(&id, &hash, &embBytes); err != nil {
			return st, fmt.Errorf("scan sqlite vector row: %w", err)
		}
		embedding, err := deserializeFloat32(embBytes)
		if err != nil {
			return st, fmt.Errorf("decode sqlite vector for %s: %w", id, err)
		}
		batchDocs = append(batchDocs, vectorDoc{ID: id, Hash: hash, Embedding: embedding})
		processed++

		if len(batchDocs) >= batchSize {
			inserted, skipped, err := flushVectorBatch(ctx, pgPool, "tapes_embeddings", batchDocs)
			if err != nil {
				return st, err
			}
			st.inserted += inserted
			st.skipped += skipped
			log.Printf("vector progress: processed=%d/%d inserted=%d skipped=%d elapsed=%s", processed, st.total, st.inserted, st.skipped, time.Since(startedAt).Round(time.Second))
			batchDocs = batchDocs[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("iterate sqlite vectors: %w", err)
	}

	if len(batchDocs) > 0 {
		inserted, skipped, err := flushVectorBatch(ctx, pgPool, "tapes_embeddings", batchDocs)
		if err != nil {
			return st, err
		}
		st.inserted += inserted
		st.skipped += skipped
		log.Printf("vector progress: processed=%d/%d inserted=%d skipped=%d elapsed=%s", processed, st.total, st.inserted, st.skipped, time.Since(startedAt).Round(time.Second))
	}

	pgCount, err := postgresCount(ctx, pgPool, `SELECT COUNT(*) FROM tapes_embeddings`)
	if err != nil {
		return st, fmt.Errorf("count postgres vectors: %w", err)
	}
	st.postgresCount = pgCount
	return st, nil
}

type vectorDoc struct {
	ID        string
	Hash      string
	Embedding []float32
}

func flushVectorBatch(ctx context.Context, pool *pgxpool.Pool, table string, docs []vectorDoc) (int64, int64, error) {
	query := fmt.Sprintf(`
INSERT INTO %s (id, hash, embedding)
VALUES (@id, @hash, @embedding)
ON CONFLICT (id) DO UPDATE SET
	hash = EXCLUDED.hash,
	embedding = EXCLUDED.embedding
`, table)

	batch := &pgx.Batch{}
	for _, doc := range docs {
		batch.Queue(query, pgx.NamedArgs{
			"id":        doc.ID,
			"hash":      doc.Hash,
			"embedding": pgvectorgo.NewVector(doc.Embedding),
		})
	}

	results := pool.SendBatch(ctx, batch)
	defer results.Close()

	var inserted int64
	for range docs {
		ct, err := results.Exec()
		if err != nil {
			return 0, 0, fmt.Errorf("upsert postgres vector batch: %w", err)
		}
		inserted += ct.RowsAffected()
	}

	return inserted, 0, nil
}

func sqliteTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, table).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func sqliteVectorDimensions(ctx context.Context, db *sql.DB) (int, error) {
	var embBytes []byte
	if err := db.QueryRowContext(ctx, `SELECT embedding FROM vec_embeddings LIMIT 1`).Scan(&embBytes); err != nil {
		return 0, err
	}
	if len(embBytes)%4 != 0 {
		return 0, fmt.Errorf("unexpected sqlite embedding blob length %d", len(embBytes))
	}
	return len(embBytes) / 4, nil
}

func postgresVectorDimensions(ctx context.Context, pool *pgxpool.Pool, table string) (int, error) {
	var typeName sql.NullString
	query := `
SELECT format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = current_schema()
  AND c.relname = $1
  AND a.attname = 'embedding'
  AND NOT a.attisdropped
`
	if err := pool.QueryRow(ctx, query, table).Scan(&typeName); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if !typeName.Valid {
		return 0, nil
	}
	const prefix = "vector("
	if !strings.HasPrefix(typeName.String, prefix) || !strings.HasSuffix(typeName.String, ")") {
		return 0, fmt.Errorf("unexpected postgres vector type %q", typeName.String)
	}
	value := strings.TrimSuffix(strings.TrimPrefix(typeName.String, prefix), ")")
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func deserializeFloat32(b []byte) ([]float32, error) {
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("invalid embedding blob length %d: must be divisible by 4", len(b))
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v, nil
}

func sqliteCount(ctx context.Context, db *sql.DB, query string) (int64, error) {
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func postgresCount(ctx context.Context, pool *pgxpool.Pool, query string) (int64, error) {
	var count int64
	if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func parseSQLiteTime(raw string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format %q", raw)
}

func expandPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("empty path")
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			return home, nil
		}
		path = filepath.Join(home, path[2:])
	}
	return filepath.Abs(path)
}

func normalizeParentHash(parent sql.NullString) sql.NullString {
	if !parent.Valid {
		return sql.NullString{}
	}
	if strings.TrimSpace(parent.String) == "" {
		return sql.NullString{}
	}
	return parent
}

func nullStringValue(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}

func nullInt64Value(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func nilIfEmptyBytes(v []byte) any {
	if len(v) == 0 {
		return nil
	}
	return v
}

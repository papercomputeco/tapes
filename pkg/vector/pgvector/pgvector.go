// Package pgvector provides a PostgreSQL-backed vector driver using the pgvector extension.
package pgvector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvec "github.com/pgvector/pgvector-go"

	"github.com/papercomputeco/tapes/pkg/vector"
)

const (
	// DefaultTableName is the default table name for storing vector documents.
	DefaultTableName = "tapes_embeddings"
)

// Driver implements vector.Driver using PostgreSQL with the pgvector extension.
type Driver struct {
	pool       *pgxpool.Pool
	tableName  string
	dimensions uint
	logger     *slog.Logger
}

// Config holds configuration for the pgvector driver.
type Config struct {
	// ConnString is the PostgreSQL connection string (e.g. "postgres://user:pass@host:5432/db").
	ConnString string

	// TableName is the name of the table to store embeddings in.
	// Defaults to DefaultTableName if empty.
	TableName string

	// Dimensions is the number of dimensions for the embedding vectors.
	Dimensions uint
}

// NewDriver creates a new pgvector driver connected to PostgreSQL.
func NewDriver(c Config, log *slog.Logger) (*Driver, error) {
	if c.ConnString == "" {
		return nil, errors.New("pgvector connection string must be provided")
	}

	if c.Dimensions == 0 {
		return nil, errors.New("pgvector embedding dimensions cannot be 0, must be configured")
	}

	tableName := c.TableName
	if tableName == "" {
		tableName = DefaultTableName
	}

	pool, err := pgxpool.New(context.Background(), c.ConnString)
	if err != nil {
		return nil, fmt.Errorf("creating pgx connection pool: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: %w", vector.ErrConnection, err)
	}

	d := &Driver{
		pool:       pool,
		tableName:  tableName,
		dimensions: c.Dimensions,
		logger:     log,
	}

	if err := d.ensureSchema(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensuring schema: %w", err)
	}

	log.Info("connected to PostgreSQL with pgvector",
		"table", tableName,
		"dimensions", c.Dimensions,
	)

	return d, nil
}

func (d *Driver) ensureSchema(ctx context.Context) error {
	// Enable the pgvector extension
	if _, err := d.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
		return fmt.Errorf("enabling vector extension: %w", err)
	}

	// Create the embeddings table
	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			hash TEXT NOT NULL DEFAULT '',
			embedding vector(%d) NOT NULL
		)
	`, d.tableName, d.dimensions)

	if _, err := d.pool.Exec(ctx, createTable); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	// Create a cosine distance index for efficient similarity search.
	// IVFFlat requires rows to exist for training, so use HNSW which works on empty tables.
	createIndex := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s_embedding_idx
		ON %s
		USING hnsw (embedding vector_cosine_ops)
	`, d.tableName, d.tableName)

	if _, err := d.pool.Exec(ctx, createIndex); err != nil {
		return fmt.Errorf("creating index: %w", err)
	}

	return nil
}

// Add stores documents with their embeddings.
// If a document with the same ID already exists, it is updated.
func (d *Driver) Add(ctx context.Context, docs []vector.Document) error {
	if len(docs) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, hash, embedding)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			hash = EXCLUDED.hash,
			embedding = EXCLUDED.embedding
	`, d.tableName)

	batch := &pgx.Batch{}
	for _, doc := range docs {
		batch.Queue(query, doc.ID, doc.Hash, pgvec.NewVector(doc.Embedding))
	}

	br := d.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range docs {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upserting document: %w", err)
		}
	}

	d.logger.Debug("added documents to pgvector", "count", len(docs))
	return nil
}

// Query finds the topK most similar documents to the given embedding.
// Uses cosine distance; results are ordered by similarity (highest first).
func (d *Driver) Query(ctx context.Context, embedding []float32, topK int) ([]vector.QueryResult, error) {
	if topK <= 0 {
		topK = 10
	}

	// Cosine distance: 0 = identical, 2 = opposite.
	// Convert to similarity score: 1 - distance gives [−1, 1] range.
	query := fmt.Sprintf(`
		SELECT id, hash, embedding, 1 - (embedding <=> $1) AS score
		FROM %s
		ORDER BY embedding <=> $1
		LIMIT $2
	`, d.tableName)

	rows, err := d.pool.Query(ctx, query, pgvec.NewVector(embedding), topK)
	if err != nil {
		return nil, fmt.Errorf("querying vectors: %w", err)
	}
	defer rows.Close()

	var results []vector.QueryResult
	for rows.Next() {
		var (
			id    string
			hash  string
			emb   pgvec.Vector
			score float32
		)
		if err := rows.Scan(&id, &hash, &emb, &score); err != nil {
			return nil, fmt.Errorf("scanning query result: %w", err)
		}

		results = append(results, vector.QueryResult{
			Document: vector.Document{
				ID:        id,
				Hash:      hash,
				Embedding: emb.Slice(),
			},
			Score: score,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating query results: %w", err)
	}

	d.logger.Debug("queried pgvector", "results", len(results))
	return results, nil
}

// Get retrieves documents by their IDs.
func (d *Driver) Get(ctx context.Context, ids []string) ([]vector.Document, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	// Build parameterized query with positional args
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, hash, embedding
		FROM %s
		WHERE id IN (%s)
	`, d.tableName, strings.Join(placeholders, ","))

	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying documents: %w", err)
	}
	defer rows.Close()

	var docs []vector.Document
	for rows.Next() {
		var (
			id   string
			hash string
			emb  pgvec.Vector
		)
		if err := rows.Scan(&id, &hash, &emb); err != nil {
			return nil, fmt.Errorf("scanning document: %w", err)
		}
		docs = append(docs, vector.Document{
			ID:        id,
			Hash:      hash,
			Embedding: emb.Slice(),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating documents: %w", err)
	}

	return docs, nil
}

// Delete removes documents by their IDs.
func (d *Driver) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`DELETE FROM %s WHERE id IN (%s)`, d.tableName, strings.Join(placeholders, ","))

	if _, err := d.pool.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("deleting documents: %w", err)
	}

	d.logger.Debug("deleted documents from pgvector", "count", len(ids))
	return nil
}

// Close releases resources held by the driver.
func (d *Driver) Close() error {
	d.pool.Close()
	return nil
}

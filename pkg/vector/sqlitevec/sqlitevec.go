// Package sqlitevec provides a SQLite-backed vector driver using sqlite-vec.
package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/vector"
)

// SQLiteVecDriver implements vector.Driver using SQLite with sqlite-vec.
type SQLiteVecDriver struct {
	db     *sql.DB
	logger *zap.Logger
}

// Config holds configuration for the SQLite vec driver.
type Config struct {
	// DBPath is the path to the SQLite database file.
	// Use ":memory:" for an in-memory database.
	DBPath string

	// Dimensions is the number of dimensions for the embedding vectors.
	// Defaults to DefaultDimensions if zero.
	Dimensions uint
}

// NewSQLiteVecDriver creates a new SQLite vector driver backed by sqlite-vec.
func NewSQLiteVecDriver(c Config, logger *zap.Logger) (*SQLiteVecDriver, error) {
	// enable connection to have sqlite-vec extension
	sqlite_vec.Auto()

	if c.DBPath == "" {
		return nil, fmt.Errorf("database path is required")
	}

	dimensions := c.Dimensions
	if dimensions == 0 {
		return nil, fmt.Errorf("sqlite-vec embedding dimensions cannot be 0, must be configured")
	}

	db, err := sql.Open("sqlite3", c.DBPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Verify sqlite-vec is loaded
	var vecVersion string
	if err := db.QueryRow("SELECT vec_version()").Scan(&vecVersion); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite-vec not available: %w", err)
	}

	// Create the document ID mapping table.
	// vec0 virtual tables use integer rowids, so we need a mapping from
	// string document IDs to integer rowids.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS vec_documents (
			rowid INTEGER PRIMARY KEY AUTOINCREMENT,
			doc_id TEXT NOT NULL UNIQUE,
			hash TEXT NOT NULL DEFAULT ''
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("creating documents table: %w", err)
	}

	// Create the vec0 virtual table for vector storage and KNN queries.
	createVec := fmt.Sprintf(
		`CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings USING vec0(embedding float[%d])`,
		dimensions,
	)
	if _, err := db.Exec(createVec); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating vec0 table: %w", err)
	}

	logger.Info("sqlite-vec vector driver initialized",
		zap.String("db_path", c.DBPath),
		zap.Uint("dimensions", dimensions),
		zap.String("vec_version", vecVersion),
	)

	return &SQLiteVecDriver{
		db:     db,
		logger: logger,
	}, nil
}

// serializeFloat32 converts a float32 slice to a little-endian byte slice
// suitable for sqlite-vec BLOB format.
func serializeFloat32(v []float32) ([]byte, error) {
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf, nil
}

// deserializeFloat32 converts a little-endian byte slice back to a float32 slice.
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

// Add stores documents with their embeddings.
// If a document with the same ID already exists, it is updated.
func (d *SQLiteVecDriver) Add(ctx context.Context, docs []vector.Document) error {
	if len(docs) == 0 {
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for _, doc := range docs {
		embBlob, err := serializeFloat32(doc.Embedding)
		if err != nil {
			return fmt.Errorf("serializing embedding for doc %s: %w", doc.ID, err)
		}

		// Check if document already exists
		var existingRowID int64
		err = tx.QueryRowContext(ctx,
			`SELECT rowid FROM vec_documents WHERE doc_id = ?`, doc.ID,
		).Scan(&existingRowID)

		switch err {
		case nil:
			// Document exists — update hash and embedding
			if _, err := tx.ExecContext(ctx,
				`UPDATE vec_documents SET hash = ? WHERE rowid = ?`,
				doc.Hash, existingRowID,
			); err != nil {
				return fmt.Errorf("updating document %s: %w", doc.ID, err)
			}

			// Update embedding in vec0 table via DELETE + INSERT
			// (vec0 does not support UPDATE)
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM vec_embeddings WHERE rowid = ?`, existingRowID,
			); err != nil {
				return fmt.Errorf("deleting old embedding for doc %s: %w", doc.ID, err)
			}

			if _, err := tx.ExecContext(ctx,
				`INSERT INTO vec_embeddings(rowid, embedding) VALUES (?, ?)`,
				existingRowID, embBlob,
			); err != nil {
				return fmt.Errorf("re-inserting embedding for doc %s: %w", doc.ID, err)
			}
		case sql.ErrNoRows:
			// New document — insert into mapping table first to get the rowid
			result, err := tx.ExecContext(ctx,
				`INSERT INTO vec_documents(doc_id, hash) VALUES (?, ?)`,
				doc.ID, doc.Hash,
			)
			if err != nil {
				return fmt.Errorf("inserting document %s: %w", doc.ID, err)
			}

			rowID, err := result.LastInsertId()
			if err != nil {
				return fmt.Errorf("getting rowid for doc %s: %w", doc.ID, err)
			}

			// Insert embedding into vec0 table with matching rowid
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO vec_embeddings(rowid, embedding) VALUES (?, ?)`,
				rowID, embBlob,
			); err != nil {
				return fmt.Errorf("inserting embedding for doc %s: %w", doc.ID, err)
			}
		default:
			return fmt.Errorf("checking for existing document %s: %w", doc.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	d.logger.Debug("added documents to sqlite-vec",
		zap.Int("count", len(docs)),
	)

	return nil
}

// Query finds the topK most similar documents to the given embedding.
func (d *SQLiteVecDriver) Query(ctx context.Context, embedding []float32, topK int) ([]vector.QueryResult, error) {
	if topK <= 0 {
		topK = 10
	}

	queryBlob, err := serializeFloat32(embedding)
	if err != nil {
		return nil, fmt.Errorf("serializing query embedding: %w", err)
	}

	// Use KNN query via vec0 MATCH, then JOIN back to get doc_id and hash.
	rows, err := d.db.QueryContext(ctx, `
		SELECT
			d.doc_id,
			d.hash,
			ve.distance
		FROM vec_embeddings ve
		INNER JOIN vec_documents d ON d.rowid = ve.rowid
		WHERE ve.embedding MATCH ?
			AND ve.k = ?
		ORDER BY ve.distance
	`, queryBlob, topK)
	if err != nil {
		return nil, fmt.Errorf("querying vectors: %w", err)
	}
	defer rows.Close()

	var results []vector.QueryResult
	for rows.Next() {
		var docID, hash string
		var distance float64
		if err := rows.Scan(&docID, &hash, &distance); err != nil {
			return nil, fmt.Errorf("scanning query result: %w", err)
		}

		results = append(results, vector.QueryResult{
			Document: vector.Document{
				ID:   docID,
				Hash: hash,
			},
			// Convert distance to similarity score: lower distance = higher similarity
			Score: float32(1.0 / (1.0 + distance)),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating query results: %w", err)
	}

	d.logger.Debug("queried sqlite-vec",
		zap.Int("results", len(results)),
	)

	return results, nil
}

// Get retrieves documents by their IDs.
func (d *SQLiteVecDriver) Get(ctx context.Context, ids []string) ([]vector.Document, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT d.doc_id, d.hash, d.rowid
		FROM vec_documents d
		WHERE d.doc_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying documents: %w", err)
	}
	defer rows.Close()

	// Collect results first so we can close the rows cursor before
	// issuing additional queries (SQLite uses a single connection).
	type docRow struct {
		docID string
		hash  string
		rowID int64
	}
	var docRows []docRow

	for rows.Next() {
		var dr docRow
		if err := rows.Scan(&dr.docID, &dr.hash, &dr.rowID); err != nil {
			return nil, fmt.Errorf("scanning document: %w", err)
		}
		docRows = append(docRows, dr)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating documents: %w", err)
	}
	rows.Close()

	// Now retrieve embeddings for each document
	docs := make([]vector.Document, 0, len(docRows))
	for _, dr := range docRows {
		doc := vector.Document{
			ID:   dr.docID,
			Hash: dr.hash,
		}

		var embBlob []byte
		err := d.db.QueryRowContext(ctx,
			`SELECT embedding FROM vec_embeddings WHERE rowid = ?`, dr.rowID,
		).Scan(&embBlob)
		if err == nil && len(embBlob) > 0 {
			doc.Embedding, _ = deserializeFloat32(embBlob)
		}

		docs = append(docs, doc)
	}

	return docs, nil
}

// Delete removes documents by their IDs.
func (d *SQLiteVecDriver) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	// Build placeholders for IN clause
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	inClause := strings.Join(placeholders, ",")

	// First, get the rowids for the documents to delete from vec0
	query := fmt.Sprintf(
		`SELECT rowid FROM vec_documents WHERE doc_id IN (%s)`, inClause,
	)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("querying rowids for deletion: %w", err)
	}

	var rowIDs []int64
	for rows.Next() {
		var rowID int64
		if err := rows.Scan(&rowID); err != nil {
			rows.Close()
			return fmt.Errorf("scanning rowid: %w", err)
		}
		rowIDs = append(rowIDs, rowID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating rowids: %w", err)
	}

	// Delete embeddings from vec0 table
	for _, rowID := range rowIDs {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM vec_embeddings WHERE rowid = ?`, rowID,
		); err != nil {
			return fmt.Errorf("deleting embedding rowid %d: %w", rowID, err)
		}
	}

	// Delete from mapping table
	deleteQuery := fmt.Sprintf(
		`DELETE FROM vec_documents WHERE doc_id IN (%s)`, inClause,
	)
	if _, err := tx.ExecContext(ctx, deleteQuery, args...); err != nil {
		return fmt.Errorf("deleting documents: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	d.logger.Debug("deleted documents from sqlite-vec",
		zap.Int("count", len(ids)),
	)

	return nil
}

// Close releases resources held by the driver.
func (d *SQLiteVecDriver) Close() error {
	return d.db.Close()
}

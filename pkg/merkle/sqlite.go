package merkle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "modernc.org/sqlite"
)

// SQLiteStorer implements Storer using SQLite as the storage backend.
type SQLiteStorer struct {
	db *sql.DB
}

// NewSQLiteStorer creates a new SQLite-backed storer.
// The dbPath can be a file path or ":memory:" for an in-memory database.
func NewSQLiteStorer(dbPath string) (*SQLiteStorer, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	s := &SQLiteStorer{db: db}

	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return s, nil
}

// migrate creates the necessary tables if they don't exist.
func (s *SQLiteStorer) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS nodes (
		hash TEXT PRIMARY KEY,
		parent_hash TEXT,
		content TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_nodes_parent_hash ON nodes(parent_hash);
	`

	_, err := s.db.Exec(schema)
	return err
}

// Put stores a node. If the node already exists (by hash), this is a no-op.
func (s *SQLiteStorer) Put(ctx context.Context, node *Node) error {
	if node == nil {
		return fmt.Errorf("cannot store nil node")
	}

	contentJSON, err := json.Marshal(node.Content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %w", err)
	}

	// Use INSERT OR IGNORE for idempotent inserts (deduplication via content-addressing)
	query := `INSERT OR IGNORE INTO nodes (hash, parent_hash, content) VALUES (?, ?, ?)`

	_, err = s.db.ExecContext(ctx, query, node.Hash, node.ParentHash, string(contentJSON))
	if err != nil {
		return fmt.Errorf("failed to insert node: %w", err)
	}

	return nil
}

// Get retrieves a node by its hash.
func (s *SQLiteStorer) Get(ctx context.Context, hash string) (*Node, error) {
	query := `SELECT hash, parent_hash, content FROM nodes WHERE hash = ?`

	row := s.db.QueryRowContext(ctx, query, hash)

	var node Node
	var contentJSON string
	var parentHash sql.NullString

	err := row.Scan(&node.Hash, &parentHash, &contentJSON)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound{Hash: hash}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan node: %w", err)
	}

	if parentHash.Valid {
		node.ParentHash = &parentHash.String
	}

	if err := json.Unmarshal([]byte(contentJSON), &node.Content); err != nil {
		return nil, fmt.Errorf("failed to unmarshal content: %w", err)
	}

	return &node, nil
}

// Has checks if a node exists by its hash.
func (s *SQLiteStorer) Has(ctx context.Context, hash string) (bool, error) {
	query := `SELECT 1 FROM nodes WHERE hash = ? LIMIT 1`

	row := s.db.QueryRowContext(ctx, query, hash)

	var exists int
	err := row.Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}

	return true, nil
}

// GetByParent retrieves all nodes that have the given parent hash.
func (s *SQLiteStorer) GetByParent(ctx context.Context, parentHash *string) ([]*Node, error) {
	var query string
	var args []any

	if parentHash == nil {
		query = `SELECT hash, parent_hash, content FROM nodes WHERE parent_hash IS NULL`
	} else {
		query = `SELECT hash, parent_hash, content FROM nodes WHERE parent_hash = ?`
		args = append(args, *parentHash)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}
	defer rows.Close()

	return s.scanNodes(rows)
}

// List returns all nodes in the store.
func (s *SQLiteStorer) List(ctx context.Context) ([]*Node, error) {
	query := `SELECT hash, parent_hash, content FROM nodes ORDER BY created_at`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}
	defer rows.Close()

	return s.scanNodes(rows)
}

// Roots returns all root nodes (nodes with no parent).
func (s *SQLiteStorer) Roots(ctx context.Context) ([]*Node, error) {
	return s.GetByParent(ctx, nil)
}

// Leaves returns all leaf nodes (nodes with no children).
func (s *SQLiteStorer) Leaves(ctx context.Context) ([]*Node, error) {
	// Find nodes whose hash is not referenced as a parent by any other node
	query := `
		SELECT n.hash, n.parent_hash, n.content 
		FROM nodes n
		LEFT JOIN nodes c ON c.parent_hash = n.hash
		WHERE c.hash IS NULL
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query leaves: %w", err)
	}
	defer rows.Close()

	return s.scanNodes(rows)
}

// Ancestry returns the path from a node back to its root (node first, root last).
func (s *SQLiteStorer) Ancestry(ctx context.Context, hash string) ([]*Node, error) {
	var path []*Node
	current := hash

	for {
		node, err := s.Get(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("getting node %s: %w", current, err)
		}
		path = append(path, node)

		if node.ParentHash == nil {
			break
		}
		current = *node.ParentHash
	}

	return path, nil
}

// Descendants returns the path from root to node (root first, node last).
func (s *SQLiteStorer) Descendants(ctx context.Context, hash string) ([]*Node, error) {
	path, err := s.Ancestry(ctx, hash)
	if err != nil {
		return nil, err
	}

	// Reverse the path
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return path, nil
}

// Depth returns the depth of a node (0 for roots).
func (s *SQLiteStorer) Depth(ctx context.Context, hash string) (int, error) {
	depth := 0
	current := hash

	for {
		node, err := s.Get(ctx, current)
		if err != nil {
			return 0, err
		}
		if node.ParentHash == nil {
			break
		}
		depth++
		current = *node.ParentHash
	}

	return depth, nil
}

// scanNodes scans multiple rows into Node structs.
func (s *SQLiteStorer) scanNodes(rows *sql.Rows) ([]*Node, error) {
	var nodes []*Node

	for rows.Next() {
		var node Node
		var contentJSON string
		var parentHash sql.NullString

		if err := rows.Scan(&node.Hash, &parentHash, &contentJSON); err != nil {
			return nil, fmt.Errorf("failed to scan node: %w", err)
		}

		if parentHash.Valid {
			node.ParentHash = &parentHash.String
		}

		if err := json.Unmarshal([]byte(contentJSON), &node.Content); err != nil {
			return nil, fmt.Errorf("failed to unmarshal content: %w", err)
		}

		nodes = append(nodes, &node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return nodes, nil
}

// Close closes the database connection.
func (s *SQLiteStorer) Close() error {
	return s.db.Close()
}

// Ensure SQLiteStorer implements Storer
var _ Storer = (*SQLiteStorer)(nil)

//go:build turso

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/tursodatabase/go-libsql" // register the "libsql" database/sql driver
)

// openSQLiteDB opens a SQLite database using the go-libsql driver.
func openSQLiteDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	// go-libsql accepts ":memory:" as-is, or "file:" prefixed paths.
	dsn := dbPath
	if dsn != ":memory:" && !strings.HasPrefix(dsn, "file:") {
		dsn = "file:" + dsn
	}

	db, err := sql.Open("libsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable foreign keys via PRAGMA (go-libsql does not support DSN query parameters).
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	return db, nil
}

//go:build !turso

package sqlite

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3" // register the "sqlite3" database/sql driver
)

// openSQLiteDB opens a SQLite database using the mattn/go-sqlite3 driver.
func openSQLiteDB(dbPath string) (*sql.DB, error) {
	// Enable foreign keys via DSN query parameter so that every pooled
	// connection has the pragma applied (not just the first one).
	dsn := dbPath
	if !strings.Contains(dsn, "?") {
		dsn += "?_foreign_keys=on"
	} else {
		dsn += "&_foreign_keys=on"
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return db, nil
}

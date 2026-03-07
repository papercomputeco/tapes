//go:build !turso

package migrate_test

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func openTestDB() (*sql.DB, error) {
	return sql.Open("sqlite3", ":memory:")
}

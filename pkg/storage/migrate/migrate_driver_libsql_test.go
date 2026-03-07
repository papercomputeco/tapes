//go:build turso

package migrate_test

import (
	"database/sql"

	_ "github.com/tursodatabase/go-libsql"
)

func openTestDB() (*sql.DB, error) {
	return sql.Open("libsql", ":memory:")
}

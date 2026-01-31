// Package sqlite provides a SQLite-backed storage driver using ent ORM.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/mattn/go-sqlite3"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	entdriver "github.com/papercomputeco/tapes/pkg/storage/ent/driver"
)

// SQLiteDriver implements storage.Driver using SQLite via the ent driver
type SQLiteDriver struct {
	*entdriver.EntDriver
}

// NewSQLiteDriver creates a new SQLite-backed storer.
// The dbPath can be a file path or ":memory:" for an in-memory database.
func NewSQLiteDriver(dbPath string) (*SQLiteDriver, error) {
	// Open the database using the github.com/mattn/go-sqlite3 driver (registered as "sqlite3")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// SQLite-specific pragmas
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Wrap the database connection with ent's SQL driver
	drv := entsql.OpenDB(dialect.SQLite, db)
	client := ent.NewClient(ent.Driver(drv))

	// Run ent's auto-migration to create/update the schema
	// This handles append-only schema changes (new tables, columns, indexes)
	if err := client.Schema.Create(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return &SQLiteDriver{
		EntDriver: &entdriver.EntDriver{
			Client: client,
		},
	}, nil
}

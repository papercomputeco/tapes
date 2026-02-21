// Package postgres provides a PostgreSQL-backed storage driver using ent ORM.
package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/jackc/pgx/v5/stdlib" // register the pgx PostgreSQL driver as "pgx"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	entdriver "github.com/papercomputeco/tapes/pkg/storage/ent/driver"
)

// Driver implements storage.Driver using PostgreSQL via the ent driver.
type Driver struct {
	*entdriver.EntDriver
}

// NewDriver creates a new PostgreSQL-backed storer.
// The connStr is a PostgreSQL connection string, e.g.
// "host=localhost port=5432 user=tapes password=tapes dbname=tapes sslmode=disable"
// or a connection URI like "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable".
func NewDriver(ctx context.Context, connStr string) (*Driver, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Verify the connection is reachable
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Wrap the database connection with ent's SQL driver
	drv := entsql.OpenDB(dialect.Postgres, db)
	client := ent.NewClient(ent.Driver(drv))

	// Run ent's auto-migration to create/update the schema
	if err := client.Schema.Create(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return &Driver{
		EntDriver: &entdriver.EntDriver{
			Client: client,
		},
	}, nil
}

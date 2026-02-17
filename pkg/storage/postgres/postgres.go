// Package postgres provides a PostgreSQL-backed storage driver using ent ORM.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/jackc/pgx/v5/stdlib" // register the pgx PostgreSQL driver as "pgx"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	entdriver "github.com/papercomputeco/tapes/pkg/storage/ent/driver"
	"github.com/papercomputeco/tapes/pkg/storage/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Driver implements storage.Driver using PostgreSQL via the ent driver.
type Driver struct {
	*entdriver.EntDriver
	db *sql.DB
}

// NewDriver creates a new PostgreSQL-backed storer.
// The connStr is a PostgreSQL connection string, e.g.
// "host=localhost port=5432 user=tapes password=tapes dbname=tapes sslmode=disable"
// or a connection URI like "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable".
//
// NewDriver does not run schema migrations. Call Migrate() after construction
// to apply any pending migrations.
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

	return &Driver{
		EntDriver: &entdriver.EntDriver{
			Client: client,
		},
		db: db,
	}, nil
}

// Migrate applies any pending schema migrations using the versioned migration engine.
// It is safe to call concurrently from multiple processes — a Postgres advisory lock
// serializes concurrent migrators.
func (d *Driver) Migrate(ctx context.Context) error {
	subFS, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("reading embedded migrations sub-directory: %w", err)
	}

	migrations, err := migrate.MigrationsFromFS(subFS)
	if err != nil {
		return fmt.Errorf("loading embedded migrations: %w", err)
	}

	migrator, err := migrate.NewMigrator(d.db, migrate.DialectPostgres, migrations)
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}

	return migrator.Apply(ctx)
}

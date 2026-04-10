// Package sqlite provides a SQLite-backed storage driver using ent ORM.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"strings"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/mattn/go-sqlite3" // load up the sqlite3 CGO libs

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	entdriver "github.com/papercomputeco/tapes/pkg/storage/ent/driver"
	"github.com/papercomputeco/tapes/pkg/storage/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Driver implements storage.Driver using SQLite via the ent driver
type Driver struct {
	*entdriver.EntDriver
	db *sql.DB
}

// NewDriver creates a new SQLite-backed storer.
// The dbPath can be a file path or ":memory:" for an in-memory database.
//
// NewDriver does not run schema migrations. Call Migrate() after construction
// to apply any pending migrations.
func NewDriver(_ context.Context, dbPath string) (*Driver, error) {
	// Enable foreign keys via DSN query parameter so that every pooled
	// connection has the pragma applied (not just the first one).
	dsn := dbPath
	if !strings.Contains(dsn, "?") {
		dsn += "?_foreign_keys=on"
	} else {
		dsn += "&_foreign_keys=on"
	}

	// Open the database using the github.com/mattn/go-sqlite3 driver (registered as "sqlite3")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Wrap the database connection with ent's SQL driver
	drv := entsql.OpenDB(dialect.SQLite, db)
	client := ent.NewClient(ent.Driver(drv))

	return &Driver{
		EntDriver: &entdriver.EntDriver{
			Client:  client,
			DB:      db,
			Dialect: dialect.SQLite,
		},
		db: db,
	}, nil
}

// Migrate applies any pending schema migrations using the versioned migration engine.
func (d *Driver) Migrate(ctx context.Context) error {
	subFS, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("reading embedded migrations sub-directory: %w", err)
	}

	migrations, err := migrate.MigrationsFromFS(subFS)
	if err != nil {
		return fmt.Errorf("loading embedded migrations: %w", err)
	}

	migrator, err := migrate.NewMigrator(d.db, migrate.DialectSQLite, migrations)
	if err != nil {
		return fmt.Errorf("creating migrator: %w", err)
	}

	return migrator.Apply(ctx)
}

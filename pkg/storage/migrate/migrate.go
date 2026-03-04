// Package migrate provides a versioned schema migration engine for tapes storage backends.
//
// Storage drivers construct a [Migrator] with [NewMigrator], passing their own
// ordered list of [Migration] values (typically loaded via //go:embed from a
// migrations/ directory under the driver package). The engine tracks applied
// versions in a schema_migrations table with a UNIQUE constraint on version,
// and detects gaps to fail fast on corrupted state.
//
// Postgres uses an advisory lock to serialize concurrent migrators; SQLite
// relies on its file-level lock.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
)

const (
	// DialectPostgres identifies the PostgreSQL dialect.
	DialectPostgres = "postgres"

	// DialectSQLite identifies the SQLite dialect.
	DialectSQLite = "sqlite"
)

// advisoryLockID is the Postgres advisory lock key used to serialize
// concurrent migration attempts. pg_advisory_lock requires a bigint,
// so we hash a human-readable name to a stable int64 at init time.
var advisoryLockID = func() int64 {
	h := fnv.New64a()
	h.Write([]byte("tapes_schema_migrations_lock"))
	return int64(h.Sum64())
}()

// Migration represents a single versioned schema migration.
type Migration struct {
	// Version is the sequential migration number (1, 2, 3, ...).
	Version int

	// Description is a human-readable summary of the migration.
	Description string

	// Up is the SQL to apply for this migration.
	// The SQL is dialect-specific — each driver supplies its own migrations
	// so the engine does not need to branch on dialect for the SQL itself.
	Up string
}

// Migrator applies an ordered set of migrations to a database.
// Create one with [NewMigrator].
type Migrator struct {
	db         *sql.DB
	dialect    string
	migrations []Migration
}

// NewMigrator creates a Migrator for the given database, dialect, and ordered
// migration list. The dialect must be [DialectPostgres] or [DialectSQLite];
// unknown dialects are rejected immediately.
//
// Migrations are sorted by version internally. Callers should supply them in
// order, but the constructor enforces sorting for safety.
func NewMigrator(db *sql.DB, dialect string, migrations []Migration) (*Migrator, error) {
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return nil, fmt.Errorf("unsupported dialect %q: must be %q or %q", dialect, DialectPostgres, DialectSQLite)
	}

	// Defensive copy + sort by version.
	sorted := make([]Migration, len(migrations))
	copy(sorted, migrations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Version < sorted[j].Version })

	return &Migrator{
		db:         db,
		dialect:    dialect,
		migrations: sorted,
	}, nil
}

// Apply runs all pending migrations against the database.
// It is safe to call concurrently from multiple processes — Postgres uses
// an advisory lock to serialize, and SQLite serializes via its file lock.
func (m *Migrator) Apply(ctx context.Context) error {
	if m.dialect == DialectPostgres {
		if _, err := m.db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID); err != nil {
			return fmt.Errorf("acquiring advisory lock: %w", err)
		}
		defer m.db.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockID) //nolint:errcheck
	}

	if err := m.ensureMigrationsTable(ctx); err != nil {
		return fmt.Errorf("ensuring migrations table: %w", err)
	}

	current, count, err := m.migrationState(ctx)
	if err != nil {
		return fmt.Errorf("reading migration state: %w", err)
	}

	// Gap detection: if the number of recorded rows doesn't match the
	// highest applied version we have migrations for, the state is corrupt.
	if count > 0 && count != current {
		return fmt.Errorf("migration state corrupt: %d version rows recorded but max version is %d (expected equal)", count, current)
	}

	for _, migration := range m.migrations {
		if migration.Version <= current {
			continue
		}

		if migration.Up == "" {
			return fmt.Errorf("migration %d (%s) has no SQL", migration.Version, migration.Description)
		}

		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("beginning transaction for migration %d: %w", migration.Version, err)
		}

		if _, err := tx.ExecContext(ctx, migration.Up); err != nil {
			tx.Rollback() //nolint:errcheck
			return fmt.Errorf("applying migration %d (%s): %w", migration.Version, migration.Description, err)
		}

		if err := m.setVersion(ctx, tx, migration.Version); err != nil {
			tx.Rollback() //nolint:errcheck
			return fmt.Errorf("recording migration %d: %w", migration.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing migration %d: %w", migration.Version, err)
		}
	}

	return nil
}

// CurrentVersion returns the current schema version for the database.
// Returns 0 if no migrations have been applied (or the table doesn't exist).
func (m *Migrator) CurrentVersion(ctx context.Context) (int, error) {
	if err := m.ensureMigrationsTable(ctx); err != nil {
		return 0, fmt.Errorf("ensuring migrations table: %w", err)
	}
	version, _, err := m.migrationState(ctx)
	return version, err
}

// ensureMigrationsTable creates the schema_migrations table if it doesn't exist.
// The version column has a UNIQUE constraint so duplicate rows cannot be inserted.
func (m *Migrator) ensureMigrationsTable(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER NOT NULL UNIQUE,
			applied_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

// migrationState returns the max applied version and the total number of
// recorded version rows. Both are needed for gap detection.
func (m *Migrator) migrationState(ctx context.Context) (maxVersion int, count int, err error) {
	err = m.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0), COUNT(*) FROM schema_migrations",
	).Scan(&maxVersion, &count)
	return
}

// setVersion inserts a new version record into the schema_migrations table.
func (m *Migrator) setVersion(ctx context.Context, tx *sql.Tx, version int) error {
	query := "INSERT INTO schema_migrations (version) VALUES ($1)"
	if m.dialect == DialectSQLite {
		query = "INSERT INTO schema_migrations (version) VALUES (?)"
	}
	_, err := tx.ExecContext(ctx, query, version)
	return err
}

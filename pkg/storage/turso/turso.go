//go:build turso

// Package turso provides a Turso/libSQL-backed storage driver using ent ORM.
//
// Turso is built on libSQL, an open-source fork of SQLite. This driver uses
// the embedded replica connector from github.com/tursodatabase/go-libsql,
// which keeps a local SQLite file in sync with a remote Turso database.
//
// The driver requires CGO (same as the existing SQLite driver).
package turso

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/tursodatabase/go-libsql"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
	entdriver "github.com/papercomputeco/tapes/pkg/storage/ent/driver"
	"github.com/papercomputeco/tapes/pkg/storage/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Driver implements storage.Driver using Turso/libSQL via the ent driver.
type Driver struct {
	*entdriver.EntDriver
	db        *sql.DB
	connector *libsql.Connector
}

type options struct {
	authToken    string
	localPath    string
	syncInterval time.Duration
}

// Option configures the Turso driver.
type Option func(*options)

// WithAuthToken sets the authentication token for the Turso database.
func WithAuthToken(token string) Option {
	return func(o *options) {
		o.authToken = token
	}
}

// WithLocalPath sets the local database file path for embedded replica mode.
// When set, the driver keeps a local SQLite copy that syncs with the remote.
func WithLocalPath(path string) Option {
	return func(o *options) {
		o.localPath = path
	}
}

// WithSyncInterval sets the automatic sync interval for embedded replicas.
// If zero (the default), sync must be triggered manually.
func WithSyncInterval(d time.Duration) Option {
	return func(o *options) {
		o.syncInterval = d
	}
}

// NewDriver creates a new Turso/libSQL-backed storer.
//
// The primaryURL is the Turso database URL (e.g. "libsql://<name>.turso.io").
// Options control authentication and embedded replica behavior.
//
// NewDriver does not run schema migrations. Call Migrate() after construction
// to apply any pending migrations.
func NewDriver(ctx context.Context, primaryURL string, opts ...Option) (*Driver, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	// Build the list of libsql connector options.
	var connOpts []libsql.Option
	if o.authToken != "" {
		connOpts = append(connOpts, libsql.WithAuthToken(o.authToken))
	}
	if o.syncInterval > 0 {
		connOpts = append(connOpts, libsql.WithSyncInterval(o.syncInterval))
	}

	// A local path is required by the go-libsql connector.
	localPath := o.localPath
	if localPath == "" {
		localPath = "turso-replica.db"
	}

	connector, err := libsql.NewEmbeddedReplicaConnector(localPath, primaryURL, connOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create libsql connector: %w", err)
	}

	db := sql.OpenDB(connector)

	// Verify the connection is reachable.
	if err := db.PingContext(ctx); err != nil {
		connector.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Wrap the database connection with ent's SQL driver.
	// libSQL is SQLite-compatible, so we use dialect.SQLite.
	drv := entsql.OpenDB(dialect.SQLite, db)
	client := ent.NewClient(ent.Driver(drv))

	return &Driver{
		EntDriver: &entdriver.EntDriver{
			Client: client,
		},
		db:        db,
		connector: connector,
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

// Close closes the database connection and the libsql connector.
func (d *Driver) Close() error {
	if err := d.EntDriver.Close(); err != nil {
		return err
	}
	if d.connector != nil {
		return d.connector.Close()
	}
	return nil
}

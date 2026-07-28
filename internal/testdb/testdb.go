// Package testdb resolves the Postgres the DB-backed test suites run against.
//
// Every suite that needs a real database asks here, so there is one definition
// of "is the test database usable" and one message telling you what to do when
// it isn't. Two packages previously carried byte-identical copies of this
// logic, which meant two places for the instructions to go stale.
//
// The DSN comes from TEST_POSTGRES_DSN, which is set for you in three places
// that must agree on the image but not on the address:
//
//   - the nix dev shell (flake.nix), pointing at the docker-compose Postgres;
//   - `make test-local`, which starts that same service first;
//   - the Dagger pipeline, which binds its own Postgres service container.
package testdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

// EnvVar names the connection string every DB-backed suite reads.
const EnvVar = "TEST_POSTGRES_DSN"

// connectTimeout bounds the reachability probe. It is short on purpose: the
// probe runs before every DB-backed suite, and a developer whose database is
// down should get the instructions quickly rather than watch a default TCP
// timeout elapse once per package.
const connectTimeout = 5 * time.Second

// suiteLockID is a process-independent advisory lock key ("tapes_db" in hex).
// Every package-level suite that mutates the shared test database holds this
// lock for its full run, while non-database packages remain free to run in
// parallel.
const suiteLockID int64 = 0x74617065735f6462

// ErrNotConfigured is returned when the DSN is absent from the environment.
var ErrNotConfigured = errors.New(EnvVar + " is not set")

// SuiteLock serializes package-level suites that mutate the shared test
// database. Go runs test packages concurrently, but several storage specs use
// whole-table TRUNCATE statements that cannot be isolated by per-spec IDs.
type SuiteLock struct {
	conn *pgx.Conn
}

// AcquireSuiteLock waits for exclusive use of the shared test database.
// PostgreSQL releases the advisory lock automatically if the test process
// exits before Close runs.
func AcquireSuiteLock(ctx context.Context) (*SuiteLock, error) {
	dsn, err := configuredDSN()
	if err != nil {
		return nil, err
	}

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	conn, err := pgx.Connect(connectCtx, dsn)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("cannot reach the test postgres at %s=%q for suite lock: %w.\n%s", EnvVar, dsn, err, hint())
	}

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1::bigint)", suiteLockID); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("acquire test database suite lock: %w", err)
	}

	return &SuiteLock{conn: conn}, nil
}

// Close releases the suite lock and closes its dedicated connection.
func (l *SuiteLock) Close(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}

	_, unlockErr := l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1::bigint)", suiteLockID)
	closeErr := l.conn.Close(ctx)
	l.conn = nil
	return errors.Join(unlockErr, closeErr)
}

// DSN returns a verified connection string for the test database.
//
// It probes the connection rather than handing back whatever is in the
// environment, so an unreachable database fails once with an explanation
// instead of once per spec with a driver error.
//
// Both failure modes stay failures rather than skips. A suite that skips when
// it cannot find a database looks identical to one that passed, and CI would
// go green having exercised none of the storage layer the moment its service
// binding broke.
func DSN() (string, error) {
	dsn, err := configuredDSN()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("cannot reach the test postgres at %s=%q: %w.\n%s", EnvVar, dsn, err, hint())
	}
	defer conn.Close(context.Background())

	if err := conn.Ping(ctx); err != nil {
		return "", fmt.Errorf("cannot ping the test postgres at %s=%q: %w.\n%s", EnvVar, dsn, err, hint())
	}

	return dsn, nil
}

func configuredDSN() (string, error) {
	dsn := os.Getenv(EnvVar)
	if dsn == "" {
		return "", fmt.Errorf("%w.\n%s", ErrNotConfigured, hint())
	}
	return dsn, nil
}

// hint is the whole point of this package: a failure here is almost always a
// database that isn't running, not a bug, and the fix is one command.
func hint() string {
	return "  Start it with:  make test-db-up      (or run the suite with: make test-local)\n" +
		"  Stop it with:   make test-db-down\n" +
		"\n" +
		"  The image must match the one CI uses — it carries pgvector and pg_duckdb,\n" +
		"  and a stock postgres fails pkg/spanembed on a missing extension. Use the\n" +
		"  make targets rather than starting a container by hand.\n" +
		"\n" +
		"  In the nix dev shell " + EnvVar + " is exported for you; if you just changed\n" +
		"  flake.nix, run `direnv reload`. In CI the Dagger pipeline sets it."
}

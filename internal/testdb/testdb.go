// Package testdb coordinates suites that share the Postgres service provided
// by the Dagger Test check.
package testdb

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

const (
	envVar      = "TEST_POSTGRES_DSN"
	suiteLockID = int64(0x74617065735f6462) // "tapes_db"
)

// ErrNotConfigured indicates that the suite is not running in Dagger's test
// environment.
var ErrNotConfigured = errors.New(envVar + " is not set; run tests with `make test` so Dagger provisions Postgres")

// Suite holds the DSN and advisory lock for a DB-backed test suite.
type Suite struct {
	dsn  string
	conn *pgx.Conn
}

// AcquireSuite resolves Dagger's Postgres DSN and serializes suites whose
// whole-table cleanup cannot safely overlap. Other test packages remain free to
// run in parallel.
func AcquireSuite(ctx context.Context) (*Suite, error) {
	dsn := os.Getenv(envVar)
	if dsn == "" {
		return nil, ErrNotConfigured
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to test postgres: %w", err)
	}

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1::bigint)", suiteLockID); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("acquire test postgres suite lock: %w", err)
	}

	return &Suite{dsn: dsn, conn: conn}, nil
}

// DSN returns the connection string for the Dagger-provided Postgres service.
func (s *Suite) DSN() string {
	return s.dsn
}

// Close releases the advisory lock by closing its dedicated connection.
func (s *Suite) Close(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return nil
	}

	err := s.conn.Close(ctx)
	s.conn = nil
	return err
}

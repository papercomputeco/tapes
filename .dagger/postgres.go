package main

import (
	"context"
	"dagger/tapes/internal/dagger"
	"fmt"
)

// These consts are exported from the dagger module as a test utility ready
// to use in integration tests throughout the tapes codebase.
const (
	postgresImage = "public.ecr.aws/g4e5l3z3/papercomputeco/postgres:17.7-pgduckdb-1.1.1"
	testPgHost    = "postgres"
	testPgUser    = "tapes"
	testPgPass    = "tapes"
	testPgDB      = "tapes"
	testPgPort    = 5432
)

// newPostgresDSN returns a new connection string used by the tapes services
// to reach the Postgres service container during integration tests.
func newPostgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", testPgUser, testPgPass, testPgHost, testPgPort, testPgDB)
}

// PostgresStack returns the full Postgres stack ready for use.
// This is a essentially a no op since the stack is just the postgres image but
// is kept for alignment with other service stacks (ollama-stack, kafka-stack, etc.).
func (m *Tapes) PostgresStack(ctx context.Context) (postresSvc *dagger.Service, err error) {
	return PostgresService(), nil
}

// PostgresService provides a ready to run postgres service with "tapes" user, password, and db
func PostgresService() *dagger.Service {
	return dag.Container().From(postgresImage).
		WithEnvVariable("POSTGRES_USER", testPgUser).
		WithEnvVariable("POSTGRES_PASSWORD", testPgPass).
		WithEnvVariable("POSTGRES_DB", testPgDB).
		WithExposedPort(testPgPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

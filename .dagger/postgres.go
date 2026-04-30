package main

import (
	"dagger/tapes/internal/dagger"
	"fmt"
)

// These consts are exported from the dagger module as a test utility ready
// to use in integration tests throughout the tapes codebase.
const (
	postgresImage = "public.ecr.aws/g4e5l3z3/papercomputeco/postgres:17.7-pgduckdb-1.1.1"
	testPgUser    = "tapes"
	testPgPass    = "tapes"
	testPgDB      = "tapes"
	testPgPort    = 5432
)

// newPostgresDSN returns a new connection string used by the tapes services
// to reach the Postgres service container during integration tests.
func newPostgresDSN() string {
	return fmt.Sprintf("host=postgres user=%s password=%s dbname=%s port=%d sslmode=disable", testPgUser, testPgPass, testPgDB, testPgPort)
}

// PostgresService provides a ready to run postgres service with "tapes" user, password, and db
func (m *Tapes) PostgresService() *dagger.Service {
	return dag.Container().From(postgresImage).
		WithEnvVariable("POSTGRES_USER", testPgUser).
		WithEnvVariable("POSTGRES_PASSWORD", testPgPass).
		WithEnvVariable("POSTGRES_DB", testPgDB).
		WithExposedPort(testPgPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

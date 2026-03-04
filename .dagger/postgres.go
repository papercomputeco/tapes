package main

import (
	"dagger/tapes/internal/dagger"
	"fmt"
)

const (
	pgUser = "tapes"
	pgPass = "tapes"
	pgDB   = "tapes"
	pgPort = 5432
)

var (
	// postgresDSN is the connection string used by the tapes services
	// to reach the Postgres service container.
	postgresDSN = fmt.Sprintf("host=postgres user=%s password=%s dbname=%s port=%d sslmode=disable", pgUser, pgPass, pgDB, pgPort)
)

// PostgresService provides a ready to run postgres service with "tapes" user, password, and db
func (m *Tapes) PostgresService() *dagger.Service {
	return dag.Container().
		From("postgres:17-bookworm").
		WithEnvVariable("POSTGRES_USER", pgUser).
		WithEnvVariable("POSTGRES_PASSWORD", pgPass).
		WithEnvVariable("POSTGRES_DB", pgDB).
		WithExposedPort(pgPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

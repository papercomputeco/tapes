package main

import (
	"context"
	"dagger/tapes/internal/dagger"
	"fmt"
)

// These consts are exported from the dagger module as a test utility ready
// to use in integration tests throughout the tapes codebase.
const (
	testPgUser = "tapes"
	testPgPass = "tapes"
	testPgDB   = "tapes"
	testPgPort = 5432
)

// newPostgresDSN returns a new connection string used by the tapes services
// to reach the Postgres service container during integration tests.
func newPostgresDSN() string {
	return fmt.Sprintf("host=postgres user=%s password=%s dbname=%s port=%d sslmode=disable", testPgUser, testPgPass, testPgDB, testPgPort)
}

// PostgresService provides a ready to run postgres service with "tapes" user, password, and db
func (m *Tapes) PostgresService() *dagger.Service {
	ctr := m.Source.DockerBuild(dagger.DirectoryDockerBuildOpts{
		Dockerfile: "postgres/Dockerfile",
	})

	return ctr.
		WithEnvVariable("POSTGRES_USER", testPgUser).
		WithEnvVariable("POSTGRES_PASSWORD", testPgPass).
		WithEnvVariable("POSTGRES_DB", testPgDB).
		WithExposedPort(testPgPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

// BuildPostgresImage builds the local-platform postgres container image from
// the repository postgres Dockerfile.
func (t *Tapes) BuildPostgresImage() *dagger.Container {
	return t.buildDockerfileImage("postgres/Dockerfile", nil)
}

// BuildPushPostgresImages builds a multi-arch postgres image and publishes it
// to the provided registry.
//
// Image naming convention: <registry>/postgres:<tag>
func (t *Tapes) BuildPushPostgresImages(
	ctx context.Context,

	// Container registry address (e.g., "123456789.dkr.ecr.us-east-1.amazonaws.com")
	registry string,

	// Image tags to apply (e.g., ["v1.0.0", "latest"])
	tags []string,
) ([]string, error) {
	return t.publishImageVariants(ctx, registry, postgresImageName, tags, t.buildDockerfileImages("postgres/Dockerfile", nil))
}

// Tapes CI/CD
//
// Package main provides reproducible builds and tests locally and in GitHub actions.
// It is the main harness for handling nearly all dev operations.
package main

import (
	"context"

	"dagger/tapes/internal/dagger"
)

// Tapes is the main module for the Tapes CI/CD pipeline
type Tapes struct {
	// Project source directory
	//
	// +private
	Source *dagger.Directory
}

// New creates a new Tapes CI/CD module instance
func New(
	// Project source directory.
	//
	// +defaultPath="/"
	// +ignore=[".git", ".direnv", ".devenv", "build", "tmp", "tapes.dev/node_modules", "tapes.dev/.astro"]
	source *dagger.Directory,
) *Tapes {
	return &Tapes{
		Source: source,
	}
}

// goContainer returns a Debian Bookworm-based Go container with gcc,
// libsqlite3-dev, CGO enabled, and the project source mounted.
//
// It is the shared foundation for tests, builds, and linting.
func (t *Tapes) goContainer() *dagger.Container {
	return dag.Container().
		From("golang:1.25-bookworm").
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("GOEXPERIMENT", "jsonv2").
		WithEnvVariable("PATH", "/go/bin:$PATH", dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithWorkdir("/src").
		WithDirectory("/src", t.Source)
}

// CheckGenerate verifies that sqlc-generated code is up to date
// by regenerating it from sqlc.yaml and diffing the result against
// the committed files.
//
// +check
func (t *Tapes) CheckGenerate(ctx context.Context) (string, error) {
	return t.goContainer().
		WithExec([]string{"cp", "-r", "pkg/storage/postgres/gensqlc", "/tmp/gensqlc-before"}).
		WithExec([]string{"go", "run", "github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0", "generate", "-f", "sqlc.yaml"}).
		WithExec([]string{"diff", "-r", "/tmp/gensqlc-before", "pkg/storage/postgres/gensqlc"}).
		Stdout(ctx)
}

// Test runs the tapes unit tests via "go test" and provides a just-in-time
// Postgres service that can be used by the tests via the "TEST_POSTGRES_DSN" env var.
//
// +check
func (t *Tapes) Test(ctx context.Context) (string, error) {
	postgresSvc, err := t.PostgresStack(ctx)
	if err != nil {
		return "", err
	}

	dsn := newPostgresDSN()
	ctr := t.goContainer().
		WithServiceBinding("postgres", postgresSvc).
		WithEnvVariable("TEST_POSTGRES_DSN", dsn).
		WithExec([]string{"go", "test", "-v", "./..."})

	return ctr.Stdout(ctx)
}

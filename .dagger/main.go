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

// Test runs the tapes unit tests via "go test"
func (t *Tapes) Test(ctx context.Context) (string, error) {
	return t.testContainer().
		WithExec([]string{"go", "test", "-v", "./..."}).
		Stdout(ctx)
}

// testContainer returns a container configured for running tests
// Tests require CGO for sqlite3
func (t *Tapes) testContainer() *dagger.Container {
	return dag.Container().
		From("golang:1.25-alpine").
		WithExec([]string{"apk", "add", "--no-cache", "gcc", "musl-dev", "sqlite-dev"}).
		WithEnvVariable("CGO_ENABLED", "1").
		WithEnvVariable("GOEXPERIMENT", "jsonv2").
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithWorkdir("/src").
		WithDirectory("/src", t.Source)
}

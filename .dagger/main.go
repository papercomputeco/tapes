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
		WithExec([]string{"apt-get", "update"}).
		WithExec([]string{"apt-get", "install", "-y", "gcc", "libsqlite3-dev"}).
		WithEnvVariable("CGO_ENABLED", "1").
		WithEnvVariable("GOEXPERIMENT", "jsonv2").
		WithEnvVariable("PATH", "/go/bin:$PATH", dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithWorkdir("/src").
		WithDirectory("/src", t.Source)
}

// CheckGenerate verifies that generated code (e.g. ent) is up to date
// by running go generate and diffing the result against the committed files.
func (t *Tapes) CheckGenerate(ctx context.Context) (string, error) {
	return t.goContainer().
		WithExec([]string{"cp", "-r", "pkg/storage/ent", "/tmp/ent-before"}).
		WithExec([]string{"go", "generate", "./pkg/storage/ent/..."}).
		WithExec([]string{"diff", "-r", "/tmp/ent-before", "pkg/storage/ent"}).
		Stdout(ctx)
}

// Test runs the tapes unit tests via "go test"
func (t *Tapes) Test(ctx context.Context) (string, error) {
	return t.goContainer().
		WithExec([]string{"go", "test", "-v", "./..."}).
		Stdout(ctx)
}

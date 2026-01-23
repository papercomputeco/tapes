package main

import (
	"fmt"
	"strings"
	"time"

	"context"

	"dagger/tapes/internal/dagger"
)

// Build and return directory of go binaries
func (t *Tapes) Build(
	ctx context.Context,

	// Linker flags for go build
	// +optional
	// +default="-s -w"
	ldflags string,
) *dagger.Directory {
	// define build matrix
	gooses := []string{"linux", "darwin"}
	goarches := []string{"amd64", "arm64"}

	// create empty directory to put build artifacts
	outputs := dag.Directory()

	golang := dag.Container().
		From("golang:1.25-alpine").
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("GOEXPERIMENT", "jsonv2").
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithDirectory("/src", t.Source).
		WithWorkdir("/src")

	for _, goos := range gooses {
		for _, goarch := range goarches {
			// create directory for each OS and architecture
			path := fmt.Sprintf("%s/%s/", goos, goarch)

			// build artifact
			build := golang.
				WithEnvVariable("GOOS", goos).
				WithEnvVariable("GOARCH", goarch).
				WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapes"}).
				WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesprox"}).
				WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesapi"})

			// add build to outputs
			outputs = outputs.WithDirectory(path, build.Directory(path))
		}
	}

	// return build directory
	return outputs
}

// BuildRelease compiles versioned release binaries with embedded version info
func (t *Tapes) BuildRelease(
	ctx context.Context,

	// Version string of build
	version string,

	// Git commit SHA of build
	commit string,
) *dagger.Directory {
	buildtime := time.Now()

	ldflags := []string{
		"-s",
		"-w",
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Version=%s'", version),
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Sha=%s'", commit),
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Buildtime=%s'", buildtime),
	}

	return t.Build(ctx, strings.Join(ldflags, " "))
}

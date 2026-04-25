package main

import (
	"fmt"
	"strings"
	"time"

	"context"

	"dagger/tapes/internal/dagger"
)

type buildTarget struct {
	goos   string
	goarch string
}

// Build and return directory of go binaries for all platforms.
func (t *Tapes) Build(
	ctx context.Context,

	// Linker flags for go build
	// +optional
	// +default="-s -w"
	ldflags string,
) *dagger.Directory {
	targets := []buildTarget{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
	}

	golang := t.goContainer()
	outputs := dag.Directory()

	for _, target := range targets {
		path := fmt.Sprintf("%s/%s/", target.goos, target.goarch)

		build := golang.
			WithEnvVariable("GOOS", target.goos).
			WithEnvVariable("GOARCH", target.goarch).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapes"})

		outputs = outputs.WithDirectory(path, build.Directory(path))
	}

	return outputs
}

func (t *Tapes) releaseLDFlags(version string, commit string, postHogPublicKey string, postHogEndpoint string) string {
	buildtime := time.Now()

	ldflags := []string{
		"-s",
		"-w",
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Version=%s'", version),
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Sha=%s'", commit),
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Buildtime=%s'", buildtime),
	}

	if postHogPublicKey != "" {
		ldflags = append(ldflags, fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/telemetry.PostHogAPIKey=%s'", postHogPublicKey))
	}

	if postHogEndpoint != "" {
		ldflags = append(ldflags, fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/telemetry.PostHogEndpoint=%s'", postHogEndpoint))
	}

	return strings.Join(ldflags, " ")
}

// BuildRelease compiles versioned release binaries with embedded version info
func (t *Tapes) BuildRelease(
	ctx context.Context,

	// Version string of build
	version string,

	// Git commit SHA of build
	commit string,

	// PostHog telemetry public API key (write-only). Empty disables telemetry.
	// +optional
	postHogPublicKey string,

	// PostHog ingestion endpoint
	// +optional
	postHogEndpoint string,
) *dagger.Directory {
	dir := t.Build(ctx, t.releaseLDFlags(version, commit, postHogPublicKey, postHogEndpoint))
	return t.checksum(ctx, dir)
}

// checksum generates SHA256 checksums for all files in the given dagger directory
func (t *Tapes) checksum(
	ctx context.Context,

	// Directory containing build artifacts
	dir *dagger.Directory,
) *dagger.Directory {
	// Use a container to generate checksums
	checksumContainer := dag.Container().
		From("alpine:latest").
		WithDirectory("/artifacts", dir).
		WithWorkdir("/artifacts").
		WithExec([]string{"sh", "-c", `
			find . -type f ! -name "*.sha256" | while read file; do
				sha256sum "$file" | sed 's|./||' > "${file}.sha256"
			done
		`})

	return checksumContainer.Directory("/artifacts")
}

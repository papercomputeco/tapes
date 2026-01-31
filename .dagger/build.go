package main

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"context"

	"dagger/tapes/internal/dagger"
)

const (
	zigVersion string = "0.15.2"
)

type buildTarget struct {
	goos   string
	goarch string
	cc     string
	cxx    string
}

func zigArch() string {
	switch runtime.GOARCH {
	case "arm64":
		return "aarch64"
	case "amd64":
		return "x86_64"
	default:
		return runtime.GOARCH
	}
}

// Build and return directory of go binaries
func (t *Tapes) Build(
	ctx context.Context,

	// Linker flags for go build
	// +optional
	// +default="-s -w"
	ldflags string,
) *dagger.Directory {
	targets := []buildTarget{
		{"linux", "amd64", "zig cc -target x86_64-linux-gnu", "zig c++ -target x86_64-linux-gnu"},
		{"linux", "arm64", "zig cc -target aarch64-linux-gnu", "zig c++ -target aarch64-linux-gnu"},
		{"darwin", "amd64", "zig cc -target x86_64-macos", "zig c++ -target x86_64-macos"},
		{"darwin", "arm64", "zig cc -target aarch64-macos", "zig c++ -target aarch64-macos"},
	}

	// create empty directory to put build artifacts
	outputs := dag.Directory()

	// Build zig download URL based on host architecture
	zigArch := zigArch()
	zigDownloadURL := fmt.Sprintf("https://ziglang.org/download/%s/zig-%s-linux-%s.tar.xz", zigVersion, zigArch, zigVersion)
	zigDir := fmt.Sprintf("zig-%s-linux-%s", zigArch, zigVersion)

	golang := dag.Container().
		From("golang:1.25-bookworm").
		WithExec([]string{"apt-get", "update"}).
		WithExec([]string{"apt-get", "install", "-y", "xz-utils"}).
		WithExec([]string{"sh", "-c", fmt.Sprintf("curl -L %s | tar -xJ -C /usr/local", zigDownloadURL)}).
		WithEnvVariable("PATH", fmt.Sprintf("/usr/local/%s:$PATH", zigDir), dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithEnvVariable("CGO_ENABLED", "1").
		WithEnvVariable("GOEXPERIMENT", "jsonv2").
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithDirectory("/src", t.Source).
		WithWorkdir("/src")

	for _, target := range targets {
		path := fmt.Sprintf("%s/%s/", target.goos, target.goarch)

		build := golang.
			WithEnvVariable("GOOS", target.goos).
			WithEnvVariable("GOARCH", target.goarch).
			WithEnvVariable("CC", target.cc).
			WithEnvVariable("CXX", target.cxx).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapes"}).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesprox"}).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesapi"})

		outputs = outputs.WithDirectory(path, build.Directory(path))
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

	dir := t.Build(ctx, strings.Join(ldflags, " "))
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

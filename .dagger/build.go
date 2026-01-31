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

	// osxcross image provides the macOS SDK and cross-compilation toolchain
	// for building CGO-enabled Go binaries targeting darwin from Linux containers.
	osxcrossImage string = "crazymax/osxcross:latest-ubuntu"
)

type buildTarget struct {
	goos       string
	goarch     string
	cc         string
	cxx        string
	cgoFlags   string
	cgoLdFlags string
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

// Build and return directory of go binaries for all platforms.
// Linux targets are cross-compiled using Zig as the C toolchain.
// Darwin targets are cross-compiled using osxcross (macOS SDK + clang).
func (t *Tapes) Build(
	ctx context.Context,

	// Linker flags for go build
	// +optional
	// +default="-s -w"
	ldflags string,
) *dagger.Directory {
	outputs := dag.Directory()
	outputs = t.buildLinux(outputs, ldflags)
	outputs = t.buildDarwin(outputs, ldflags)
	return outputs
}

// buildLinux compiles Go binaries for linux/amd64 and linux/arm64
// using Zig as the cross-compilation C toolchain.
func (t *Tapes) buildLinux(outputs *dagger.Directory, ldflags string) *dagger.Directory {
	cgoFlags := "-I/opt/sqlite -fno-sanitize=all"
	cgoLdFlags := "-fno-sanitize=all"

	targets := []buildTarget{
		{"linux", "amd64", "zig cc -target x86_64-linux-gnu", "zig c++ -target x86_64-linux-gnu", cgoFlags, cgoLdFlags},
		{"linux", "arm64", "zig cc -target aarch64-linux-gnu", "zig c++ -target aarch64-linux-gnu", cgoFlags, cgoLdFlags},
	}

	// Build zig download URL based on host architecture
	zigArch := zigArch()
	zigDownloadURL := fmt.Sprintf("https://ziglang.org/download/%s/zig-%s-linux-%s.tar.xz", zigVersion, zigArch, zigVersion)
	zigDir := fmt.Sprintf("zig-%s-linux-%s", zigArch, zigVersion)

	golang := dag.Container().
		From("golang:1.25-bookworm").
		WithExec([]string{"apt-get", "update"}).
		WithExec([]string{"apt-get", "install", "-y", "libsqlite3-dev", "xz-utils"}).
		WithExec([]string{"mkdir", "-p", "/opt/sqlite"}).
		WithExec([]string{"cp", "/usr/include/sqlite3.h", "/opt/sqlite/"}).
		WithExec([]string{"cp", "/usr/include/sqlite3ext.h", "/opt/sqlite/"}).
		WithExec([]string{"sh", "-c", fmt.Sprintf("curl -L %s | tar -xJ -C /usr/local", zigDownloadURL)}).
		WithEnvVariable("PATH", fmt.Sprintf("/usr/local/%s:$PATH", zigDir), dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithDirectory("/src", t.Source).
		WithWorkdir("/src")

	for _, target := range targets {
		path := fmt.Sprintf("%s/%s/", target.goos, target.goarch)

		build := golang.
			WithEnvVariable("CGO_ENABLED", "1").
			WithEnvVariable("GOEXPERIMENT", "jsonv2").
			WithEnvVariable("GOOS", target.goos).
			WithEnvVariable("GOARCH", target.goarch).
			WithEnvVariable("CC", target.cc).
			WithEnvVariable("CXX", target.cxx).
			WithEnvVariable("CGO_CFLAGS", target.cgoFlags).
			WithEnvVariable("CGO_LDFLAGS", target.cgoLdFlags).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapes"}).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesprox"}).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesapi"})

		outputs = outputs.WithDirectory(path, build.Directory(path))
	}

	return outputs
}

// buildDarwin compiles Go binaries for darwin/amd64 and darwin/arm64
// using the osxcross toolchain which provides the macOS SDK and clang
// cross-compilers inside a Linux container.
func (t *Tapes) buildDarwin(outputs *dagger.Directory, ldflags string) *dagger.Directory {
	cgoFlags := "-I/opt/sqlite"
	cgoLdFlags := ""

	targets := []buildTarget{
		{"darwin", "amd64", "o64-clang", "o64-clang++", cgoFlags, cgoLdFlags},
		{"darwin", "arm64", "oa64-clang", "oa64-clang++", cgoFlags, cgoLdFlags},
	}

	// Pull the osxcross toolchain (macOS SDK + clang cross-compilers)
	osxcross := dag.Container().
		From(osxcrossImage).
		Directory("/osxcross")

	// Use Debian Trixie as the base for darwin builds because the osxcross
	// toolchain binaries require GLIBC 2.38+ (Bookworm only has 2.36).
	golang := dag.Container().
		From("golang:1.25-trixie").
		WithExec([]string{"apt-get", "update"}).
		WithExec([]string{"apt-get", "install", "-y", "clang", "lld", "libsqlite3-dev"}).
		WithExec([]string{"mkdir", "-p", "/opt/sqlite"}).
		WithExec([]string{"cp", "/usr/include/sqlite3.h", "/opt/sqlite/"}).
		WithExec([]string{"cp", "/usr/include/sqlite3ext.h", "/opt/sqlite/"}).
		WithDirectory("/osxcross", osxcross).
		WithEnvVariable("PATH", "/osxcross/bin:$PATH", dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithEnvVariable("LD_LIBRARY_PATH", "/osxcross/lib:$LD_LIBRARY_PATH", dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithDirectory("/src", t.Source).
		WithWorkdir("/src")

	for _, target := range targets {
		path := fmt.Sprintf("%s/%s/", target.goos, target.goarch)

		build := golang.
			WithEnvVariable("CGO_ENABLED", "1").
			WithEnvVariable("GOEXPERIMENT", "jsonv2").
			WithEnvVariable("GOOS", target.goos).
			WithEnvVariable("GOARCH", target.goarch).
			WithEnvVariable("CC", target.cc).
			WithEnvVariable("CXX", target.cxx).
			WithEnvVariable("CGO_CFLAGS", target.cgoFlags).
			WithEnvVariable("CGO_LDFLAGS", target.cgoLdFlags).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapes"}).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesprox"}).
			WithExec([]string{"go", "build", "-ldflags", ldflags, "-o", path, "./cli/tapesapi"})

		outputs = outputs.WithDirectory(path, build.Directory(path))
	}

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

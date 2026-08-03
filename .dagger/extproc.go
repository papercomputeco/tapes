package main

import (
	"context"

	"dagger/tapes/internal/dagger"
)

const extprocImageName = "tapes-extproc"

// extprocPackages is the ext_proc adapter's own test surface: the service
// package, its header parser, and the replay server that exercises captured
// bundles through it.
var extprocPackages = []string{
	"./extproc/...",
	"./cli/extproc-replay-server/...",
}

// CheckExtproc runs the ext_proc adapter's test suite.
//
// It is separate from Test, and bound to no database, because none of these
// tests need one — extproc talks to ingest over HTTP and is tested against
// fixtures. That is what makes it affordable on every PR, which matters: Test
// binds Postgres and for that reason does not run on the PR path, so folding
// extproc's tests into it would have quietly retired the gate that used to run
// on every extproc change. This keeps that gate, including the wire-capture
// fidelity tests, which read the committed recordings and therefore always run
// rather than skipping.
//
// +check
func (t *Tapes) CheckExtproc(ctx context.Context) (string, error) {
	args := append([]string{"go", "test", "-count=1"}, extprocPackages...)

	return t.goContainer().
		WithExec(args).
		Stdout(ctx)
}

// BuildExtprocImage builds the local-platform tapes-extproc container image.
//
// A second Dockerfile rather than a second stage in the first one: the two
// binaries ship on different base images (extproc on distroless nonroot, whose
// uid is part of its deployed contract) and are released independently.
func (t *Tapes) BuildExtprocImage(
	_ context.Context,

	// Version string for ldflags
	version string,

	// Git commit SHA for ldflags
	commit string,
) *dagger.Container {
	return t.buildDockerfileImage("Dockerfile.extproc", []dagger.BuildArg{{
		Name: "LDFLAGS",
		// No PostHog keys: extproc emits Prometheus metrics and structured
		// logs, and has no product-telemetry surface to configure.
		Value: t.releaseLDFlags(version, commit, "", ""),
	}})
}

// BuildPushExtprocImages builds a multi-arch tapes-extproc image and publishes
// it to the provided registry.
//
// Image naming convention: <registry>/tapes-extproc:<tag>
func (t *Tapes) BuildPushExtprocImages(
	ctx context.Context,

	// Container registry address (e.g., "123456789.dkr.ecr.us-east-1.amazonaws.com")
	registry string,

	// Image tags to apply (e.g., ["v1.0.0", "latest"])
	tags []string,

	// Version string for ldflags
	version string,

	// Git commit SHA for ldflags
	commit string,
) ([]string, error) {
	images := t.buildDockerfileImages("Dockerfile.extproc", []dagger.BuildArg{{
		Name:  "LDFLAGS",
		Value: t.releaseLDFlags(version, commit, "", ""),
	}})
	return t.publishImageVariants(ctx, registry, extprocImageName, tags, images)
}

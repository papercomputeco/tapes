package main

import (
	"context"

	"dagger/tapes/internal/dagger"
)

// BuildTapesImage builds the local-platform tapes container image from the
// repository Dockerfile.
func (t *Tapes) BuildTapesImage(
	_ context.Context,

	// Version string for ldflags
	version string,

	// Git commit SHA for ldflags
	commit string,

	// PostHog telemetry public API key (write-only). Empty disables telemetry.
	// +optional
	postHogPublicKey string,
) *dagger.Container {
	return t.buildDockerfileImage("Dockerfile", []dagger.BuildArg{{
		Name:  "LDFLAGS",
		Value: t.releaseLDFlags(version, commit, postHogPublicKey, ""),
	}})
}

// BuildPushTapesImage builds a multi-arch image for tapes and publishes to the
// provided registry.
//
// Image naming convention: <registry>/tapes:<tag>
func (t *Tapes) BuildPushTapesImages(
	ctx context.Context,

	// Container registry address (e.g., "123456789.dkr.ecr.us-east-1.amazonaws.com")
	registry string,

	// Image tags to apply (e.g., ["v1.0.0", "latest"])
	tags []string,

	// Version string for ldflags
	version string,

	// Git commit SHA for ldflags
	commit string,

	// PostHog telemetry public API key (write-only). Empty disables telemetry.
	// +optional
	postHogPublicKey string,
) ([]string, error) {
	images := t.buildDockerfileImages("Dockerfile", []dagger.BuildArg{{
		Name:  "LDFLAGS",
		Value: t.releaseLDFlags(version, commit, postHogPublicKey, ""),
	}})
	return t.publishImageVariants(ctx, registry, tapesImageName, tags, images)
}

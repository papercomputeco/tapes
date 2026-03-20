package main

import (
	"context"
	"fmt"

	"dagger/tapes/internal/dagger"
)

const (
	imageName    = "tapes"
	runtimeImage = "gcr.io/distroless/base-debian12:nonroot"
)

// BuildImages builds multi-arch container images for tapes using "BuildRelease"
func (t *Tapes) BuildImages(
	ctx context.Context,

	// Version string for ldflags
	version string,

	// Git commit SHA for ldflags
	commit string,

	// PostHog telemetry public API key (write-only). Empty disables telemetry.
	// +optional
	postHogPublicKey string,
) []*dagger.Container {
	artifacts := t.BuildRelease(ctx, version, commit, postHogPublicKey, "")

	platforms := []struct {
		platform dagger.Platform
		path     string
	}{
		{"linux/amd64", "linux/amd64/tapes"},
		{"linux/arm64", "linux/arm64/tapes"},
	}

	images := make([]*dagger.Container, 0, len(platforms))
	for _, p := range platforms {
		image := t.packageTapesImage(artifacts.File(p.path), p.platform)
		images = append(images, image)
	}

	return images
}

// packageTapesImage wraps a compiled tapes binary into a distroless runtime
// container tagged for the correct platform.
//
// gcr.io/distroless/base-debian12:nonroot provides glibc + CA certs without a
// shell. The :nonroot variant sets the default user to uid/gid 65532.
// No -static linking is required because glibc is present at runtime.
func (t *Tapes) packageTapesImage(binary *dagger.File, platform dagger.Platform) *dagger.Container {
	return dag.Container(dagger.ContainerOpts{Platform: platform}).
		From(runtimeImage).
		// Pre-create /data owned by nonroot so SQLite has a writable home.
		WithDirectory("/data", dag.Directory(), dagger.ContainerWithDirectoryOpts{
			Owner: "65532:65532",
		}).
		WithFile("/app/tapes", binary, dagger.ContainerWithFileOpts{
			Permissions: 0755,
			Owner:       "65532:65532",
		}).
		WithExposedPort(8080).
		WithEntrypoint([]string{"/app/tapes"})
}

// BuildPushImage builds a multi-arch image for tapes and publishes to the
// provided registry.
//
// Image naming convention: <registry>/tapes:<tag>
// For example: 123.dkr.ecr.us-east-1.amazonaws.com/paper/tapes:v1.0.0
func (t *Tapes) BuildPushImage(
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
	published := []string{}
	images := t.BuildImages(ctx, version, commit, postHogPublicKey)

	for _, tag := range tags {
		ref := fmt.Sprintf("%s/%s:%s", registry, imageName, tag)
		addr, err := dag.Container().
			Publish(ctx, ref, dagger.ContainerPublishOpts{
				PlatformVariants: images,
			})
		if err != nil {
			return published, fmt.Errorf("failed to publish %s: %w", ref, err)
		}
		published = append(published, addr)
	}

	return published, nil
}

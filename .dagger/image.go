package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"dagger/tapes/internal/dagger"
)

const (
	dockerfile = "dockerfiles/tapes.Dockerfile"
	imageName  = "tapes"
)

// BuildPushImage builds a multi-arch image for tapes
// using the existing Dockerfile and publishes to the provided registry.
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
) ([]string, error) {
	buildtime := time.Now()
	ldflags := strings.Join([]string{
		"-s",
		"-w",
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Version=%s'", version),
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Sha=%s'", commit),
		fmt.Sprintf("-X 'github.com/papercomputeco/tapes/pkg/utils.Buildtime=%s'", buildtime),
	}, " ")

	published := []string{}

	amd64 := t.Source.DockerBuild(dagger.DirectoryDockerBuildOpts{
		Dockerfile: dockerfile,
		Platform:   "linux/amd64",
		BuildArgs: []dagger.BuildArg{
			{Name: "LDFLAGS", Value: ldflags},
		},
	})
	arm64 := t.Source.DockerBuild(dagger.DirectoryDockerBuildOpts{
		Dockerfile: dockerfile,
		Platform:   "linux/arm64",
		BuildArgs: []dagger.BuildArg{
			{Name: "LDFLAGS", Value: ldflags},
		},
	})

	for _, tag := range tags {
		ref := fmt.Sprintf("%s/%s:%s", registry, imageName, tag)
		addr, err := dag.Container().
			Publish(ctx, ref, dagger.ContainerPublishOpts{
				PlatformVariants: []*dagger.Container{amd64, arm64},
			})
		if err != nil {
			return published, fmt.Errorf("failed to publish %s: %w", ref, err)
		}
		published = append(published, addr)
	}

	return published, nil
}

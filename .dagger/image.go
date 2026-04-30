package main

import (
	"context"
	"fmt"

	"dagger/tapes/internal/dagger"
)

const tapesImageName = "tapes"

var imagePlatforms = []dagger.Platform{
	"linux/amd64",
	"linux/arm64",
}

func (t *Tapes) buildDockerfileImage(dockerfile string, buildArgs []dagger.BuildArg) *dagger.Container {
	return t.Source.DockerBuild(dagger.DirectoryDockerBuildOpts{
		Dockerfile: dockerfile,
		BuildArgs:  buildArgs,
	})
}

func (t *Tapes) buildDockerfileImages(dockerfile string, buildArgs []dagger.BuildArg) []*dagger.Container {
	images := make([]*dagger.Container, 0, len(imagePlatforms))
	for _, platform := range imagePlatforms {
		image := t.Source.DockerBuild(dagger.DirectoryDockerBuildOpts{
			Dockerfile: dockerfile,
			Platform:   platform,
			BuildArgs:  buildArgs,
		})
		images = append(images, image)
	}

	return images
}

func (t *Tapes) publishImageVariants(
	ctx context.Context,
	registry string,
	imageName string,
	tags []string,
	images []*dagger.Container,
) ([]string, error) {
	published := []string{}

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

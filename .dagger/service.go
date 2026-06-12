package main

import (
	"context"
	"dagger/tapes/internal/dagger"
	"fmt"
)

type SvcBindingArgOption func(*dagger.Container, []string) (*dagger.Container, []string)

func WithPostgresSvc(s *dagger.Service) SvcBindingArgOption {
	return func(c *dagger.Container, args []string) (*dagger.Container, []string) {
		args = append(args, "--postgres")
		args = append(args, newPostgresDSN())

		return c.WithServiceBinding("postgres", s), args
	}
}

func WithOllamaSvc(s *dagger.Service) SvcBindingArgOption {
	return func(c *dagger.Container, args []string) (*dagger.Container, []string) {
		args = append(args, "--upstream")
		args = append(args, fmt.Sprintf("http://ollama:%d", OLLAMA_PORT))

		args = append(args, "--provider")
		args = append(args, "ollama")

		return c.WithServiceBinding("ollama", s), args
	}
}

func WithKafkaSvc(s *dagger.Service) SvcBindingArgOption {
	return func(c *dagger.Container, args []string) (*dagger.Container, []string) {
		args = append(args, "--kafka-brokers")
		args = append(args, fmt.Sprintf("kafka:%d", KAFKA_PORT))

		args = append(args, "--kafka-topic")
		args = append(args, kafkaE2ETopic)

		return c.WithServiceBinding("kafka", s), args
	}
}

func TapesSvc(ctx context.Context, tapesBase *dagger.Container, opts ...SvcBindingArgOption) *dagger.Service {
	args := []string{
		"/app/tapes", "serve",
	}

	for _, opt := range opts {
		tapesBase, args = opt(tapesBase, args)
	}

	tapesSvc := tapesBase.
		WithExposedPort(TAPES_API_DEFAULT_PORT).
		WithExposedPort(TAPES_PROXY_DEFAULT_PORT).
		AsService(dagger.ContainerAsServiceOpts{
			Args: args,
		})

	return tapesSvc
}

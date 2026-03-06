package main

import (
	"context"
	"fmt"

	"dagger/tapes/internal/dagger"
)

// TestE2E runs end-to-end tests against Postgres and Ollama services.
//
// It stands up a PostgreSQL database and an Ollama LLM service,
// builds the tapes binary, runs the proxy and API as Dagger services
// backed by Postgres, and uses hurl to verify the full pipeline.
func (t *Tapes) TestE2E(ctx context.Context) (string, error) {
	postgresSvc := t.PostgresService()
	ollamaSvc, err := t.OllamaStack(ctx)
	if err != nil {
		return "", fmt.Errorf("could not bring up ollama stack: %v", err)
	}

	// --- Build the tapes binary ---
	tapesBin := t.goContainer().
		WithServiceBinding("postgres", postgresSvc).
		WithExec([]string{"go", "build", "-o", "/usr/local/bin/tapes", "./cli/tapes/"}).
		File("/usr/local/bin/tapes")

	// Base container for running tapes services (needs the binary + service bindings).
	tapesBase := dag.Container().
		From("golang:1.25-bookworm").
		WithFile("/usr/local/bin/tapes", tapesBin).
		WithServiceBinding("postgres", postgresSvc).
		WithServiceBinding("ollama", ollamaSvc)

	// --- Run migrations once before starting services ---
	_, err = tapesBase.
		WithExec([]string{
			"tapes",
			"migrate",
			"--postgres", postgresDSN,
		}).
		Stdout(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to run migrations: %w", err)
	}

	// --- tapes proxy service ---
	proxySvc := tapesBase.
		WithExposedPort(8080).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "proxy",
				"--postgres", postgresDSN,
				"--upstream", fmt.Sprintf("http://ollama:%d", ollamaPort),
				"--provider", "ollama",
				"--listen", ":8080",
				"--vector-store-target", "",
				"--project", "e2e-test",
			},
		})

	// --- tapes API service ---
	apiSvc := tapesBase.
		WithExposedPort(8081).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "api",
				"--postgres", postgresDSN,
				"--listen", ":8081",
			},
		})

	// --- Test container ---
	// Use a Nix container with hurl pre-installed to avoid Debian apt
	// repository issues. The hurl package is pinned in the project flake.
	testCtr := dag.Container().
		From("nixos/nix:latest").
		WithExec([]string{"sh", "-c", "echo 'extra-experimental-features = nix-command flakes' >> /etc/nix/nix.conf"}).
		WithMountedCache("/nix/store-cache", dag.CacheVolume("nix-store")).
		WithExec([]string{"nix", "profile", "install", "nixpkgs#hurl", "nixpkgs#coreutils"}).
		WithWorkdir("/src").
		WithDirectory("/src", t.Source).
		WithServiceBinding("tapes-proxy", proxySvc).
		WithServiceBinding("tapes-api", apiSvc).

		// Run hurl e2e tests.
		WithExec([]string{"hurl", "--test", ".dagger/e2e/01-health.hurl"}).
		WithExec([]string{"hurl", "--test", "--very-verbose", ".dagger/e2e/02-chat-nonstreaming.hurl"}).

		// Brief pause for async worker pool to flush to Postgres.
		WithExec([]string{"sleep", "3"}).
		WithExec([]string{"hurl", "--test", ".dagger/e2e/03-verify-storage.hurl"}).
		WithExec([]string{"hurl", "--test", ".dagger/e2e/04-history.hurl"})

	return testCtr.Stdout(ctx)
}

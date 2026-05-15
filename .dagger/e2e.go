package main

import (
	"context"
	"fmt"
)

// TestE2E runs end-to-end tests against Postgres and Ollama services.
//
// It stands up a PostgreSQL database and an Ollama LLM service,
// builds the tapes binary, runs the proxy and API as Dagger services
// backed by Postgres, and uses hurl to verify the full pipeline.
func (t *Tapes) TestE2E(ctx context.Context) (string, error) {
	postgresSvc, err := t.PostgresStack(ctx)
	if err != nil {
		return "", fmt.Errorf("could not bring up postgres stack: %v", err)
	}

	ollamaSvc, err := t.OllamaStack(ctx)
	if err != nil {
		return "", fmt.Errorf("could not bring up ollama stack: %v", err)
	}

	tapesBase := t.BuildTapesDevImage(ctx)
	tapesProxySvc := TapesProxySvc(ctx, tapesBase, WithPostgresSvc(postgresSvc), WithOllamaSvc(ollamaSvc))
	tapesAPISvc := TapesAPISvc(ctx, tapesBase, WithPostgresSvc(postgresSvc))

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
		WithServiceBinding("tapes-proxy", tapesProxySvc).
		WithServiceBinding("tapes-api", tapesAPISvc).

		// Run hurl e2e tests.
		WithExec([]string{"hurl", "--test", ".dagger/e2e/01-health.hurl"}).
		WithExec([]string{"hurl", "--test", "--very-verbose", ".dagger/e2e/02-chat-nonstreaming.hurl"}).

		// Brief pause for async worker pool to flush to Postgres.
		WithExec([]string{"sleep", "3"}).
		WithExec([]string{"hurl", "--test", ".dagger/e2e/03-verify-storage.hurl"}).
		WithExec([]string{"hurl", "--test", ".dagger/e2e/04-history.hurl"})

	return testCtr.Stdout(ctx)
}

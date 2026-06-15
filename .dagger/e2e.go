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

	// --- tapes proxy service ---
	proxySvc := tapesBase.
		WithExposedPort(8080).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "proxy",
				"--postgres", newPostgresDSN(),
				"--upstream", fmt.Sprintf("http://ollama:%d", ollamaPort),
				"--provider", "ollama",
				"--listen", ":8080",
				"--vector-store-target", "",
				"--project", "e2e-test",
			},
		})

	// --- tapes API service ---
	// The embedding flags configure the search read path (query
	// embedding): they must match the model/dims the embed-spans
	// backfill writes with, or the span vector comparison fails.
	apiSvc := tapesBase.
		WithExposedPort(8081).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "api",
				"--postgres", newPostgresDSN(),
				"--listen", ":8081",
				"--embedding-provider", "ollama",
				"--embedding-target", fmt.Sprintf("http://ollama:%d", ollamaPort),
				"--embedding-model", ollamaEmbedModel,
				"--embedding-dimensions", ollamaEmbedDimensions,
			},
		})

	// --- tapes ingest service (sidecar capture path) ---
	// Feeds the raw layer the derive -> embed -> span-search legs
	// operate on; the proxy path above does not write raw turns.
	ingestSvc := tapesBase.
		WithExposedPort(8082).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"tapes", "serve", "ingest",
				"--postgres", newPostgresDSN(),
				"--listen", ":8082",
				"--project", "e2e-test",
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
		WithFile("/usr/local/bin/tapes", tapesBin).
		WithServiceBinding("tapes-proxy", proxySvc).
		WithServiceBinding("tapes-api", apiSvc).
		WithServiceBinding("tapes-ingest", ingestSvc).
		WithServiceBinding("postgres", postgresSvc).
		WithServiceBinding("ollama", ollamaSvc).

		// Run hurl e2e tests.
		WithExec([]string{"hurl", "--test", ".dagger/e2e/01-health.hurl"}).
		WithExec([]string{"hurl", "--test", "--very-verbose", ".dagger/e2e/02-chat-nonstreaming.hurl"}).

		// Brief pause for async worker pool to flush to Postgres.
		// 03 doubles as the proxy-leg persistence check: the legacy
		// node-layer stats fallback proves the captured chain landed
		// (the old 04-history leg read /v1/stems, which is gone).
		WithExec([]string{"sleep", "3"}).
		WithExec([]string{"hurl", "--test", ".dagger/e2e/03-verify-storage.hurl"}).

		// Span pipeline round trip: ingest a turn into the raw layer,
		// derive the span projection, embed eligible spans via Ollama
		// (the one-shot backfill the e2e backfill uses), then search.
		WithExec([]string{"hurl", "--test", "--very-verbose", ".dagger/e2e/05-ingest-turn.hurl"}).
		WithExec([]string{"sleep", "3"}).
		WithExec([]string{"hurl", "--test", ".dagger/e2e/06-derive-run.hurl"}).
		WithExec([]string{
			"/usr/local/bin/tapes", "dev", "embed-spans",
			"--postgres", newPostgresDSN(),
			"--embedding-provider", "ollama",
			"--embedding-target", fmt.Sprintf("http://ollama:%d", ollamaPort),
			"--embedding-model", ollamaEmbedModel,
			"--embedding-dimensions", ollamaEmbedDimensions,
		}).
		WithExec([]string{"hurl", "--test", "--very-verbose", ".dagger/e2e/07-search-spans.hurl"}).

		// Demo seed round trip: replay the bundled corpora through the
		// ingest write path, derive, and browse the result — proving
		// seeded data is indistinguishable from capture.
		WithExec([]string{"hurl", "--test", ".dagger/e2e/08-seed-demo.hurl"})

	return testCtr.Stdout(ctx)
}

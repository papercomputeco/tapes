package main

import (
	"context"
	"dagger/tapes/internal/dagger"
	"fmt"
)

const (
	OLLAMA_IMG = "ollama/ollama:0.24.0"

	// OLLAMA_PORT is the port the ollama service starts on
	OLLAMA_PORT = 11434

	// OLLAMA_MODEL is the small model pulled for e2e testing.
	OLLAMA_MODEL = "qwen3:0.6b"
)

func (t *Tapes) OllamaStack(ctx context.Context) (*dagger.Service, error) {
	ollamaSvc := OllamaService()

	// Start Ollama explicitly so we can pull the model before running tests.
	ollamaSvc, err := ollamaSvc.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start ollama service: %w", err)
	}

	// Pull the model using a sidecar container bound to the ollama service.
	_, err = ollamaPullModel(ctx, OLLAMA_MODEL, ollamaSvc)
	if err != nil {
		ollamaSvc.Stop(ctx)
		return nil, fmt.Errorf("failed to pull ollama model %s: %w", OLLAMA_MODEL, err)
	}

	return ollamaSvc, nil
}

// OllamaService provides an Ollama ready to run service.
// This service uses a cache volume so models are only pulled once across runs.
// Pre-create the models/manifests directory tree so Ollama's serve
// command doesn't crash on a fresh (empty) cache volume.
func OllamaService() *dagger.Service {
	return dag.Container().
		From(OLLAMA_IMG).
		WithMountedCache("/root/.ollama", dag.CacheVolume("ollama-models")).
		WithExec([]string{"mkdir", "-p", "/root/.ollama/models/manifests"}).
		WithExposedPort(OLLAMA_PORT).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

// ollamaPullModel pulls a given Ollama model in a sidecare container.
func ollamaPullModel(ctx context.Context, model string, ollamaSvc *dagger.Service) (string, error) {
	return dag.Container().
		From(OLLAMA_IMG).
		WithServiceBinding("ollama", ollamaSvc).
		WithEnvVariable("OLLAMA_HOST", fmt.Sprintf("http://ollama:%d", OLLAMA_PORT)).
		WithExec([]string{"ollama", "pull", model}).
		Stdout(ctx)
}

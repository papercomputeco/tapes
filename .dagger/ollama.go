package main

import (
	"context"
	"dagger/tapes/internal/dagger"
	"fmt"
)

const (
	ollamaPort = 11434

	// ollamaModel is the small model pulled for e2e testing.
	ollamaModel = "qwen3:0.6b"
)

func (t *Tapes) OllamaStack(ctx context.Context) (*dagger.Service, error) {
	ollamaSvc := t.OllamaService()

	// Start Ollama explicitly so we can pull the model before running tests.
	ollamaSvc, err := ollamaSvc.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start ollama service: %w", err)
	}

	// Pull the model using a sidecar container bound to the ollama service.
	_, err = t.OllamaPullModel(ctx, ollamaModel, ollamaSvc)
	if err != nil {
		return nil, fmt.Errorf("failed to pull ollama model %s: %w", ollamaModel, err)
	}

	return ollamaSvc, nil
}

// OllamaService provides an Ollama ready to run service.
// This service uses a cache volume so models are only pulled once across runs.
// Pre-create the models/manifests directory tree so Ollama's serve
// command doesn't crash on a fresh (empty) cache volume.
func (m *Tapes) OllamaService() *dagger.Service {
	return dag.Container().
		From("ollama/ollama:latest").
		WithMountedCache("/root/.ollama", dag.CacheVolume("ollama-models")).
		WithExec([]string{"mkdir", "-p", "/root/.ollama/models/manifests"}).
		WithExposedPort(ollamaPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

// ollamaPullModel pulls a given Ollama model in a sidecare container.
func (m *Tapes) OllamaPullModel(ctx context.Context, model string, ollamaSvc *dagger.Service) (string, error) {
	return dag.Container().
		From("ollama/ollama:latest").
		WithServiceBinding("ollama", ollamaSvc).
		WithEnvVariable("OLLAMA_HOST", fmt.Sprintf("http://ollama:%d", ollamaPort)).
		WithExec([]string{"ollama", "pull", model}).
		Stdout(ctx)
}

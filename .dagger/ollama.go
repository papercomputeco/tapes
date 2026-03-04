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

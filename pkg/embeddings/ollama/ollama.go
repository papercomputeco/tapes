// Package ollama implements pkg/embedding's Embedder client for Ollama's embedding APIs
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/vector"
)

const (
	// DefaultEmbeddingModel is the default model used for embeddings.
	DefaultEmbeddingModel = "nomic-embed-text"

	// DefaultBaseURL is the default Ollama API URL.
	DefaultBaseURL = "http://localhost:11434"
)

// Embedder wraps Ollama's embedding API.
type Embedder struct {
	baseURL    string
	model      string
	httpClient *http.Client
}

// EmbedderConfig holds configuration for the Ollama embedder.
type EmbedderConfig struct {
	// BaseURL is the Ollama API URL (e.g., "http://localhost:11434").
	// Defaults to DefaultBaseURL if empty.
	BaseURL string

	// Model is the embedding model to use (e.g., "nomic-embed-text", "all-minilm").
	// Defaults to DefaultEmbeddingModel if empty.
	Model string
}

// embedRequest is the request body for Ollama's embedding API.
type embedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// embedResponse is the response from Ollama's embedding API.
type embedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

// NewEmbedder creates a new embedder using Ollama's embedding API.
func NewEmbedder(cfg EmbedderConfig) (*Embedder, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}

	model := cfg.Model
	if model == "" {
		model = DefaultEmbeddingModel
	}

	return &Embedder{
		baseURL: baseURL,
		model:   model,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
	}, nil
}

// Embed converts text into a vector embedding.
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embedRequest{
		Model: e.model,
		Input: text,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("%w: marshaling request: %v", vector.ErrEmbedding, err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", e.baseURL+"/api/embed", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("%w: creating request: %v", vector.ErrEmbedding, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: sending request: %v", vector.ErrEmbedding, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: ollama returned status %d: %s", vector.ErrEmbedding, resp.StatusCode, string(body))
	}

	var embedResp embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("%w: decoding response: %v", vector.ErrEmbedding, err)
	}

	if len(embedResp.Embeddings) == 0 {
		return nil, fmt.Errorf("%w: no embeddings returned", vector.ErrEmbedding)
	}

	return embedResp.Embeddings[0], nil
}

// Close releases resources held by the embedder.
func (e *Embedder) Close() error {
	// HTTP client doesn't require explicit cleanup
	return nil
}

// Ensure Embedder implements vector.Embedder
var _ embeddings.Embedder = (*Embedder)(nil)

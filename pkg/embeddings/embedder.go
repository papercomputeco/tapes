// Package embeddings
package embeddings

import "context"

// Embedder provides text embedding capabilities.
type Embedder interface {
	// Embed converts text into a vector embedding.
	Embed(ctx context.Context, text string) ([]float32, error)

	// Close releases any resources held by the embedder.
	Close() error
}

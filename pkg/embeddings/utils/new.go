// Package embeddingutils is the embeddings utility package
package embeddingutils

import (
	"fmt"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/embeddings/ollama"
)

type NewEmbedderOpts struct {
	ProviderType string
	TargetURL    string
	Model        string
}

func NewEmbedder(o *NewEmbedderOpts) (embeddings.Embedder, error) {
	switch o.ProviderType {
	case "ollama":
		return ollama.NewEmbedder(ollama.EmbedderConfig{
			BaseURL: o.TargetURL,
			Model:   o.Model,
		})
	default:
		return nil, fmt.Errorf("unsupported embedding provider: %s", o.ProviderType)
	}
}

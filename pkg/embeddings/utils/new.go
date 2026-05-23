// Package embeddingutils is the embeddings utility package
package embeddingutils

import (
	"fmt"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/embeddings/ollama"
	"github.com/papercomputeco/tapes/pkg/embeddings/openai"
)

type NewEmbedderOpts struct {
	ProviderType string
	TargetURL    string
	Model        string
	Dimensions   uint
}

func NewEmbedder(o *NewEmbedderOpts) (embeddings.Embedder, error) {
	switch o.ProviderType {
	case "ollama":
		return ollama.NewEmbedder(ollama.EmbedderConfig{
			BaseURL: o.TargetURL,
			Model:   o.Model,
		})
	case "openai":
		return openai.NewEmbedder(openai.EmbedderConfig{
			BaseURL:    o.TargetURL,
			Model:      o.Model,
			Dimensions: o.Dimensions,
		})
	default:
		return nil, fmt.Errorf("unsupported embedding provider: %s", o.ProviderType)
	}
}

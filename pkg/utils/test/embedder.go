package testutils

import (
	"context"
	"fmt"
)

// MockEmbedder is a test embedder that returns predictable embeddings
type MockEmbedder struct {
	Embeddings map[string][]float32

	// FailOn causes Embed to return an error when the input text matches
	FailOn string
}

func NewMockEmbedder() *MockEmbedder {
	return &MockEmbedder{
		Embeddings: make(map[string][]float32),
	}
}

func (m *MockEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if m.FailOn != "" && text == m.FailOn {
		return nil, fmt.Errorf("mock embedding failure for: %s", text)
	}

	if emb, ok := m.Embeddings[text]; ok {
		return emb, nil
	}

	// Return a default embedding for any text
	return []float32{0.1, 0.2, 0.3}, nil
}

func (m *MockEmbedder) Close() error {
	return nil
}

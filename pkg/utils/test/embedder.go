package testutils

import "context"

// MockEmbedder is a test embedder that returns predictable embeddings
type MockEmbedder struct {
	embeddings map[string][]float32
}

func NewMockEmbedder() *MockEmbedder {
	return &MockEmbedder{
		embeddings: make(map[string][]float32),
	}
}

func (m *MockEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if emb, ok := m.embeddings[text]; ok {
		return emb, nil
	}
	// Return a default embedding for any text
	return []float32{0.1, 0.2, 0.3}, nil
}

func (m *MockEmbedder) Close() error {
	return nil
}

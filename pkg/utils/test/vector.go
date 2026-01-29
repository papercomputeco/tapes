package testutils

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/vector"
)

// MockVectorDriver is a test vector driver
type MockVectorDriver struct {
	documents []vector.Document
	results   []vector.QueryResult
}

func NewMockVectorDriver() *MockVectorDriver {
	return &MockVectorDriver{
		documents: make([]vector.Document, 0),
		results:   make([]vector.QueryResult, 0),
	}
}

func (m *MockVectorDriver) Add(_ context.Context, docs []vector.Document) error {
	m.documents = append(m.documents, docs...)
	return nil
}

func (m *MockVectorDriver) Query(_ context.Context, _ []float32, topK int) ([]vector.QueryResult, error) {
	if len(m.results) < topK {
		return m.results, nil
	}
	return m.results[:topK], nil
}

func (m *MockVectorDriver) Get(_ context.Context, _ []string) ([]vector.Document, error) {
	return m.documents, nil
}

func (m *MockVectorDriver) Delete(_ context.Context, _ []string) error {
	return nil
}

func (m *MockVectorDriver) Close() error {
	return nil
}

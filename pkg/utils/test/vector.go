package testutils

import (
	"context"
	"errors"

	"github.com/papercomputeco/tapes/pkg/vector"
)

// MockVectorDriver is a test vector driver
type MockVectorDriver struct {
	Documents []vector.Document
	Results   []vector.QueryResult

	// FailQuery causes Query to return an error
	FailQuery bool
}

func NewMockVectorDriver() *MockVectorDriver {
	return &MockVectorDriver{
		Documents: make([]vector.Document, 0),
		Results:   make([]vector.QueryResult, 0),
	}
}

func (m *MockVectorDriver) Add(_ context.Context, docs []vector.Document) error {
	m.Documents = append(m.Documents, docs...)
	return nil
}

func (m *MockVectorDriver) Query(_ context.Context, _ []float32, topK int) ([]vector.QueryResult, error) {
	if m.FailQuery {
		return nil, errors.New("mock vector query failure")
	}

	if len(m.Results) < topK {
		return m.Results, nil
	}

	return m.Results[:topK], nil
}

func (m *MockVectorDriver) Get(_ context.Context, _ []string) ([]vector.Document, error) {
	return m.Documents, nil
}

func (m *MockVectorDriver) Delete(_ context.Context, _ []string) error {
	return nil
}

func (m *MockVectorDriver) Close() error {
	return nil
}

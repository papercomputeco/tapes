package testutils

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/memory"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// MockMemoryDriver is a test memory driver that records calls and returns
// configurable results.
type MockMemoryDriver struct {
	// StoredNodes accumulates all nodes passed to Store.
	StoredNodes []*merkle.Node

	// RecallResults is returned by Recall for any hash.
	RecallResults []memory.Fact

	// FailStore causes Store to return an error.
	FailStore bool

	// FailRecall causes Recall to return an error.
	FailRecall bool
}

// NewMockMemoryDriver creates a new mock memory driver.
func NewMockMemoryDriver() *MockMemoryDriver {
	return &MockMemoryDriver{
		StoredNodes:   make([]*merkle.Node, 0),
		RecallResults: make([]memory.Fact, 0),
	}
}

func (m *MockMemoryDriver) Store(_ context.Context, nodes []*merkle.Node) error {
	if m.FailStore {
		return memory.ErrNotConfigured
	}
	m.StoredNodes = append(m.StoredNodes, nodes...)
	return nil
}

func (m *MockMemoryDriver) Recall(_ context.Context, _ string) ([]memory.Fact, error) {
	if m.FailRecall {
		return nil, memory.ErrNotConfigured
	}
	return m.RecallResults, nil
}

func (m *MockMemoryDriver) Close() error {
	return nil
}

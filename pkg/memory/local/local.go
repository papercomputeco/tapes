// Package local provides an in-memory implementation of the memory.Driver interface.
//
// Facts are extracted from stored nodes and keyed by their originating node
// hash. Recall returns all facts associated with a given hash. This is a
// simple local-dev story — production backends (e.g., Cognee) will use
// sophisticated ML pipelines to extract and recall cross-branch knowledge.
package local

import (
	"context"
	"sync"

	"github.com/papercomputeco/tapes/pkg/memory"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// Config holds configuration for the local memory driver.
type Config struct {
	// Enabled controls whether the driver stores and recalls facts.
	// When false, Store is a no-op and Recall returns nil.
	Enabled bool
}

// Driver implements memory.Driver using in-process data structures.
type Driver struct {
	config Config

	mu sync.RWMutex

	// facts maps node hash -> extracted facts from that node.
	facts map[string][]memory.Fact
}

// NewDriver creates a local in-memory memory driver.
func NewDriver(config Config) *Driver {
	return &Driver{
		config: config,
		facts:  make(map[string][]memory.Fact),
	}
}

// Store extracts text content from nodes and persists them as facts.
// Each node's text content becomes a fact keyed by the node's hash.
func (d *Driver) Store(_ context.Context, nodes []*merkle.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	if !d.config.Enabled {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for _, node := range nodes {
		text := node.Bucket.ExtractText()
		if text == "" {
			continue
		}

		d.facts[node.Hash] = append(d.facts[node.Hash], memory.Fact{
			Content: text,
		})
	}

	return nil
}

// Recall retrieves facts associated with the given node hash.
// Returns nil if no facts exist for the hash or if the driver is disabled.
func (d *Driver) Recall(_ context.Context, hash string) ([]memory.Fact, error) {
	if !d.config.Enabled {
		return nil, nil
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	facts, ok := d.facts[hash]
	if !ok {
		return nil, nil
	}

	// Return a copy to avoid callers mutating internal state.
	result := make([]memory.Fact, len(facts))
	copy(result, facts)

	return result, nil
}

// Close is a no-op for the in-memory driver.
func (d *Driver) Close() error {
	return nil
}

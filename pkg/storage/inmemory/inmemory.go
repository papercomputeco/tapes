package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Driver implements Storer using an in-memory map.
type Driver struct {
	// mu is a read write sync mutex for locking the mapping of nodes
	mu sync.RWMutex

	// nodes is the in memory map of nodes where the key is the content-addressed
	// hash for the node
	nodes map[string]*merkle.Node
}

// NewDriver creates a new in-memory storer.
func NewDriver() *Driver {
	return &Driver{
		nodes: make(map[string]*merkle.Node),
	}
}

// Put stores a node. Returns true if the node was newly inserted,
// false if it already existed (no-op due to content-addressing).
func (s *Driver) Put(_ context.Context, node *merkle.Node) (bool, error) {
	if node == nil {
		return false, errors.New("cannot store nil node")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotent insert - deduplication via content-addressing
	_, ok := s.nodes[node.Hash]
	if ok {
		return false, nil
	}

	s.nodes[node.Hash] = node
	return true, nil
}

// Get retrieves a node by its hash.
func (s *Driver) Get(_ context.Context, hash string) (*merkle.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node, ok := s.nodes[hash]
	if !ok {
		return nil, storage.NotFoundError{Hash: hash}
	}

	return node, nil
}

// Has checks if a node exists by its hash.
func (s *Driver) Has(_ context.Context, hash string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.nodes[hash]
	return ok, nil
}

// GetByParent retrieves all nodes that have the provided parent.
// This is useful for determining where branching occurs.
func (s *Driver) GetByParent(_ context.Context, parentHash *string) ([]*merkle.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*merkle.Node
	for _, node := range s.nodes {
		if parentHash == nil {
			if node.ParentHash == nil {
				result = append(result, node)
			}
		} else {
			if node.ParentHash != nil && *node.ParentHash == *parentHash {
				result = append(result, node)
			}
		}
	}
	return result, nil
}

// List returns all nodes in the store.
func (s *Driver) List(_ context.Context) ([]*merkle.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]*merkle.Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// Roots returns all root nodes
func (s *Driver) Roots(ctx context.Context) ([]*merkle.Node, error) {
	return s.GetByParent(ctx, nil)
}

// Leaves returns all leaf nodes
func (s *Driver) Leaves(_ context.Context) ([]*merkle.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build a set of all parent hashes
	hasChildren := make(map[string]bool)
	for _, node := range s.nodes {
		if node.ParentHash != nil {
			hasChildren[*node.ParentHash] = true
		}
	}

	// Find nodes that are not parents of any other node
	var leaves []*merkle.Node
	for _, node := range s.nodes {
		if !hasChildren[node.Hash] {
			leaves = append(leaves, node)
		}
	}

	return leaves, nil
}

// Ancestry returns the path from a node back to its root (node first, root last).
func (s *Driver) Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error) {
	var path []*merkle.Node
	current := hash

	for {
		node, err := s.Get(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("getting node %s: %w", current, err)
		}
		path = append(path, node)

		if node.ParentHash == nil {
			break
		}
		current = *node.ParentHash
	}

	return path, nil
}

// Depth returns the depth of a node (0 for roots).
func (s *Driver) Depth(ctx context.Context, hash string) (int, error) {
	depth := 0
	current := hash

	for {
		node, err := s.Get(ctx, current)
		if err != nil {
			return 0, err
		}
		if node.ParentHash == nil {
			break
		}
		depth++
		current = *node.ParentHash
	}

	return depth, nil
}

// Count returns the number of nodes in the in-memory store.
func (s *Driver) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.nodes)
}

// Close is a no-op for the in-memory storer.
func (s *Driver) Close() error {
	return nil
}

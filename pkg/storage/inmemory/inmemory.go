package inmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// InMemoryStorer implements Storer using an in-memory map.
type InMemoryStorer struct {
	// mu is a read write sync mutex for locking the mapping of nodes
	mu sync.RWMutex

	// nodes is the in memory map of nodes where the key is the content-addressed
	// hash for the node
	nodes map[string]*merkle.Node
}

// NewInMemoryStorer creates a new in-memory storer.
func NewInMemoryStorer() *InMemoryStorer {
	return &InMemoryStorer{
		nodes: make(map[string]*merkle.Node),
	}
}

// Put stores a node. If the node already exists (by hash), this is a no-op.
func (s *InMemoryStorer) Put(ctx context.Context, node *merkle.Node) error {
	if node == nil {
		return fmt.Errorf("cannot store nil node")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotent insert - deduplication via content-addressing
	_, ok := s.nodes[node.Hash]
	if !ok {
		s.nodes[node.Hash] = node
	}

	return nil
}

// Get retrieves a node by its hash.
func (s *InMemoryStorer) Get(ctx context.Context, hash string) (*merkle.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node, ok := s.nodes[hash]
	if !ok {
		return nil, storage.ErrNotFound{Hash: hash}
	}

	return node, nil
}

// Has checks if a node exists by its hash.
func (s *InMemoryStorer) Has(ctx context.Context, hash string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.nodes[hash]
	return ok, nil
}

// GetByParent retrieves all nodes that have the provided parent.
// This is useful for determining where branching occurs.
func (s *InMemoryStorer) GetByParent(ctx context.Context, parentHash *string) ([]*merkle.Node, error) {
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
func (s *InMemoryStorer) List(ctx context.Context) ([]*merkle.Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]*merkle.Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// Roots returns all root nodes
func (s *InMemoryStorer) Roots(ctx context.Context) ([]*merkle.Node, error) {
	return s.GetByParent(ctx, nil)
}

// Leaves returns all leaf nodes
func (s *InMemoryStorer) Leaves(ctx context.Context) ([]*merkle.Node, error) {
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
func (s *InMemoryStorer) Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error) {
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

// Descendants returns the path from root to node (root first, node last).
func (s *InMemoryStorer) Descendants(ctx context.Context, hash string) ([]*merkle.Node, error) {
	path, err := s.Ancestry(ctx, hash)
	if err != nil {
		return nil, err
	}

	// Reverse the path
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return path, nil
}

// Depth returns the depth of a node (0 for roots).
func (s *InMemoryStorer) Depth(ctx context.Context, hash string) (int, error) {
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
func (s *InMemoryStorer) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.nodes)
}

// Close is a no-op for the in-memory storer.
func (s *InMemoryStorer) Close() error {
	return nil
}

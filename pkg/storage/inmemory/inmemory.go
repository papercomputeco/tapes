package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
//
// Put stores a copy of the node so that storage-managed metadata
// (currently CreatedAt) can be assigned without mutating the caller.
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

	stored := *node
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = time.Now().UTC()
	}
	s.nodes[node.Hash] = &stored
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

// Ancestry returns the path from a node back to its root (node first, root last).
// See AncestryChain for a variant that also signals when the walk stopped at
// a missing parent.
func (s *Driver) Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error) {
	chain, err := s.AncestryChain(ctx, hash)
	if err != nil {
		return nil, err
	}
	return chain.Nodes, nil
}

// AncestryChain walks the parent chain starting at hash and returns a Chain
// describing whether the walk reached a real root, stopped at a parent that
// is not present in this store, or was guarded out of a cycle.
func (s *Driver) AncestryChain(ctx context.Context, hash string) (*storage.Chain, error) {
	node, err := s.Get(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting node %s: %w", hash, err)
	}

	seen := map[string]struct{}{node.Hash: {}}
	chain := &storage.Chain{Nodes: []*merkle.Node{node}}
	for {
		if node.ParentHash == nil || *node.ParentHash == "" {
			return chain, nil
		}
		if _, loop := seen[*node.ParentHash]; loop {
			chain.Incomplete = true
			chain.CycleDetected = true
			return chain, nil
		}
		parent, err := s.Get(ctx, *node.ParentHash)
		if err != nil {
			var notFound storage.NotFoundError
			if errors.As(err, &notFound) {
				chain.Incomplete = true
				chain.MissingParent = *node.ParentHash
				return chain, nil
			}
			return nil, fmt.Errorf("getting node %s: %w", *node.ParentHash, err)
		}
		seen[parent.Hash] = struct{}{}
		chain.Nodes = append(chain.Nodes, parent)
		node = parent
	}
}

// AncestryChains walks each input hash's ancestry and returns a Chain per
// starting hash. The in-memory driver has O(1) Get, so the batched ent
// fast path offers no benefit here — this is a straightforward loop over
// AncestryChain.
func (s *Driver) AncestryChains(ctx context.Context, hashes []string) (map[string]*storage.Chain, error) {
	out := make(map[string]*storage.Chain, len(hashes))
	seen := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		chain, err := s.AncestryChain(ctx, h)
		if err != nil {
			var notFound storage.NotFoundError
			if errors.As(err, &notFound) {
				continue
			}
			return nil, err
		}
		out[h] = chain
	}
	return out, nil
}

// LoadDag takes a node hash and returns the branch containing that node:
// its ancestry up to the root and all descendants reachable from that node.
func (s *Driver) LoadDag(ctx context.Context, hash string) (*merkle.Dag, error) {
	dag := merkle.NewDag()

	ancestry, err := s.Ancestry(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting ancestry for %s: %w", hash, err)
	}
	if len(ancestry) == 0 {
		return nil, storage.NotFoundError{Hash: hash}
	}

	for i := len(ancestry) - 1; i >= 0; i-- {
		if _, err := dag.AddNode(ancestry[i]); err != nil {
			return nil, fmt.Errorf("adding ancestor node %s: %w", ancestry[i].Hash, err)
		}
	}

	seen := map[string]struct{}{hash: {}}
	var addDescendants func(string) error
	addDescendants = func(parentHash string) error {
		children, err := s.GetByParent(ctx, &parentHash)
		if err != nil {
			return fmt.Errorf("getting children of %s: %w", parentHash, err)
		}
		for _, child := range children {
			if _, err := dag.AddNode(child); err != nil {
				return fmt.Errorf("adding child node %s: %w", child.Hash, err)
			}
			if _, ok := seen[child.Hash]; ok {
				continue
			}
			seen[child.Hash] = struct{}{}
			if err := addDescendants(child.Hash); err != nil {
				return err
			}
		}
		return nil
	}

	if err := addDescendants(hash); err != nil {
		return nil, err
	}

	return dag, nil
}

// Open is a no-op for the in-memory storer.
func (s *Driver) Open(_ context.Context) error {
	return nil
}

// Close is a no-op for the in-memory storer.
func (s *Driver) Close() error {
	return nil
}

package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
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

// ListSessions returns a page of leaf nodes (sessions), ordered by created_at
// descending then hash descending, optionally filtered by opts.
func (s *Driver) ListSessions(_ context.Context, opts storage.ListOpts) (*storage.Page[*merkle.Node], error) {
	opts = opts.Normalize()

	cursor, err := storage.DecodeCursor(opts.Cursor)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	hasChildren := s.computeHasChildren()

	var matches []*merkle.Node
	for _, node := range s.nodes {
		if hasChildren[node.Hash] {
			continue
		}
		if !matchesFilter(node, opts) {
			continue
		}
		if opts.Cursor != "" && !beforeCursor(node, cursor) {
			continue
		}
		matches = append(matches, node)
	}

	sort.Slice(matches, func(i, j int) bool {
		if !matches[i].CreatedAt.Equal(matches[j].CreatedAt) {
			return matches[i].CreatedAt.After(matches[j].CreatedAt)
		}
		return matches[i].Hash > matches[j].Hash
	})

	hasMore := len(matches) > opts.Limit
	if hasMore {
		matches = matches[:opts.Limit]
	}

	page := &storage.Page[*merkle.Node]{Items: matches}
	if hasMore && len(matches) > 0 {
		last := matches[len(matches)-1]
		page.NextCursor = storage.Cursor{
			CreatedAt: last.CreatedAt,
			Hash:      last.Hash,
		}.Encode()
	}
	return page, nil
}

// CountSessions returns aggregate counts for the slice of data matching opts.
// Pagination fields on opts are ignored.
func (s *Driver) CountSessions(_ context.Context, opts storage.ListOpts) (storage.SessionStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hasChildren := s.computeHasChildren()

	var stats storage.SessionStats
	for _, node := range s.nodes {
		if !matchesFilter(node, opts) {
			continue
		}
		stats.TurnCount++
		if !hasChildren[node.Hash] {
			stats.SessionCount++
		}
		if node.ParentHash == nil || *node.ParentHash == "" {
			stats.RootCount++
		}
	}
	return stats, nil
}

// computeHasChildren builds a set of node hashes that are referenced as a
// parent by some other node. Caller must hold s.mu.
func (s *Driver) computeHasChildren() map[string]bool {
	hasChildren := make(map[string]bool, len(s.nodes))
	for _, node := range s.nodes {
		if node.ParentHash != nil {
			hasChildren[*node.ParentHash] = true
		}
	}
	return hasChildren
}

// matchesFilter reports whether n matches the per-field filters in opts.
// Pagination fields are ignored here.
func matchesFilter(n *merkle.Node, opts storage.ListOpts) bool {
	if opts.Project != "" && n.Project != opts.Project {
		return false
	}
	if opts.Agent != "" && n.Bucket.AgentName != opts.Agent {
		return false
	}
	if opts.Model != "" && n.Bucket.Model != opts.Model {
		return false
	}
	if opts.Provider != "" && n.Bucket.Provider != opts.Provider {
		return false
	}
	if opts.Since != nil && n.CreatedAt.Before(*opts.Since) {
		return false
	}
	if opts.Until != nil && !n.CreatedAt.Before(*opts.Until) {
		return false
	}
	return true
}

// beforeCursor reports whether n comes strictly after the cursor in
// (CreatedAt DESC, Hash DESC) order — that is, n should appear on a later page.
func beforeCursor(n *merkle.Node, c storage.Cursor) bool {
	if n.CreatedAt.Before(c.CreatedAt) {
		return true
	}
	if n.CreatedAt.Equal(c.CreatedAt) && n.Hash < c.Hash {
		return true
	}
	return false
}

// Migrate is a no-op for the in-memory storer.
func (s *Driver) Migrate(_ context.Context) error {
	return nil
}

// Close is a no-op for the in-memory storer.
func (s *Driver) Close() error {
	return nil
}

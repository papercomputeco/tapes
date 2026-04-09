// Package entdriver
package entdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
	"github.com/papercomputeco/tapes/pkg/storage/ent/node"
)

// EntDriver provides storage operations using an ent client.
// It is database-agnostic and can be embedded by specific drivers.
type EntDriver struct {
	Client *ent.Client
}

// Put stores a node. Returns true if the node was newly inserted,
// false if it already existed. This is a no-op due to content-addressing.
func (ed *EntDriver) Put(ctx context.Context, n *merkle.Node) (bool, error) {
	if n == nil {
		return false, errors.New("cannot store nil node")
	}

	// Check if node already exists (idempotent insert)
	exists, err := ed.Client.Node.Query().
		Where(node.ID(n.Hash)).
		Exist(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}
	if exists {
		return false, nil
	}

	create := ed.Client.Node.Create().
		SetID(n.Hash).
		SetNillableParentHash(n.ParentHash).
		SetType(n.Bucket.Type).
		SetRole(n.Bucket.Role).
		SetModel(n.Bucket.Model).
		SetProvider(n.Bucket.Provider).
		SetStopReason(n.StopReason)

	if n.Project != "" {
		create.SetProject(n.Project)
	}

	if n.Bucket.AgentName != "" {
		create.SetAgentName(n.Bucket.AgentName)
	}

	// Honor an explicit CreatedAt when supplied (e.g. by tests). When zero,
	// the schema default (CURRENT_TIMESTAMP) applies.
	if !n.CreatedAt.IsZero() {
		create.SetCreatedAt(n.CreatedAt)
	}

	// Marshal bucket to JSON for storage
	bucketJSON, err := json.Marshal(n.Bucket)
	if err != nil {
		return false, fmt.Errorf("failed to marshal bucket: %w", err)
	}
	var bucketMap map[string]any
	if err := json.Unmarshal(bucketJSON, &bucketMap); err != nil {
		return false, fmt.Errorf("failed to unmarshal bucket to map: %w", err)
	}
	create.SetBucket(bucketMap)

	// Marshal content blocks
	contentJSON, err := json.Marshal(n.Bucket.Content)
	if err != nil {
		return false, fmt.Errorf("failed to marshal content: %w", err)
	}
	var contentSlice []map[string]any
	if err := json.Unmarshal(contentJSON, &contentSlice); err != nil {
		return false, fmt.Errorf("failed to unmarshal content to slice: %w", err)
	}
	create.SetContent(contentSlice)

	// Set usage fields if available
	if n.Usage != nil {
		if n.Usage.PromptTokens > 0 {
			create.SetPromptTokens(n.Usage.PromptTokens)
		}
		if n.Usage.CompletionTokens > 0 {
			create.SetCompletionTokens(n.Usage.CompletionTokens)
		}
		if n.Usage.TotalTokens > 0 {
			create.SetTotalTokens(n.Usage.TotalTokens)
		}
		if n.Usage.CacheCreationInputTokens > 0 {
			create.SetCacheCreationInputTokens(n.Usage.CacheCreationInputTokens)
		}
		if n.Usage.CacheReadInputTokens > 0 {
			create.SetCacheReadInputTokens(n.Usage.CacheReadInputTokens)
		}
		if n.Usage.TotalDurationNs > 0 {
			create.SetTotalDurationNs(n.Usage.TotalDurationNs)
		}
		if n.Usage.PromptDurationNs > 0 {
			create.SetPromptDurationNs(n.Usage.PromptDurationNs)
		}
	}

	err = create.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("could not execute node creation: %w", err)
	}

	return true, nil
}

// Get retrieves a node by its hash.
func (ed *EntDriver) Get(ctx context.Context, hash string) (*merkle.Node, error) {
	entNode, err := ed.Client.Node.Get(ctx, hash)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, storage.NotFoundError{Hash: hash}
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}
	return ed.entNodeToMerkleNode(entNode)
}

// Has checks if a node exists by its hash.
func (ed *EntDriver) Has(ctx context.Context, hash string) (bool, error) {
	return ed.Client.Node.Query().
		Where(node.ID(hash)).
		Exist(ctx)
}

// GetByParent retrieves all nodes that have the given parent hash.
// Uses the children edge for efficient lookups.
func (ed *EntDriver) GetByParent(ctx context.Context, parentHash *string) ([]*merkle.Node, error) {
	var entNodes []*ent.Node
	var err error

	if parentHash == nil {
		// Root nodes have no parent
		entNodes, err = ed.Client.Node.Query().
			Where(node.ParentHashIsNil()).
			All(ctx)
	} else {
		// Use the edge to find children
		entNodes, err = ed.Client.Node.Query().
			Where(node.ID(*parentHash)).
			QueryChildren().
			All(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}
	return ed.entNodesToMerkleNodes(entNodes)
}

// List returns all nodes in the store.
func (ed *EntDriver) List(ctx context.Context) ([]*merkle.Node, error) {
	entNodes, err := ed.Client.Node.Query().
		Order(ent.Asc(node.FieldCreatedAt)).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes: %w", err)
	}
	return ed.entNodesToMerkleNodes(entNodes)
}

// Roots returns all root nodes (nodes with no parent).
func (ed *EntDriver) Roots(ctx context.Context) ([]*merkle.Node, error) {
	return ed.GetByParent(ctx, nil)
}

// Leaves returns all leaf nodes (nodes with no children).
// Uses the children edge for efficient detection.
func (ed *EntDriver) Leaves(ctx context.Context) ([]*merkle.Node, error) {
	entNodes, err := ed.Client.Node.Query().
		Where(node.Not(node.HasChildren())).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query leaves: %w", err)
	}
	return ed.entNodesToMerkleNodes(entNodes)
}

// Ancestry returns the path from a node back to its root (node first, root last).
// Uses the parent edge for traversal.
func (ed *EntDriver) Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error) {
	var path []*merkle.Node

	current, err := ed.Client.Node.Get(ctx, hash)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, storage.NotFoundError{Hash: hash}
		}
		return nil, fmt.Errorf("failed to get node: %w", err)
	}

	for current != nil {
		n, err := ed.entNodeToMerkleNode(current)
		if err != nil {
			return nil, err
		}
		path = append(path, n)

		// Use the parent edge to traverse up
		parent, err := current.QueryParent().Only(ctx)
		if ent.IsNotFound(err) {
			break // Reached root
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query parent: %w", err)
		}
		current = parent
	}

	return path, nil
}

// BulkAncestries returns chronological (root-first) ancestry chains for each
// of the given leaf hashes in a single batched walk. Instead of N individual
// per-leaf walks (each of which is itself O(depth) queries), this issues one
// IN-query per depth level across the full leaf set. Result shape mirrors
// Ancestry (root-first, so callers do not need to reverse).
//
// Missing hashes are silently skipped. Cycles are defended against via the
// seen set.
func (ed *EntDriver) BulkAncestries(ctx context.Context, leafHashes []string) (map[string][]*merkle.Node, error) {
	if len(leafHashes) == 0 {
		return map[string][]*merkle.Node{}, nil
	}

	// nodesByID accumulates every node we load across all depth levels.
	nodesByID := make(map[string]*merkle.Node, len(leafHashes)*8)

	// frontier is the set of hashes whose parents we still need to fetch.
	// Seeded with the leaves, then replaced each iteration with the unique
	// set of parent hashes we just discovered.
	frontier := dedupeStrings(leafHashes)

	for len(frontier) > 0 {
		entNodes, err := ed.Client.Node.Query().
			Where(node.IDIn(frontier...)).
			All(ctx)
		if err != nil {
			return nil, fmt.Errorf("bulk ancestries fetch: %w", err)
		}

		nextFrontier := make([]string, 0, len(entNodes))
		for _, en := range entNodes {
			if _, ok := nodesByID[en.ID]; ok {
				continue
			}
			mn, err := ed.entNodeToMerkleNode(en)
			if err != nil {
				return nil, err
			}
			nodesByID[en.ID] = mn
			if mn.ParentHash != nil && *mn.ParentHash != "" {
				if _, already := nodesByID[*mn.ParentHash]; !already {
					nextFrontier = append(nextFrontier, *mn.ParentHash)
				}
			}
		}
		frontier = dedupeStrings(nextFrontier)
	}

	// Build each leaf's chain by walking parent pointers in the loaded map.
	result := make(map[string][]*merkle.Node, len(leafHashes))
	for _, leafHash := range leafHashes {
		leaf, ok := nodesByID[leafHash]
		if !ok {
			continue
		}
		chain := make([]*merkle.Node, 0, 8)
		seen := make(map[string]bool, 8)
		current := leaf
		for current != nil {
			if seen[current.Hash] {
				break
			}
			seen[current.Hash] = true
			chain = append(chain, current)
			if current.ParentHash == nil || *current.ParentHash == "" {
				break
			}
			current = nodesByID[*current.ParentHash]
		}
		// Reverse in place to root-first.
		for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
			chain[i], chain[j] = chain[j], chain[i]
		}
		result[leafHash] = chain
	}
	return result, nil
}

// dedupeStrings returns a deduped copy of in, preserving first-seen order.
func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// Depth returns the depth of a node (0 for roots).
func (ed *EntDriver) Depth(ctx context.Context, hash string) (int, error) {
	path, err := ed.Ancestry(ctx, hash)
	if err != nil {
		return 0, err
	}
	return len(path) - 1, nil
}

// UpdateUsage updates only the token usage fields on an existing node by hash.
func (ed *EntDriver) UpdateUsage(ctx context.Context, hash string, usage *llm.Usage) error {
	if usage == nil {
		return errors.New("cannot update with nil usage")
	}

	update := ed.Client.Node.UpdateOneID(hash)

	if usage.PromptTokens > 0 {
		update.SetPromptTokens(usage.PromptTokens)
	}
	if usage.CompletionTokens > 0 {
		update.SetCompletionTokens(usage.CompletionTokens)
	}
	if usage.TotalTokens > 0 {
		update.SetTotalTokens(usage.TotalTokens)
	}
	if usage.CacheCreationInputTokens > 0 {
		update.SetCacheCreationInputTokens(usage.CacheCreationInputTokens)
	}
	if usage.CacheReadInputTokens > 0 {
		update.SetCacheReadInputTokens(usage.CacheReadInputTokens)
	}

	return update.Exec(ctx)
}

// Close closes the database connection.
func (ed *EntDriver) Close() error {
	return ed.Client.Close()
}

// Conversion helpers
func (ed *EntDriver) entNodeToMerkleNode(entNode *ent.Node) (*merkle.Node, error) {
	// Unmarshal the bucket JSON back to merkle.Bucket
	bucketJSON, err := json.Marshal(entNode.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bucket map: %w", err)
	}

	var bucket merkle.Bucket
	if err := json.Unmarshal(bucketJSON, &bucket); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bucket: %w", err)
	}

	node := &merkle.Node{
		Hash:       entNode.ID,
		ParentHash: entNode.ParentHash,
		Bucket:     bucket,
		StopReason: entNode.StopReason,
		CreatedAt:  entNode.CreatedAt,
	}

	if entNode.Project != nil {
		node.Project = *entNode.Project
	}

	// Rebuild usage metrics if they exist.
	if entNode.PromptTokens != nil ||
		entNode.CompletionTokens != nil ||
		entNode.TotalTokens != nil ||
		entNode.CacheCreationInputTokens != nil ||
		entNode.CacheReadInputTokens != nil ||
		entNode.TotalDurationNs != nil ||
		entNode.PromptDurationNs != nil {
		node.Usage = &llm.Usage{}

		if entNode.PromptTokens != nil {
			node.Usage.PromptTokens = *entNode.PromptTokens
		}

		if entNode.CompletionTokens != nil {
			node.Usage.CompletionTokens = *entNode.CompletionTokens
		}

		if entNode.TotalTokens != nil {
			node.Usage.TotalTokens = *entNode.TotalTokens
		}

		if entNode.CacheCreationInputTokens != nil {
			node.Usage.CacheCreationInputTokens = *entNode.CacheCreationInputTokens
		}

		if entNode.CacheReadInputTokens != nil {
			node.Usage.CacheReadInputTokens = *entNode.CacheReadInputTokens
		}

		if entNode.TotalDurationNs != nil {
			node.Usage.TotalDurationNs = *entNode.TotalDurationNs
		}

		if entNode.PromptDurationNs != nil {
			node.Usage.PromptDurationNs = *entNode.PromptDurationNs
		}
	}

	return node, nil
}

func (ed *EntDriver) entNodesToMerkleNodes(entNodes []*ent.Node) ([]*merkle.Node, error) {
	nodes := make([]*merkle.Node, 0, len(entNodes))
	for _, entNode := range entNodes {
		n, err := ed.entNodeToMerkleNode(entNode)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

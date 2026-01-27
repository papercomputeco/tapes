// Package entdriver
package entdriver

import (
	"context"
	"encoding/json"
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

// Put stores a node. If the node already exists (by hash), this is a no-op.
func (ed *EntDriver) Put(ctx context.Context, n *merkle.Node) error {
	if n == nil {
		return fmt.Errorf("cannot store nil node")
	}

	// Check if node already exists (idempotent insert)
	exists, err := ed.Client.Node.Query().
		Where(node.ID(n.Hash)).
		Exist(ctx)
	if err != nil {
		return fmt.Errorf("failed to check existence: %w", err)
	}
	if exists {
		return nil
	}

	create := ed.Client.Node.Create().
		SetID(n.Hash).
		SetNillableParentHash(n.ParentHash).
		SetType(n.Bucket.Type).
		SetRole(n.Bucket.Role).
		SetModel(n.Bucket.Model).
		SetProvider(n.Bucket.Provider).
		SetStopReason(n.Bucket.StopReason)

	// Marshal bucket to JSON for storage
	bucketJSON, err := json.Marshal(n.Bucket)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket: %w", err)
	}
	var bucketMap map[string]any
	if err := json.Unmarshal(bucketJSON, &bucketMap); err != nil {
		return fmt.Errorf("failed to unmarshal bucket to map: %w", err)
	}
	create.SetBucket(bucketMap)

	// Marshal content blocks
	contentJSON, err := json.Marshal(n.Bucket.Content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %w", err)
	}
	var contentSlice []map[string]any
	if err := json.Unmarshal(contentJSON, &contentSlice); err != nil {
		return fmt.Errorf("failed to unmarshal content to slice: %w", err)
	}
	create.SetContent(contentSlice)

	// Set usage fields if available
	if n.Bucket.Usage != nil {
		if n.Bucket.Usage.PromptTokens > 0 {
			create.SetPromptTokens(n.Bucket.Usage.PromptTokens)
		}
		if n.Bucket.Usage.CompletionTokens > 0 {
			create.SetCompletionTokens(n.Bucket.Usage.CompletionTokens)
		}
		if n.Bucket.Usage.TotalTokens > 0 {
			create.SetTotalTokens(n.Bucket.Usage.TotalTokens)
		}
		if n.Bucket.Usage.TotalDurationNs > 0 {
			create.SetTotalDurationNs(n.Bucket.Usage.TotalDurationNs)
		}
		if n.Bucket.Usage.PromptDurationNs > 0 {
			create.SetPromptDurationNs(n.Bucket.Usage.PromptDurationNs)
		}
	}

	return create.Exec(ctx)
}

// Get retrieves a node by its hash.
func (ed *EntDriver) Get(ctx context.Context, hash string) (*merkle.Node, error) {
	entNode, err := ed.Client.Node.Get(ctx, hash)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, storage.ErrNotFound{Hash: hash}
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
			return nil, storage.ErrNotFound{Hash: hash}
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

// Descendants returns the path from root to node (root first, node last).
func (ed *EntDriver) Descendants(ctx context.Context, hash string) ([]*merkle.Node, error) {
	path, err := ed.Ancestry(ctx, hash)
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
func (ed *EntDriver) Depth(ctx context.Context, hash string) (int, error) {
	path, err := ed.Ancestry(ctx, hash)
	if err != nil {
		return 0, err
	}
	return len(path) - 1, nil
}

// Close closes the database connection.
func (ed *EntDriver) Close() error {
	return ed.Client.Close()
}

// Conversion helpers (unchanged from your original)
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

	// Reconstruct usage from individual fields if not in bucket
	if bucket.Usage == nil && (entNode.PromptTokens != nil || entNode.CompletionTokens != nil ||
		entNode.TotalTokens != nil || entNode.TotalDurationNs != nil || entNode.PromptDurationNs != nil) {
		bucket.Usage = &llm.Usage{}
		if entNode.PromptTokens != nil {
			bucket.Usage.PromptTokens = *entNode.PromptTokens
		}
		if entNode.CompletionTokens != nil {
			bucket.Usage.CompletionTokens = *entNode.CompletionTokens
		}
		if entNode.TotalTokens != nil {
			bucket.Usage.TotalTokens = *entNode.TotalTokens
		}
		if entNode.TotalDurationNs != nil {
			bucket.Usage.TotalDurationNs = *entNode.TotalDurationNs
		}
		if entNode.PromptDurationNs != nil {
			bucket.Usage.PromptDurationNs = *entNode.PromptDurationNs
		}
	}

	return &merkle.Node{
		Hash:       entNode.ID,
		ParentHash: entNode.ParentHash,
		Bucket:     bucket,
	}, nil
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

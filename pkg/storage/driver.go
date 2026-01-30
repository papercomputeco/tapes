// Package storage
package storage

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// Driver defines the interface for persisting and retrieving nodes in a storage backend.
// The Driver is the primary interface for working with pkg/merkle - it handles
// storage, retrieval, and traversal of nodes per the storage implementor.
type Driver interface {
	// Put stores a node. Returns true if the node was newly inserted,
	// false if it already exists. If the node already exists, this should be
	// a no-op. Put provides automatic deduplication via content-addressing in the dag.
	Put(ctx context.Context, node *merkle.Node) (bool, error)

	// Get retrieves a node by its hash.
	Get(ctx context.Context, hash string) (*merkle.Node, error)

	// Has checks if a node exists by its hash.
	Has(ctx context.Context, hash string) (bool, error)

	// List returns all nodes in the store.
	List(ctx context.Context) ([]*merkle.Node, error)

	// Roots returns all root nodes (nodes with no parent).
	Roots(ctx context.Context) ([]*merkle.Node, error)

	// Leaves returns all leaf nodes (nodes with no children).
	Leaves(ctx context.Context) ([]*merkle.Node, error)

	// Ancestry returns the path from a node back to its root (node first, root last).
	Ancestry(ctx context.Context, hash string) ([]*merkle.Node, error)

	// Depth returns the depth of a node (0 for roots).
	Depth(ctx context.Context, hash string) (int, error)

	// Close closes the store and releases any resources.
	Close() error
}

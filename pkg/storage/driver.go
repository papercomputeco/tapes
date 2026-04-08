// Package storage
package storage

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// Driver defines the interface for persisting and retrieving nodes in a storage backend.
// The Driver is the primary interface for working with pkg/merkle - it handles
// storage, retrieval, and traversal of nodes per the storage implementor.
//
// A Driver embeds merkle.DagLoader, so any storage.Driver is also a merkle.DagLoader.
// This avoids the need for callers to cast a Driver to a DagLoader — they can pass
// a Driver wherever a DagLoader is expected directly.
type Driver interface {
	// DagLoader provides read and traversal operations on the DAG.
	// Get, GetByParent, and Ancestry come from this embedded interface.
	merkle.DagLoader

	// Put stores a node. Returns true if the node was newly inserted,
	// false if it already exists. If the node already exists, this should be
	// a no-op. Put provides automatic deduplication via content-addressing in the dag.
	Put(ctx context.Context, node *merkle.Node) (bool, error)

	// Has checks if a node exists by its hash.
	Has(ctx context.Context, hash string) (bool, error)

	// List returns all nodes in the store.
	List(ctx context.Context) ([]*merkle.Node, error)

	// Roots returns all root nodes (nodes with no parent).
	Roots(ctx context.Context) ([]*merkle.Node, error)

	// Leaves returns all leaf nodes (nodes with no children).
	Leaves(ctx context.Context) ([]*merkle.Node, error)

	// Depth returns the depth of a node (0 for roots).
	Depth(ctx context.Context, hash string) (int, error)

	// Migrate applies any pending schema migrations for the storage backend.
	// Implementations must be safe to call concurrently from multiple processes.
	// For backends that don't require migrations (e.g. in-memory), this is a no-op.
	Migrate(ctx context.Context) error

	// Close closes the store and releases any resources.
	Close() error
}

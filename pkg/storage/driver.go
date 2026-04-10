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

	// ListSessions returns a page of leaf nodes ordered by created_at descending,
	// optionally filtered by ListOpts. The returned Page.NextCursor is empty
	// when there are no further pages.
	//
	// "Session" here is the API-layer concept: a leaf node identifies the head
	// of a conversation chain. Filters apply to the leaf node itself, not to
	// any ancestor in the chain.
	ListSessions(ctx context.Context, opts ListOpts) (*Page[*merkle.Node], error)

	// CountSessions returns aggregate counts for the slice of data matching
	// the filter in opts. Pagination fields on opts (Limit, Cursor) are ignored.
	CountSessions(ctx context.Context, opts ListOpts) (SessionStats, error)

	// AncestryChain is Ancestry with a marker describing how the walk
	// terminated. When the walk stops at a parent_hash whose target is not
	// present in this store, the returned Chain has Incomplete=true and
	// MissingParent set to that parent_hash. The nodes in Chain.Nodes are
	// still valid; this state is expected on stores that trim older data or
	// merge content from foreign sources, and is not an error.
	AncestryChain(ctx context.Context, hash string) (*Chain, error)

	// AncestryChains returns a Chain for each input hash, batched per depth
	// level so the cost scales with maximum chain depth rather than the
	// product of starting-node count and depth. Shared ancestors across
	// starts are fetched once.
	//
	// The returned map is keyed by each starting hash. Starts that are not
	// present in the store are omitted from the map rather than surfaced as
	// errors — callers that need a strict "every start must resolve" check
	// should compare the map's keys against their input slice.
	//
	// Use this instead of looping over AncestryChain when walking many
	// leaves (e.g. the /v1/sessions/summary handler): the per-leaf loop
	// issues O(N_starts × depth) queries, which on a real store with tens
	// of thousands of leaves will not complete in any reasonable time.
	AncestryChains(ctx context.Context, hashes []string) (map[string]*Chain, error)

	// Depth returns the depth of a node (0 for roots).
	Depth(ctx context.Context, hash string) (int, error)

	// Migrate applies any pending schema migrations for the storage backend.
	// Implementations must be safe to call concurrently from multiple processes.
	// For backends that don't require migrations (e.g. in-memory), this is a no-op.
	Migrate(ctx context.Context) error

	// Close closes the store and releases any resources.
	Close() error
}

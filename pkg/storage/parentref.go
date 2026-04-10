package storage

import "context"

// ParentRef is a lightweight (hash, parent_hash) tuple used by integrity
// checks and bulk traversal code that only needs the DAG edges, not the
// full node bucket JSON.
type ParentRef struct {
	Hash       string
	ParentHash *string
}

// ParentRefLister is an optional capability for drivers that can return
// every node's parent edge in a single efficient query. Drivers that do
// not implement it are assumed to be unsuitable for bulk integrity checks
// over large stores.
type ParentRefLister interface {
	ListParentRefs(ctx context.Context) ([]ParentRef, error)
}

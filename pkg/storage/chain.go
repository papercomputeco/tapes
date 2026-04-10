package storage

import "github.com/papercomputeco/tapes/pkg/merkle"

// Chain is the result of walking a node's parent edges back toward a root.
//
// A Chain whose Incomplete field is false reached a legitimate root (a node
// whose parent_hash is nil or empty). A Chain whose Incomplete field is true
// stopped because a parent_hash pointed at a node that is not currently
// present in this store — see MissingParent. The nodes in Nodes are still
// valid; the store simply can't resolve any higher ancestors from here.
//
// This state is expected on large or long-lived stores that trim/offload
// older data, merge content from foreign sources, or receive chains whose
// ancestors live in another store altogether. Callers should treat it as
// informational (a signal to render a marker, or trigger a future "thaw"
// lookup against another source), not as corruption.
type Chain struct {
	// Nodes is the walk output in node-first order: index 0 is the node
	// the walk started from, and the last element is either a real root
	// or the last resolvable node whose parent could not be found.
	Nodes []*merkle.Node

	// Incomplete is true when the walk stopped short of a real root,
	// whether because a parent_hash could not be resolved (see
	// MissingParent) or because a cycle was detected (see CycleDetected).
	Incomplete bool

	// MissingParent is the parent_hash that could not be resolved in this
	// store. Only set when Incomplete is true due to a dangling pointer;
	// left empty for cycle-detected incompletes (the cycle-triggering
	// hash is present in Nodes already).
	MissingParent string

	// CycleDetected is true when the walk stopped because it was about
	// to re-visit a hash already in Nodes. Drivers guard every walk with
	// a per-chain seen-set so a corrupt parent edge can never spin
	// forever; when this flag trips, the chain is the largest
	// acyclic prefix reachable from the starting hash.
	//
	// In practice this never trips on a healthy store. It exists because
	// a single corrupted parent_hash would otherwise hang any endpoint
	// that walks ancestry, which is a blast radius out of proportion to
	// the likelihood.
	CycleDetected bool
}

// Complete reports whether the walk reached a real root.
func (c *Chain) Complete() bool {
	return c != nil && !c.Incomplete
}

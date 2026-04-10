// Package validate walks the parent edges of a tapes store and reports DAG
// integrity problems: cycles (which cause Ancestry() to loop forever) and
// dangling parent references (parents pointing at nodes that no longer exist).
//
// The check is intentionally structured around ParentRef tuples rather than
// full merkle.Node values so it stays cheap on large stores — the ent driver
// implementation projects only the hash and parent_hash columns.
package validate

import (
	"context"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// Report summarizes the outcome of a store integrity check.
type Report struct {
	// TotalNodes is the number of nodes considered by the check.
	TotalNodes int
	// Roots is the number of nodes whose parent_hash is NULL.
	Roots int
	// Cycles holds one entry per detected cycle. Each entry lists the
	// hashes that form the cycle, in traversal order, with the repeated
	// hash appended at the end so callers can render it as A → B → A.
	Cycles [][]string
	// Dangling holds nodes whose parent_hash points at a hash not present
	// in the store.
	Dangling []Dangling
}

// Dangling is a node whose parent_hash points to a missing node.
type Dangling struct {
	Hash       string
	ParentHash string
}

// OK reports whether the store passed every check.
func (r Report) OK() bool {
	return len(r.Cycles) == 0 && len(r.Dangling) == 0
}

// Check runs the integrity check against the given lister and returns a Report.
// The lister must return every (hash, parent_hash) tuple in the store; it is
// expected to be implemented via storage.ParentRefLister.
func Check(ctx context.Context, lister storage.ParentRefLister) (Report, error) {
	refs, err := lister.ListParentRefs(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("load parent refs: %w", err)
	}
	return CheckRefs(refs), nil
}

// CheckRefs runs the integrity check against an already-materialized slice of
// parent refs. Split from Check so the pure graph analysis is independently
// testable without a storage driver.
func CheckRefs(refs []storage.ParentRef) Report {
	parent := make(map[string]string, len(refs))
	known := make(map[string]struct{}, len(refs))
	var roots int

	for _, r := range refs {
		known[r.Hash] = struct{}{}
		if r.ParentHash == nil || *r.ParentHash == "" {
			roots++
			continue
		}
		parent[r.Hash] = *r.ParentHash
	}

	report := Report{
		TotalNodes: len(refs),
		Roots:      roots,
	}

	// Pass 1: dangling parent refs. A parent pointer that can't be resolved
	// to a node in `known` would cause Ancestry() to return NotFound partway
	// through a walk — these are bad but not cycle-triggering.
	for hash, ph := range parent {
		if _, ok := known[ph]; !ok {
			report.Dangling = append(report.Dangling, Dangling{
				Hash:       hash,
				ParentHash: ph,
			})
		}
	}

	// Pass 2: cycle detection via iterative DFS over the parent edges with
	// 3-color marking (white=unseen, gray=on current stack, black=done).
	// Because each node has at most one parent, the "DFS" degenerates into
	// walking a linked list per starting node — but the 3-color trick still
	// handles cycles correctly without recursion depth issues.
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(refs))
	// `order` records the hashes on the current walk so we can slice out
	// the cycle when we hit a gray node.
	for start := range known {
		if color[start] != white {
			continue
		}
		var order []string
		indexInOrder := make(map[string]int, 8)
		cur := start
		for {
			if c := color[cur]; c == black {
				// Walked into territory that's already been cleared: mark
				// everything in this walk black and move on.
				break
			} else if c == gray {
				// Cycle: extract from the first sighting of cur to the end.
				idx := indexInOrder[cur]
				cycle := make([]string, 0, len(order)-idx+1)
				cycle = append(cycle, order[idx:]...)
				cycle = append(cycle, cur) // close the loop visually
				report.Cycles = append(report.Cycles, cycle)
				break
			}
			color[cur] = gray
			indexInOrder[cur] = len(order)
			order = append(order, cur)

			ph, ok := parent[cur]
			if !ok {
				// Reached a root (no parent entry) — clean walk.
				break
			}
			if _, exists := known[ph]; !exists {
				// Dangling parent — already recorded; stop walking.
				break
			}
			cur = ph
		}
		// Mark everything on this walk black so we don't re-traverse it.
		for _, h := range order {
			color[h] = black
		}
	}

	return report
}

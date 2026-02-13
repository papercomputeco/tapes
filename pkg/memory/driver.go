// Package memory provides a pluggable memory layer for the tapes system.
//
// Memory drivers extract durable facts from conversation nodes and recall them
// on demand. Facts are distilled, persistent knowledge derived from
// conversations — not raw messages.
//
// The [Driver] interface is intentionally minimal: Store extracts facts from
// nodes, Recall retrieves facts relevant to a DAG position, and Close releases
// resources. Memory is one-way; backend systems manage their own lifecycle and
// eviction policies.
//
// Short-term context (e.g., sliding windows over recent nodes) is a
// proxy-level concern handled via the storage driver's Ancestry method and is
// not part of the memory interface.
//
// Drivers are pluggable via configuration:
//
//	[memory]
//	provider = "local"   # or "cognee", "graph"
package memory

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// Driver handles storage and recall of conversation memory.
// Implementers extract durable facts from conversation nodes and recall them
// given a position in the DAG.
type Driver interface {
	// Store persists one or more nodes into memory. This is the forcing
	// function for driver implementors to extract facts from conversation
	// nodes. Called asynchronously by the proxy worker pool after a
	// conversation turn is stored in the DAG.
	Store(ctx context.Context, nodes []*merkle.Node) error

	// Recall retrieves facts relevant to a position in the DAG tree.
	// The hash identifies a node (typically the current leaf), and the
	// driver returns facts relevant to that branch/position.
	Recall(ctx context.Context, hash string) ([]Fact, error)

	// Close releases driver resources.
	Close() error
}

// Fact represents a distilled, durable piece of knowledge extracted from
// conversations. Facts are the output of the memory layer — not raw messages,
// but persistent knowledge that may be relevant across branches and sessions.
type Fact struct {
	// Content is the extracted fact text.
	Content string `json:"content"`
}

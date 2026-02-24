// Package publisher provides interfaces and implementations for publishing
// Merkle DAG nodes to external event streaming systems.
package publisher

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// Publisher publishes Merkle nodes to an external sink.
type Publisher interface {
	// Publish publishes one node.
	Publish(ctx context.Context, node *merkle.Node) error

	// Close releases any resources held by the publisher.
	Close() error
}

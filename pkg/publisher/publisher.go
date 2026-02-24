// Package publisher provides interfaces and implementations for publishing
// Merkle DAG nodes to external event streaming systems.
package publisher

import (
	"context"
)

// Publisher publishes events to an external sink.
type Publisher interface {
	// Publish publishes one event.
	Publish(ctx context.Context, event *Event) error

	// Close releases any resources held by the publisher.
	Close() error
}

package publisher

import (
	"context"
)

// NopPublisher is a no-op publisher intended for tests and disabled publishing.
type NopPublisher struct{}

// NewNopPublisher creates a new no-op publisher.
func NewNopPublisher() *NopPublisher {
	return &NopPublisher{}
}

// Publish is a no-op.
func (n *NopPublisher) Publish(_ context.Context, _ *Event) error {
	return nil
}

// Close is a no-op.
func (n *NopPublisher) Close() error {
	return nil
}

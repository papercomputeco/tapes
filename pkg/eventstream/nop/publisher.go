package nop

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/eventstream"
)

// Publisher is a no-op eventstream publisher used for tests and disabled mode.
type Publisher struct{}

// NewPublisher creates a new no-op eventstream publisher.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// PublishTurn validates input and otherwise does nothing.
func (p *Publisher) PublishTurn(_ context.Context, event *eventstream.TurnPersistedEvent) error {
	if event == nil {
		return eventstream.ErrNilTurnEvent
	}

	return nil
}

// Close is a no-op.
func (p *Publisher) Close() error {
	return nil
}

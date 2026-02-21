package eventstream

import "context"

// Publisher publishes turn events to an event stream backend.
type Publisher interface {
	PublishTurn(ctx context.Context, event *TurnPersistedEvent) error
	Close() error
}

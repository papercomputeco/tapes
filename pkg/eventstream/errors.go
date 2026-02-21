package eventstream

import "errors"

// ErrNilTurnEvent indicates a nil turn event payload was provided to a publisher.
var ErrNilTurnEvent = errors.New("nil turn event")

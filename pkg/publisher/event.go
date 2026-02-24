package publisher

import (
	"errors"
	"time"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

const (
	// SchemaNodeV1 is the schema identifier for node publish events.
	SchemaNodeV1 = "tapes.node.v1"
)

// ErrNilNode indicates a nil node was provided where a value is required.
var ErrNilNode = errors.New("cannot create event from nil node")

// ErrEmptyRootHash indicates an empty root hash was provided where a value is required.
var ErrEmptyRootHash = errors.New("cannot create event with empty root hash")

// Event is the publish payload for a single Merkle node.
type Event struct {
	Schema     string      `json:"schema"`
	RootHash   string      `json:"root_hash"`
	OccurredAt time.Time   `json:"occurred_at"`
	Node       merkle.Node `json:"node"`
}

// NewEvent creates an Event from a Merkle node.
func NewEvent(rootHash string, node *merkle.Node) (*Event, error) {
	if rootHash == "" {
		return nil, ErrEmptyRootHash
	}

	if node == nil {
		return nil, ErrNilNode
	}

	return &Event{
		Schema:     SchemaNodeV1,
		RootHash:   rootHash,
		OccurredAt: time.Now(),
		Node:       *node,
	}, nil
}

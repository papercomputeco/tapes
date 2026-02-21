package eventstream

import (
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

const (
	// SchemaVersionV1 is the first version of the event payload schema.
	SchemaVersionV1 = 1

	// EventTypeTurnPersisted is emitted after a conversation turn is persisted.
	EventTypeTurnPersisted = "tapes.turn.persisted"
)

// TurnPersistedEvent is a transport-neutral event payload for a persisted turn.
type TurnPersistedEvent struct {
	SchemaVersion int                  `json:"schema_version"`
	EventType     string               `json:"event_type"`
	EventID       string               `json:"event_id"`
	EmittedAt     time.Time            `json:"emitted_at"`
	Source        EventSource          `json:"source"`
	RequestMeta   TurnRequestMeta      `json:"request_meta"`
	DAG           TurnDAGMeta          `json:"dag"`
	Turn          llm.ConversationTurn `json:"turn"`
}

// EventSource identifies where the turn originated.
type EventSource struct {
	Project   string `json:"project,omitempty"`
	AgentName string `json:"agent_name,omitempty"`
	Provider  string `json:"provider"`
}

// TurnRequestMeta captures request lifecycle metadata for the event.
type TurnRequestMeta struct {
	Path        string    `json:"path,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMs  int64     `json:"duration_ms"`
	Streaming   bool      `json:"streaming"`
	HTTPStatus  int       `json:"http_status"`
}

// TurnDAGMeta captures DAG-specific metadata for the persisted turn.
type TurnDAGMeta struct {
	RootHash       string   `json:"root_hash"`
	HeadHash       string   `json:"head_hash"`
	ParentHash     *string  `json:"parent_hash,omitempty"`
	TurnNodeHashes []string `json:"turn_node_hashes"`
	NewNodeHashes  []string `json:"new_node_hashes,omitempty"`
}

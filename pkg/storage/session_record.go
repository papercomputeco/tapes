package storage

import "time"

// SessionRecord is the flat sessions-table row surfaced by
// GET /v1/sessions. Fields absent in the DB (NULL) are represented as empty
// strings or nil/zero values so API callers never have to unwrap optional
// pgtype wrappers.
type SessionRecord struct {
	ID                string
	HarnessID         string
	HarnessSessionID  string
	Name              string // empty when not set
	Cwd               string // empty when not set
	HarnessVersion    string // empty when not set
	ParentSessionID   string // empty when not set
	StartedAt         time.Time
	LastSeenAt        time.Time
	EndedAt           *time.Time // nil when session is still live
	HarnessMetadata   map[string]any
	TotalInputTokens  int64
	TotalOutputTokens int64
	TotalCostUsd      float64
	TurnCount         int
	// DerivedStatus is the chain-aware session status (completed / failed /
	// abandoned / unknown), denormalized at ingest. 'unknown' until the first
	// turn lands or, for pre-feature rows, until the status backfill runs.
	DerivedStatus string
	Preview       string // first user turn text, truncated; empty when unavailable
}

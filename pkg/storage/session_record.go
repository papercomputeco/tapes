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
	// Model is the dominant conversation-spine model, folded at derive
	// time (sessions.derived_model). Empty until the session derives.
	Model   string
	Preview string // first user turn text, truncated; empty when unavailable
	// AuthSubject is the gateway-stamped JWT subject (the WorkOS user id)
	// captured at ingest. Empty for rows captured before the edge began
	// stamping the x-paper-auth-subject header.
	AuthSubject string
}

// SessionListOpts parameterizes the sessions-list read: keyset cursor
// (last_seen_at DESC, id DESC), an optional activity window, and an
// optional attribution filter. The since/until window filters on
// last_seen_at — the sort/cursor column — so "sessions active in the
// period" pages consistently. AuthSubject "" lists every user's
// sessions; non-empty is an exact match on the gateway-stamped JWT
// subject.
type SessionListOpts struct {
	Limit       int
	CursorTs    *time.Time
	CursorID    *string
	Since       *time.Time
	Until       *time.Time
	AuthSubject string
}

package storage

import (
	"encoding/json"
	"time"
)

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
	Model string
	// ModelUsage is the per-model spend breakdown folded at derive time
	// across every thread (sessions.model_usage), cost-weighted so the
	// share reflects spend rather than call count. Nil until the session
	// derives; ordered dominant-model-first (by cost).
	ModelUsage []ModelUsage
	// Tasks is the deriver's session-scoped TaskCreate/TaskUpdate fold and
	// KindCounts the per-call_kind span tally (sessions.tasks /
	// sessions.kind_counts), both JSONB served verbatim on the composite
	// traces response. Nil until the session derives.
	Tasks      json.RawMessage
	KindCounts json.RawMessage
	Preview    string // first user turn text, truncated; empty when unavailable
	// AuthSubject is the gateway-stamped JWT subject (the WorkOS user id)
	// captured at ingest. Empty for rows captured before the edge began
	// stamping the x-paper-auth-subject header.
	AuthSubject string
	// SortVal is the canonical ::text form of this row's active sort column,
	// populated by ListSessionRecords so the API can mint an exact next cursor.
	// Empty on records returned by point lookups.
	SortVal string
}

// ModelUsage is one model's contribution to a session: how many llm
// calls ran on it and what they spent. Cost is priced at derive time,
// so the per-model share is spend-weighted (a fan-out of cheap subagent
// calls never out-votes the expensive main-spine model).
type ModelUsage struct {
	Model        string  `json:"model"`
	Calls        int64   `json:"calls"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUSD      float64 `json:"cost_usd"`
}

// SessionListOpts parameterizes the sessions-list read: keyset cursor
// ordered by any sortable column (default last_seen_at DESC, id DESC),
// an optional activity window, and an optional attribution filter.
// The since/until window filters on last_seen_at. AuthSubject "" lists
// every user's sessions; non-empty is an exact match on the
// gateway-stamped JWT subject.
type SessionListOpts struct {
	Limit       int
	Sort        SessionSortField // zero value == SortLastActive
	Dir         SortDirection    // zero value == SortDesc
	CursorVal   *string          // boundary row's sort column, canonical ::text form
	CursorID    *string          // boundary row's id (UUID), the keyset tiebreak
	Since       *time.Time
	Until       *time.Time
	AuthSubject string
}

// SessionSortField is the validated column a sessions-list page is ordered by.
// The zero value sorts by last activity (the historical default).
type SessionSortField string

const (
	SortLastActive    SessionSortField = "last_active"
	SortStartedAt     SessionSortField = "started_at"
	SortTurnCount     SessionSortField = "turn_count"
	SortTotalCost     SessionSortField = "total_cost_usd"
	SortTotalTokens   SessionSortField = "total_tokens"
	SortDurationNs    SessionSortField = "duration_ns"
	SortDerivedStatus SessionSortField = "derived_status"
	SortAuthSubject   SessionSortField = "auth_subject"
)

// SortDirection is the validated order direction.
type SortDirection string

const (
	SortAsc  SortDirection = "asc"
	SortDesc SortDirection = "desc"
)

// SortColumn is an opaque, SQL-safe sort target: a physical column name and the
// Postgres type the keyset cursor value is cast back to. Its fields are
// unexported and there is no exported constructor, so the only SortColumns that
// exist are the entries in sessionSortColumn below. A SortColumn interpolated
// into an ORDER BY clause or a ::cast therefore cannot carry an
// attacker-controlled identifier by construction — the type system enforces the
// allowlist that a bare string could only enforce by convention.
type SortColumn struct {
	col  string
	cast string
}

// Col is the physical column name, safe to interpolate into SQL because it can
// only have originated from the allowlist.
func (c SortColumn) Col() string { return c.col }

// Cast is the Postgres type the keyset cursor value is cast back to (bigint,
// numeric, timestamptz, text) — likewise allowlist-sourced and SQL-safe.
func (c SortColumn) Cast() string { return c.cast }

// sessionSortColumn maps each sort field to its physical column and the
// Postgres type the cursor value is cast back to. Membership here is the
// allowlist — any field not in this map is rejected before it reaches SQL,
// so the column name is never attacker-controlled.
//
// INVARIANT: every column listed here MUST be NOT NULL. The keyset cursor
// encodes the boundary row's value as a non-null ::text and casts it back with
// the column's cast type; a NULL value cannot round-trip through that cursor,
// and the keyset predicate (col < val) evaluates to NULL for NULL rows, silently
// dropping them after page 1. Adding a nullable sortable column therefore needs
// NULLS-ordering discipline plus a cursor sentinel first — it is not a one-line
// map entry.
//
// We deliberately do NOT add `NULLS LAST` to the ORDER BY: the indexes are
// (org_id, col DESC, id DESC) = NULLS FIRST, so NULLS LAST would forfeit the
// index for ordering and force a sort. NULL ordering is a non-issue while this
// invariant holds.
var sessionSortColumn = map[SessionSortField]SortColumn{
	SortLastActive:    {col: "last_seen_at", cast: "timestamptz"},
	SortStartedAt:     {col: "started_at", cast: "timestamptz"},
	SortTurnCount:     {col: "turn_count", cast: "bigint"},
	SortTotalCost:     {col: "total_cost_usd", cast: "numeric"},
	SortTotalTokens:   {col: "total_tokens", cast: "bigint"},
	SortDurationNs:    {col: "duration_ns", cast: "bigint"},
	SortDerivedStatus: {col: "derived_status", cast: "text"},
	SortAuthSubject:   {col: "auth_subject", cast: "text"},
}

// ParseSessionSortField validates a raw sort key. Empty string is the default
// (last_active). ok is false for any unrecognized value.
func ParseSessionSortField(raw string) (SessionSortField, bool) {
	if raw == "" {
		return SortLastActive, true
	}
	f := SessionSortField(raw)
	if _, ok := sessionSortColumn[f]; ok {
		return f, true
	}
	return "", false
}

// ParseSortDirection validates a raw direction. Empty string defaults to desc.
func ParseSortDirection(raw string) (SortDirection, bool) {
	switch raw {
	case "", string(SortDesc):
		return SortDesc, true
	case string(SortAsc):
		return SortAsc, true
	default:
		return "", false
	}
}

// SessionSortColumn resolves a validated sort field to its opaque SortColumn
// (physical column + cursor cast type). ok is false for unknown fields — the
// injection guard: an unrecognized field never yields a SortColumn, so it can
// never reach SQL.
func SessionSortColumn(f SessionSortField) (SortColumn, bool) {
	c, ok := sessionSortColumn[f]
	return c, ok
}

package storage

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultListLimit is the page size used when ListOpts.Limit is zero.
const DefaultListLimit = 50

// MaxListLimit is the maximum permitted page size. Drivers clamp
// ListOpts.Limit to this value.
//
// Set high (5000) because the AncestryChains hot path has a large
// fixed cost per request (the recursive CTE setup) and a tiny
// incremental cost per leaf — measured at ~260ms regardless of
// whether limit is 200 or 1000 against the brian_large store. Forcing
// callers to paginate at small page sizes multiplies the fixed cost.
// The deck's full Overview load drops from ~17s @ limit=200 (50 round
// trips) to ~3s @ limit=2000 (5 round trips) just from the math.
//
// Memory impact: each row carries ~200 bytes (the CTE doesn't ship
// the heavy `content` blob — see the label_hint extraction in
// pkg/storage/ent/driver/driver.go). 5000 leaves × ~30 avg depth
// × 200 bytes ≈ 30 MB peak per request, which is fine for an API
// server backing one deck instance.
const MaxListLimit = 5000

// ListOpts controls filtering and cursor pagination for session listings.
//
// All filter fields are AND-combined and apply to the head (leaf) node of
// each session. Empty string and nil pointer fields are treated as "no filter".
//
// Pagination is keyset-based on (CreatedAt DESC, Hash DESC). Callers should
// treat Cursor as opaque; use the NextCursor returned in Page.
type ListOpts struct {
	// Limit is the maximum number of items to return. If zero, DefaultListLimit
	// is used. Values larger than MaxListLimit are clamped.
	Limit int

	// Cursor is an opaque pagination token from a prior Page.NextCursor.
	// Empty means start from the most recent.
	Cursor string

	// Filters. Empty / nil values mean "no filter on this field".
	Project  string
	Agent    string
	Model    string
	Provider string
	Since    *time.Time
	Until    *time.Time
}

// Normalize returns a copy of opts with Limit clamped to [1, MaxListLimit].
// A zero Limit is replaced with DefaultListLimit.
func (o ListOpts) Normalize() ListOpts {
	out := o
	if out.Limit <= 0 {
		out.Limit = DefaultListLimit
	}
	if out.Limit > MaxListLimit {
		out.Limit = MaxListLimit
	}
	return out
}

// Page is a generic paginated result envelope.
type Page[T any] struct {
	Items []T

	// NextCursor is empty when there are no more pages.
	NextCursor string
}

// SessionStats is the aggregate result of CountSessions for a given filter.
//
// All numeric aggregates are computed over the set of nodes matching the
// supplied ListOpts filter; they are not restricted to nodes that are part
// of a matching leaf session. This mirrors the long-standing TurnCount
// semantic: the filter is per-node, not per-chain.
type SessionStats struct {
	// SessionCount is the number of distinct first-class sessions touched by
	// the matching nodes (keyed on nodes.session_id). Nodes with no
	// session_id — legacy or non-session-tracked writers, including the
	// in-memory driver — do not contribute, so a store with no session rows
	// reports 0. See StemCount for the leaf-based metric.
	SessionCount int

	// StemCount is the number of leaf nodes (Merkle chains) matching the
	// filter — one per /v1/stems entry. This is the value SessionCount
	// reported before the sessions table existed.
	StemCount int

	// TurnCount is the number of nodes (turns) matching the filter.
	TurnCount int

	// RootCount is the number of root nodes (no parent) matching the filter.
	RootCount int

	// CompletedCount is the number of distinct sessions whose chain-aware
	// derived_status is "completed" (computed at ingest via
	// pkg/sessions.DetermineStatus, read from the sessions table). Like
	// SessionCount it is session-grained: a store with no session rows
	// reports 0.
	CompletedCount int

	// InputTokens / OutputTokens are SUMs over the matching node set,
	// taken from prompt_tokens / completion_tokens columns.
	InputTokens  int64
	OutputTokens int64

	// CacheCreationTokens / CacheReadTokens are SUMs of the cache-aware
	// token columns. Surfaced so a caller (typically the API handler) can
	// fold cost via pkg/sessions.CostForTokensWithCache.
	CacheCreationTokens int64
	CacheReadTokens     int64

	// TotalDurationNs is the wall-clock span MAX(created_at) − MIN(created_at)
	// over the matching node set, in nanoseconds. It is NOT a sum of per-call
	// durations: nodes.total_duration_ns is currently never populated by the
	// proxy (see PCC-514), so SUMming the column would always return 0.
	// Wall-clock span is meaningful for a dashboard "Agent Time" card and is
	// the same shape that pkg/sessions.BuildSummary uses per session.
	TotalDurationNs int64

	// ToolCalls is the number of tool_use content blocks across the
	// matching node set.
	ToolCalls int

	// PerModel breaks tokens down by (normalized) model so the API layer
	// can apply per-model pricing without the storage driver having to
	// know about pricing tables. Keys are normalized model names; nodes
	// with no model are excluded.
	PerModel map[string]ModelTokenStats
}

// ModelTokenStats is the per-model token rollup returned inside SessionStats.
// Cost is intentionally not computed here — pricing lives in pkg/sessions
// and is applied by the API handler.
type ModelTokenStats struct {
	InputTokens         int64
	OutputTokens        int64
	CacheCreationTokens int64
	CacheReadTokens     int64
}

// Cursor is the decoded form of an opaque ListOpts.Cursor token.
// It is exported for driver implementations; clients should treat
// the encoded string as opaque.
type Cursor struct {
	// CreatedAt is the head-node timestamp of the last item on the prior page.
	CreatedAt time.Time `json:"t"`

	// Hash is the head-node hash of the last item on the prior page.
	// Used as a tiebreaker when multiple nodes share a CreatedAt.
	Hash string `json:"h"`
}

// Encode returns the opaque base64 representation of the cursor.
func (c Cursor) Encode() string {
	b, err := json.Marshal(c)
	if err != nil {
		// json.Marshal cannot fail for this struct shape.
		panic(fmt.Sprintf("encoding cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses an opaque cursor token. An empty token returns the
// zero Cursor without error, meaning "start from the most recent".
func DecodeCursor(token string) (Cursor, error) {
	if token == "" {
		return Cursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c Cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if c.Hash == "" {
		return Cursor{}, errors.New("invalid cursor: missing hash")
	}
	return c, nil
}

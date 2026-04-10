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
type SessionStats struct {
	// SessionCount is the number of leaf nodes matching the filter.
	SessionCount int

	// TurnCount is the number of nodes (turns) matching the filter.
	// Filters apply to the same per-node fields used for SessionCount;
	// it is not restricted to nodes that are part of a matching session.
	TurnCount int

	// RootCount is the number of root nodes (no parent) matching the filter.
	RootCount int
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

package storage

import (
	"context"
	"time"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// SessionIngester is an optional capability for a Driver: it folds
// session resolution, node insert, and counter rollup into a single
// transaction.
//
// Only drivers that can host the sessions table implement this. The
// Postgres driver does; the in-memory driver intentionally does not
// (it has no concept of sessions, and tests that exercise the
// classic Put-only path still want the legacy behavior). Callers
// like the worker pool MUST type-assert against this interface
// before invoking it.
//
// Implementations MUST satisfy these invariants:
//
//   - All side effects (sessions UPSERT, parent placeholder insert,
//     nodes inserts, counter UPDATE) commit atomically. A panic or
//     error on any step rolls back every other step.
//   - The resolved sessions row is keyed by
//     (org_id, harness_id, harness_session_id). When the inbound
//     envelope is nil, or HarnessID is "unknown", or
//     HarnessSessionID is empty, implementations derive a synthetic
//     harness_session_id from the captured turn's Merkle root prefix.
//   - Counters (turn_count, total_input_tokens, total_output_tokens,
//     total_cost_usd) are incremented ONLY when at least one new
//     node was actually inserted in this call. A duplicate envelope
//     (every node hash already present) is a true no-op on counters
//     — this preserves end-to-end idempotency for retried POSTs.
type SessionIngester interface {
	// IngestTurn stores every node in `nodes` (root first, leaf
	// last; ParentHash must already chain correctly) inside a single
	// Tx that also resolves/UPSERTs the sessions row, optionally
	// resolves the parent_session_id FK, stamps session_id onto each
	// newly-inserted node, and rolls up the counters.
	IngestTurn(ctx context.Context, req IngestTurnRequest) (IngestTurnResult, error)
}

// IngestTurnRequest bundles every input the SessionIngester needs.
// Built by the worker pool from a single Job + the chain of
// content-addressed nodes derived from that Job's request/response
// pair.
type IngestTurnRequest struct {
	// Session is the optional session-tracking envelope. nil is a
	// legitimate value (legacy clients): implementations fall back
	// to a Merkle-derived synthetic harness_session_id.
	Session *sessions.IngestEnvelope

	// Nodes is the ordered projection for this turn. Non-injected nodes
	// form the root-to-leaf conversation spine; injected nodes may appear
	// between them as side branches from the current spine parent. The
	// last element is the assistant response.
	Nodes []*merkle.Node

	// InputTokens / OutputTokens / CostUSD are the per-turn deltas
	// applied to the sessions counters when at least one node was
	// newly inserted. Pre-computed by the worker (which owns the
	// pricing lookup) so this layer stays free of llm/pricing
	// dependencies.
	InputTokens  int64
	OutputTokens int64
	CostUSD      float64

	// DerivedTitle folds a title-gen shadow call's output onto the
	// session (sessions.derived_title). Empty for every other call
	// kind; non-empty values overwrite (the harness regenerates the
	// title as the session evolves, so the latest wins).
	DerivedTitle string
}

// IngestTurnResult reports what the call actually did. Used by the
// worker to drive downstream side effects (publisher events, vector
// embeddings) for newly-inserted nodes only.
type IngestTurnResult struct {
	// SessionID is the resolved/created sessions row id as a 36-char
	// canonical UUID string. Stable across retries (idempotent on
	// natural key).
	SessionID string

	// NewNodes lists the nodes that were actually inserted on this
	// call (i.e. nodes.hash was not already present). May be shorter
	// than the input chain on retries.
	NewNodes []*merkle.Node

	// CountersUpdated is true when the call bumped sessions counters.
	// False for pure-retry calls where every node hash already
	// existed (idempotent retries must not double-count).
	CountersUpdated bool
}

// SessionBackfiller is an optional Driver capability for linking legacy
// node rows to a session table row after the fact. It is deliberately
// separate from SessionIngester: backfill does not insert nodes, it only
// UPSERTs session identity and stamps existing nodes that still have no
// session_id.
type SessionBackfiller interface {
	BackfillSession(ctx context.Context, req SessionBackfillRequest) (SessionBackfillResult, error)
}

type SessionBackfillRequest struct {
	Session      *sessions.IngestEnvelope
	NodeHashes   []string
	StartedAt    time.Time
	LastSeenAt   time.Time
	InputTokens  int64
	OutputTokens int64
	TurnCount    int64
}

type SessionBackfillResult struct {
	SessionID   string
	NodesLinked int
}

// SessionStatusBackfiller is an optional Driver capability that recomputes
// the denormalized derived_status (and the sticky has_git_activity flag,
// tool_result_count, and tool_error_count) for sessions whose rows predate
// the ingest-time status computation. It walks each session's nodes with the
// same signal helpers ingest uses, so a backfilled store matches what live
// ingest would have written. Idempotent and safe to run online.
type SessionStatusBackfiller interface {
	BackfillSessionStatus(ctx context.Context) (BackfillSessionStatusResult, error)
}

type BackfillSessionStatusResult struct {
	// Scanned is the number of session rows visited.
	Scanned int
	// Updated is the number whose status row was (re)written.
	Updated int
}

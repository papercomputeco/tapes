package storage

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// SessionIngester is an optional capability for a Driver: it resolves
// (or creates) the sessions row for a captured turn and folds the
// turn's status onto it in a single transaction.
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
//     status fold) commit atomically. A panic or error on any step
//     rolls back every other step.
//   - The resolved sessions row is keyed by
//     (org_id, harness_id, harness_session_id). When the inbound
//     envelope is nil, or HarnessID is "unknown", or
//     HarnessSessionID is empty, implementations derive a synthetic
//     harness_session_id from the captured turn's Merkle root prefix.
//   - Nothing turn-shaped is persisted here: the in-memory node chain
//     is consumed only to resolve session identity and fold
//     derived_status/tool counts. The token/turn/cost rollups are
//     owned by the derive-time span fold
//     (FoldSessionRollupsFromSpans), so the call is idempotent for
//     retried POSTs.
type SessionIngester interface {
	// IngestTurn resolves/UPSERTs the sessions row for the turn
	// (optionally resolving the parent_session_id FK), folds
	// derived_status and tool counts from the in-memory chain, and
	// folds the derived title when present. It persists no nodes.
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

	// Nodes is the in-memory chain of nodes for this turn, ordered
	// root-to-leaf. The first element is the conversation root; the
	// last is the assistant response. Consumed for session identity
	// and status folding only — never persisted.
	Nodes []*merkle.Node

	// DerivedTitle folds a title-gen shadow call's output onto the
	// session (sessions.derived_title). Empty for every other call
	// kind; non-empty values overwrite (the harness regenerates the
	// title as the session evolves, so the latest wins).
	DerivedTitle string
}

// IngestTurnResult reports what the call resolved.
type IngestTurnResult struct {
	// SessionID is the resolved/created sessions row id as a 36-char
	// canonical UUID string. Stable across retries (idempotent on
	// natural key).
	SessionID string
}

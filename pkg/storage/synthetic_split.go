package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// SyntheticSessionAssigner is an optional capability for a Driver: it
// places a turn that carried no harness session id into an idle split of
// its synthetic conversation (see sessions.AssignSyntheticSplit) and
// records the assignment, so the next turn of the conversation is placed
// against it.
//
// Only drivers that host the sessions surface implement this (Postgres).
// Callers MUST type-assert; without it a synthetic turn is keyed by the
// bare conversation key, as it was before idle splitting existed.
type SyntheticSessionAssigner interface {
	// AssignSyntheticSession returns the harness_session_id the turn is
	// keyed by. It is idempotent per turn: assigning the same turn again
	// (same request id, or the same capture instant) returns the same id.
	AssignSyntheticSession(ctx context.Context, req SyntheticSessionRequest) (string, error)
}

// SyntheticSessionRequest identifies one synthetic turn for assignment.
type SyntheticSessionRequest struct {
	// OrgID and HarnessID scope the conversation, as they scope sessions.
	OrgID     string
	HarnessID string

	// Conversation is the conversation key: the root-hash prefix
	// sessions.SyntheticHarnessSessionID derives.
	Conversation string

	// RequestID is the turn's capture request id. A turn already stored
	// under it keeps the session it was stored with. Empty skips that
	// lookup.
	RequestID string

	// CapturedAt is when the turn was captured upstream — the instant the
	// split rule compares, never the arrival time at ingest.
	CapturedAt time.Time

	// IdleGap is the split threshold; <= 0 disables splitting.
	IdleGap time.Duration
}

// SyntheticTurn describes a captured turn for ResolveSessionKey.
type SyntheticTurn struct {
	// RootHash is the Merkle hash of the turn's conversation root node.
	RootHash   string
	RequestID  string
	CapturedAt time.Time
	IdleGap    time.Duration
}

// ResolveSessionKey returns the envelope to attribute a captured turn to
// and the harness_session_id to key it by. A harness-provided session id
// is returned untouched — it is never split. A turn without one is keyed
// by its synthetic conversation key, placed into an idle split when the
// driver implements SyntheticSessionAssigner and turn.IdleGap is positive.
//
// The returned envelope always carries the resolved key, so a caller that
// later upserts the sessions row (SessionIngester.IngestTurn) keys it
// exactly as the raw turn was keyed.
//
// When only the split assignment fails, the error is returned alongside
// the bare conversation key and its envelope, so a caller can still keep
// the turn attributed to its conversation rather than dropping it.
func ResolveSessionKey(ctx context.Context, driver any, envelope *sessions.IngestEnvelope, turn SyntheticTurn) (*sessions.IngestEnvelope, string, error) {
	synthetic := envelope == nil || envelope.NeedsSyntheticHarnessSessionID()
	out, key, err := sessions.ResolveHarnessSessionID(envelope, turn.RootHash)
	if err != nil || !synthetic || turn.IdleGap <= 0 {
		return out, key, err
	}
	assigner, ok := driver.(SyntheticSessionAssigner)
	if !ok {
		return out, key, nil
	}
	split, err := assigner.AssignSyntheticSession(ctx, SyntheticSessionRequest{
		OrgID:        out.OrgID,
		HarnessID:    out.HarnessIDOrUnknown(),
		Conversation: key,
		RequestID:    turn.RequestID,
		CapturedAt:   turn.CapturedAt,
		IdleGap:      turn.IdleGap,
	})
	if err != nil {
		return out, key, fmt.Errorf("assign synthetic session split: %w", err)
	}
	out.HarnessSessionID = split
	return out, split, nil
}

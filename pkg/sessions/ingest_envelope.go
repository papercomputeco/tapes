package sessions

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// IngestEnvelope is the session-tracking envelope attached to a turn
// payload at the ingest HTTP boundary. It carries identity fields
// (org_id, auth_subject) plus the harness identifiers ingest uses to
// resolve a `sessions` row.
//
// IngestEnvelope is held in pkg/sessions (rather than the `ingest`
// package itself) so both the HTTP handler in `ingest` and the
// session-aware worker code under `proxy/worker` can reference the
// same type without inverting the existing import graph
// (proxy/worker is a dependency of ingest, not the other way around).
//
// Field semantics:
//
//   - OrgID, AuthSubject: identity fields; MUST be set on every
//     non-nil envelope. Empty values are not synthesized — they
//     are persisted verbatim so the row stays attributable.
//   - HarnessID: "claude" | "unknown" | other registered harness;
//     empty is normalized to "unknown".
//   - HarnessSessionID: opaque identifier for the harness session.
//     REQUIRED when HarnessID != "unknown". When absent (or when
//     HarnessID == "unknown"), ingest derives a synthetic id from
//     the captured turn's Merkle root prefix.
//   - ParentHarnessSessionID: fork-lineage hint, resolved server-side
//     to the parent's `sessions.id` within this envelope's harness
//     namespace. Parent and child are assumed to share a harness_id;
//     cross-harness forks are not supported (the envelope carries no
//     parent harness_id). When the parent's first turn hasn't landed
//     yet, ingest placeholder-inserts the parent so the FK can be set.
//   - HarnessMetadata: arbitrary JSON object merged into the
//     `sessions.harness_metadata` column last-write-wins per key.
type IngestEnvelope struct {
	OrgID                  string          `json:"org_id"`
	AuthSubject            string          `json:"auth_subject"`
	HarnessID              string          `json:"harness_id"`
	HarnessSessionID       string          `json:"harness_session_id,omitempty"`
	HarnessVersion         string          `json:"harness_version,omitempty"`
	Cwd                    string          `json:"cwd,omitempty"`
	Name                   string          `json:"name,omitempty"`
	ParentHarnessSessionID *string         `json:"parent_harness_session_id,omitempty"`
	HarnessMetadata        json.RawMessage `json:"harness_metadata,omitempty"`
}

// HarnessIDOrUnknown returns the canonical harness_id for this
// envelope: the verbatim HarnessID if set, otherwise the sentinel
// "unknown". Centralized here so both ingest and worker agree on the
// normalization.
func (e *IngestEnvelope) HarnessIDOrUnknown() string {
	if e == nil || e.HarnessID == "" {
		return "unknown"
	}
	return e.HarnessID
}

// Validate enforces wire-boundary invariants that the decoder cannot
// express in struct tags. It MUST be called at the ingest HTTP
// boundary, after JSON decode but before the envelope is handed to the
// worker / SessionIngester.
//
// Rejected shapes (each maps to a 400-equivalent at the HTTP layer):
//
//   - OrgID set but not parseable as a UUID. Empty OrgID is permitted
//     (the storage layer maps it to a nil-UUID sentinel for callers
//     without an org-scoped identity); a non-empty malformed value
//     would otherwise pass the HTTP boundary and fail asynchronously
//     in the worker, silently dropping the turn.
//   - ParentHarnessSessionID present but pointing at "". The pointer
//     model treats nil as "absent"; an explicit empty string is
//     ambiguous between "absent" and "this session has no parent",
//     which the ingest path would silently coerce to absent. Force
//     the caller to omit the field instead.
//   - HarnessMetadata that does not decode to a JSON object. The
//     Postgres `||` JSONB operator on sessions.harness_metadata is
//     defined as merge-objects; arrays concatenate and scalars
//     replace, producing surprising column shapes.
func (e *IngestEnvelope) Validate() error {
	if e == nil {
		return nil
	}
	if e.OrgID != "" {
		if _, err := uuid.Parse(e.OrgID); err != nil {
			return fmt.Errorf("org_id is not a valid UUID: %w", err)
		}
	}
	if e.ParentHarnessSessionID != nil && *e.ParentHarnessSessionID == "" {
		return errors.New("parent_harness_session_id is present but empty; omit the field instead")
	}
	if len(e.HarnessMetadata) > 0 {
		var probe map[string]any
		if err := json.Unmarshal(e.HarnessMetadata, &probe); err != nil {
			return fmt.Errorf("harness_metadata is not a JSON object: %w", err)
		}
	}
	return nil
}

// NeedsSyntheticHarnessSessionID reports whether ingest must derive a
// synthetic harness_session_id from the captured turn's Merkle root
// prefix instead of using the envelope-supplied value. Returns true
// when:
//   - the envelope is nil (no session block at all);
//   - HarnessID is "unknown" / empty; or
//   - HarnessSessionID is empty for any other reason.
func (e *IngestEnvelope) NeedsSyntheticHarnessSessionID() bool {
	if e == nil {
		return true
	}
	if e.HarnessIDOrUnknown() == "unknown" {
		return true
	}
	return e.HarnessSessionID == ""
}

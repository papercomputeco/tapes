package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Session-reflection persistence (PCC-241): one reflection row per (org_id, session_id),
// upserted by the /v1/sessions/{id}/reflection generate handler and read back by
// its GET and cache-check paths. Mirrors the skills.go wrapper idiom.

// UpsertSessionReflection inserts or replaces a session's reflection and returns the
// persisted record. Regeneration overwrites wholesale (latest wins).
func (d *Driver) UpsertSessionReflection(ctx context.Context, orgID string, rec storage.SessionReflectionRecord) (*storage.SessionReflectionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("upsert session reflection: %w", err)
	}
	sid, err := skillUUID(rec.SessionID)
	if err != nil {
		return nil, fmt.Errorf("upsert session reflection: %w", err)
	}
	if !sid.Valid {
		return nil, errors.New("upsert session reflection: session id is required")
	}

	row, err := d.q.UpsertSessionReflection(ctx, gensqlc.UpsertSessionReflectionParams{
		OrgID:       oid,
		SessionID:   sid,
		Narrative:   rec.Narrative,
		Observation: rec.Observation,
		TurnCount:   int32(rec.TurnCount), //nolint:gosec // turn counts are far below int32 max
		Model:       rec.Model,
		GeneratedAt: pgtype.Timestamptz{Time: rec.GeneratedAt, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("upsert session reflection: %w", err)
	}
	out := reflectionRecordFromRow(row)
	return &out, nil
}

// GetSessionReflection returns a session's reflection by its org-scoped session id, or
// nil when none has been generated yet.
func (d *Driver) GetSessionReflection(ctx context.Context, orgID, sessionID string) (*storage.SessionReflectionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get session reflection: %w", err)
	}
	sid, err := skillUUID(sessionID)
	if err != nil || !sid.Valid {
		// A malformed/empty id is simply "not found" from the caller's view.
		return nil, nil //nolint:nilerr // invalid id == absent reflection
	}
	row, err := d.q.GetSessionReflection(ctx, gensqlc.GetSessionReflectionParams{
		OrgID:     oid,
		SessionID: sid,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get session reflection: %w", err)
	}
	out := reflectionRecordFromRow(row)
	return &out, nil
}

// reflectionRecordFromRow maps the generated row type onto the storage record,
// flattening pgtype wrappers so API callers never unwrap them.
func reflectionRecordFromRow(row gensqlc.SessionReflection) storage.SessionReflectionRecord {
	return storage.SessionReflectionRecord{
		SessionID:   uuidString(row.SessionID),
		Narrative:   row.Narrative,
		Observation: row.Observation,
		TurnCount:   int(row.TurnCount),
		Model:       row.Model,
		GeneratedAt: row.GeneratedAt.Time,
	}
}

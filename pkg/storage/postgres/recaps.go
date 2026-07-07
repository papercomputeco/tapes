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

// Session-recap persistence (PCC-241): one recap row per (org_id, session_id),
// upserted by the /v1/sessions/{id}/recap generate handler and read back by
// its GET and cache-check paths. Mirrors the skills.go wrapper idiom.

// UpsertSessionRecap inserts or replaces a session's recap and returns the
// persisted record. Regeneration overwrites wholesale (latest wins).
func (d *Driver) UpsertSessionRecap(ctx context.Context, orgID string, rec storage.SessionRecapRecord) (*storage.SessionRecapRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("upsert session recap: %w", err)
	}
	sid, err := skillUUID(rec.SessionID)
	if err != nil {
		return nil, fmt.Errorf("upsert session recap: %w", err)
	}
	if !sid.Valid {
		return nil, errors.New("upsert session recap: session id is required")
	}

	row, err := d.q.UpsertSessionRecap(ctx, gensqlc.UpsertSessionRecapParams{
		OrgID:       oid,
		SessionID:   sid,
		Narrative:   rec.Narrative,
		Observation: rec.Observation,
		TurnCount:   int32(rec.TurnCount), //nolint:gosec // turn counts are far below int32 max
		Model:       rec.Model,
		GeneratedAt: pgtype.Timestamptz{Time: rec.GeneratedAt, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("upsert session recap: %w", err)
	}
	out := recapRecordFromRow(row)
	return &out, nil
}

// GetSessionRecap returns a session's recap by its org-scoped session id, or
// nil when none has been generated yet.
func (d *Driver) GetSessionRecap(ctx context.Context, orgID, sessionID string) (*storage.SessionRecapRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get session recap: %w", err)
	}
	sid, err := skillUUID(sessionID)
	if err != nil || !sid.Valid {
		// A malformed/empty id is simply "not found" from the caller's view.
		return nil, nil //nolint:nilerr // invalid id == absent recap
	}
	row, err := d.q.GetSessionRecap(ctx, gensqlc.GetSessionRecapParams{
		OrgID:     oid,
		SessionID: sid,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get session recap: %w", err)
	}
	out := recapRecordFromRow(row)
	return &out, nil
}

// recapRecordFromRow maps the generated row type onto the storage record,
// flattening pgtype wrappers so API callers never unwrap them.
func recapRecordFromRow(row gensqlc.SessionRecap) storage.SessionRecapRecord {
	return storage.SessionRecapRecord{
		SessionID:   uuidString(row.SessionID),
		Narrative:   row.Narrative,
		Observation: row.Observation,
		TurnCount:   int(row.TurnCount),
		Model:       row.Model,
		GeneratedAt: row.GeneratedAt.Time,
	}
}

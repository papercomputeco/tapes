package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// pgForeignKeyViolation is the Postgres SQLSTATE for an FK breach (23503),
// used to turn "saved a session that doesn't exist" into a nil record
// rather than a 500.
const pgForeignKeyViolation = "23503"

// SaveSession records the org-wide saved marker for a session. Idempotent:
// re-saving preserves the first saver's saved_by/saved_at. Returns (nil,
// nil) when the session id is malformed or no such session exists in the
// org — the handler maps that to 404.
func (d *Driver) SaveSession(ctx context.Context, orgID, sessionID, savedBy string) (*storage.SavedSessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("save session: %w", err)
	}
	parsed, err := uuid.Parse(sessionID)
	if err != nil {
		// A malformed id addresses no session — "not found", not an error.
		return nil, nil //nolint:nilerr // invalid id == absent session
	}

	row, err := d.q.SaveSession(ctx, gensqlc.SaveSessionParams{
		OrgID:     oid,
		SessionID: pgtype.UUID{Bytes: parsed, Valid: true},
		SavedBy:   savedBy,
		Now:       pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation {
			return nil, nil
		}
		return nil, fmt.Errorf("save session: %w", err)
	}
	out := savedSessionRecordFromRow(row)
	return &out, nil
}

// UnsaveSession removes the org-wide marker for everyone. Returns whether a
// row was actually deleted (false = already absent; both are success).
func (d *Driver) UnsaveSession(ctx context.Context, orgID, sessionID string) (bool, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return false, fmt.Errorf("unsave session: %w", err)
	}
	parsed, err := uuid.Parse(sessionID)
	if err != nil {
		return false, nil //nolint:nilerr // invalid id == nothing to delete
	}
	rows, err := d.q.UnsaveSession(ctx, gensqlc.UnsaveSessionParams{
		OrgID:     oid,
		SessionID: pgtype.UUID{Bytes: parsed, Valid: true},
	})
	if err != nil {
		return false, fmt.Errorf("unsave session: %w", err)
	}
	return rows > 0, nil
}

// ListSavedSessions returns every saved marker in the org,
// newest-saved-first. Unpaginated — a curated shortlist stays small.
func (d *Driver) ListSavedSessions(ctx context.Context, orgID string) ([]storage.SavedSessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list saved sessions: %w", err)
	}
	rows, err := d.q.ListSavedSessions(ctx, oid)
	if err != nil {
		return nil, fmt.Errorf("list saved sessions: %w", err)
	}
	out := make([]storage.SavedSessionRecord, len(rows))
	for i, row := range rows {
		out[i] = savedSessionRecordFromRow(row)
	}
	return out, nil
}

func savedSessionRecordFromRow(row gensqlc.SavedSession) storage.SavedSessionRecord {
	return storage.SavedSessionRecord{
		SessionID: uuidToString(row.SessionID),
		SavedBy:   row.SavedBy,
		SavedAt:   row.SavedAt.Time,
	}
}

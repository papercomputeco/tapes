package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// ListExperimentalSessions returns a page of sessions for an org ordered by
// last_seen_at DESC. Pass nil cursorTs/cursorID to start from the beginning.
func (d *Driver) ListExperimentalSessions(
	ctx context.Context,
	orgID string,
	limit int,
	cursorTs *time.Time,
	cursorID *string,
) ([]storage.ExperimentalSession, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list experimental sessions: %w", err)
	}
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}

	var tsPg pgtype.Timestamptz
	var idPg pgtype.UUID
	if cursorTs != nil && cursorID != nil && *cursorID != "" {
		tsPg = pgtype.Timestamptz{Time: *cursorTs, Valid: true}
		parsed, err := uuid.Parse(*cursorID)
		if err != nil {
			return nil, fmt.Errorf("list experimental sessions: invalid cursor id: %w", err)
		}
		idPg = pgtype.UUID{Bytes: parsed, Valid: true}
	}

	rows, err := d.q.ListExperimentalSessions(ctx, gensqlc.ListExperimentalSessionsParams{
		OrgID:    oid,
		CursorTs: tsPg,
		CursorID: idPg,
		Lim:      int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list experimental sessions: %w", err)
	}

	out := make([]storage.ExperimentalSession, len(rows))
	for i, row := range rows {
		out[i] = experimentalSessionFromRow(row)
	}
	return out, nil
}

// GetExperimentalSessionByID returns a single session by its UUID, or nil if not found.
func (d *Driver) GetExperimentalSessionByID(ctx context.Context, orgID, id string) (*storage.ExperimentalSession, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get experimental session: %w", err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("get experimental session: invalid id %q: %w", id, err)
	}
	row, err := d.q.GetExperimentalSessionByID(ctx, gensqlc.GetExperimentalSessionByIDParams{
		OrgID: oid,
		ID:    pgtype.UUID{Bytes: parsed, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get experimental session: %w", err)
	}
	s := experimentalSessionFromRow(row)
	return &s, nil
}

// ListNodesBySession returns all nodes attributed to a session ordered by
// created_at ASC (chronological order).
func (d *Driver) ListNodesBySession(ctx context.Context, sessionID string) ([]*merkle.Node, error) {
	parsed, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, fmt.Errorf("list nodes by session: invalid session id %q: %w", sessionID, err)
	}
	rows, err := d.q.ListNodesBySession(ctx, pgtype.UUID{Bytes: parsed, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("list nodes by session: %w", err)
	}
	return merkleNodesFromRows(rows)
}

// experimentalSessionFromRow converts a sqlc-generated Session row to
// the storage-level ExperimentalSession type.
func experimentalSessionFromRow(row gensqlc.Session) storage.ExperimentalSession {
	s := storage.ExperimentalSession{
		ID:                uuidToString(row.ID),
		HarnessID:         row.HarnessID,
		HarnessSessionID:  row.HarnessSessionID,
		TotalInputTokens:  row.TotalInputTokens,
		TotalOutputTokens: row.TotalOutputTokens,
		TurnCount:         int(row.TurnCount),
	}
	if row.Name.Valid {
		s.Name = row.Name.String
	}
	if row.Cwd.Valid {
		s.Cwd = row.Cwd.String
	}
	if row.HarnessVersion.Valid {
		s.HarnessVersion = row.HarnessVersion.String
	}
	if row.ParentSessionID.Valid {
		s.ParentSessionID = uuidToString(row.ParentSessionID)
	}
	if row.StartedAt.Valid {
		s.StartedAt = row.StartedAt.Time
	}
	if row.LastSeenAt.Valid {
		s.LastSeenAt = row.LastSeenAt.Time
	}
	if row.EndedAt.Valid {
		t := row.EndedAt.Time
		s.EndedAt = &t
	}
	if len(row.HarnessMetadata) > 0 {
		var m map[string]any
		if err := json.Unmarshal(row.HarnessMetadata, &m); err == nil {
			s.HarnessMetadata = m
		}
	}
	if row.TotalCostUsd.Valid {
		if f, err := row.TotalCostUsd.Float64Value(); err == nil && f.Valid {
			s.TotalCostUsd = f.Float64
		}
	}
	return s
}

// uuidToString converts a pgtype.UUID to its canonical string form.
// Returns empty string for invalid (null) UUIDs.
func uuidToString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return uuid.UUID(id.Bytes).String()
}

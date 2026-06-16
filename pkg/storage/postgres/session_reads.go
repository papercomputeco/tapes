package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// ListSessionRecords returns a page of sessions for an org ordered by
// last_seen_at DESC, optionally windowed by activity (last_seen_at)
// and narrowed to one gateway-stamped JWT subject (exact match on the
// indexed column; empty lists every user's sessions). Pass zero-value
// opts to start from the beginning, unwindowed and unfiltered.
func (d *Driver) ListSessionRecords(
	ctx context.Context,
	orgID string,
	opts storage.SessionListOpts,
) ([]storage.SessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list session records: %w", err)
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}

	var tsPg pgtype.Timestamptz
	var idPg pgtype.UUID
	if opts.CursorTs != nil && opts.CursorID != nil && *opts.CursorID != "" {
		tsPg = pgtype.Timestamptz{Time: *opts.CursorTs, Valid: true}
		parsed, err := uuid.Parse(*opts.CursorID)
		if err != nil {
			return nil, fmt.Errorf("list session records: invalid cursor id: %w", err)
		}
		idPg = pgtype.UUID{Bytes: parsed, Valid: true}
	}

	var subjectPg pgtype.Text
	if opts.AuthSubject != "" {
		subjectPg = pgtype.Text{String: opts.AuthSubject, Valid: true}
	}

	rows, err := d.q.ListSessionRecords(ctx, gensqlc.ListSessionRecordsParams{
		OrgID:       oid,
		AuthSubject: subjectPg,
		CursorTs:    tsPg,
		CursorID:    idPg,
		SinceFilter: nullTimePtr(opts.Since),
		UntilFilter: nullTimePtr(opts.Until),
		Lim:         int32(limit), //nolint:gosec // bounded above by the API handler (maxSessionsLimit)
	})
	if err != nil {
		return nil, fmt.Errorf("list session records: %w", err)
	}

	out := make([]storage.SessionRecord, len(rows))
	for i, row := range rows {
		out[i] = sessionRecordFromRow(row)
	}

	d.attachPreviews(ctx, out)

	return out, nil
}

const sessionPreviewMaxRunes = 120

// attachPreviews populates Preview on each record in place from a single
// batched preview query. It owns the best-effort policy for session reads:
// previews are decoration, so a fetch failure is logged and the records are
// returned without previews rather than failing the read.
func (d *Driver) attachPreviews(ctx context.Context, records []storage.SessionRecord) {
	previews, err := d.getSessionPreviews(ctx, records)
	if err != nil {
		slog.WarnContext(ctx, "attach session previews", "error", err)
		return
	}
	for i := range records {
		records[i].Preview = previews[records[i].ID]
	}
}

// getSessionPreviews fetches the first turn's user prompt for each
// session in the supplied list, in a single query. span_turns.user_prompt
// is the derive-time-cleaned prompt (injected harness context such as
// Claude Code's <system-reminder> claudeMd blocks already stripped), so
// it serves as the preview verbatim. Returns a map of session UUID
// string → truncated preview. Reading from span_turns (keyed by
// session_id) also sidesteps the legacy node path's cross-session
// content-collapse, where a shared-content node could be attributed to
// the wrong session.
func (d *Driver) getSessionPreviews(ctx context.Context, sessions []storage.SessionRecord) (map[string]string, error) {
	if len(sessions) == 0 {
		return nil, nil
	}
	ids := make([]string, len(sessions))
	for i, s := range sessions {
		ids[i] = s.ID
	}

	rows, err := d.conn.Query(ctx, `
SELECT DISTINCT ON (session_id) session_id::text, user_prompt
FROM span_turns
WHERE session_id = ANY($1::uuid[])
ORDER BY session_id, started_at ASC
`, ids)
	if err != nil {
		return nil, fmt.Errorf("get session previews: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(sessions))
	for rows.Next() {
		var sessionID, userPrompt string
		if err := rows.Scan(&sessionID, &userPrompt); err != nil {
			continue
		}
		text := strings.TrimSpace(userPrompt)
		if utf8.RuneCountInString(text) > sessionPreviewMaxRunes {
			runes := []rune(text)
			text = string(runes[:sessionPreviewMaxRunes])
		}
		out[sessionID] = text
	}
	return out, rows.Err()
}

// GetSessionRecord returns a single session by its UUID, or nil if not found.
func (d *Driver) GetSessionRecord(ctx context.Context, orgID, id string) (*storage.SessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get session record: %w", err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("get session record: invalid id %q: %w", id, err)
	}
	row, err := d.q.GetSessionRecord(ctx, gensqlc.GetSessionRecordParams{
		OrgID: oid,
		ID:    pgtype.UUID{Bytes: parsed, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get session record: %w", err)
	}
	s := sessionRecordFromRow(row)
	return &s, nil
}

// GetSessionRecordByHarness returns the single session matching the
// org-scoped natural key (org_id, harness_id, harness_session_id), or nil
// if no row matches. The lookup is an exact-match point read on the
// sessions_harness_uq unique index, mirroring the GetSessionRecord
// nil-on-no-rows contract.
func (d *Driver) GetSessionRecordByHarness(
	ctx context.Context,
	orgID string,
	harnessID string,
	harnessSessionID string,
) (*storage.SessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get session record by harness: %w", err)
	}
	row, err := d.q.GetSessionByNaturalKey(ctx, gensqlc.GetSessionByNaturalKeyParams{
		OrgID:            oid,
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get session record by harness: %w", err)
	}
	recs := []storage.SessionRecord{sessionRecordFromRow(row)}
	d.attachPreviews(ctx, recs)
	return &recs[0], nil
}

// sessionRecordFromRow converts a sqlc-generated Session row to
// the storage-level SessionRecord type.
func sessionRecordFromRow(row gensqlc.Session) storage.SessionRecord {
	s := storage.SessionRecord{
		ID:                uuidToString(row.ID),
		HarnessID:         row.HarnessID,
		HarnessSessionID:  row.HarnessSessionID,
		TotalInputTokens:  row.TotalInputTokens,
		TotalOutputTokens: row.TotalOutputTokens,
		TurnCount:         int(row.TurnCount),
		DerivedStatus:     row.DerivedStatus,
		Model:             row.DerivedModel,
		AuthSubject:       row.AuthSubject,
	}
	// The folded title-gen output is the session's display title; the
	// envelope's internal name (a plan slug for Claude Code) is the
	// fallback. See the derived_title migration.
	if row.DerivedTitle.Valid && row.DerivedTitle.String != "" {
		s.Name = row.DerivedTitle.String
	} else if row.Name.Valid {
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
	if len(row.ModelUsage) > 0 {
		var mu []storage.ModelUsage
		if err := json.Unmarshal(row.ModelUsage, &mu); err == nil {
			s.ModelUsage = mu
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

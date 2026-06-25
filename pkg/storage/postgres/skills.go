package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// pgUniqueViolation is the Postgres SQLSTATE for a unique-constraint breach
// (23505), used to turn a duplicate skill-version insert into a typed conflict.
const pgUniqueViolation = "23505"

// skillUUID parses a skill/parent id string into a pgtype.UUID. An empty string
// yields an invalid (NULL) value so optional ids (parent_id) round-trip cleanly.
func skillUUID(id string) (pgtype.UUID, error) {
	if id == "" {
		return pgtype.UUID{}, nil
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("invalid id %q: %w", id, err)
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}, nil
}

// pgText wraps a non-empty string as a valid pgtype.Text, leaving the empty
// string as SQL NULL so the nullable query predicates (search/scope) disable.
func pgText(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

// UpsertSkill inserts or replaces a skill keyed by (org_id, id) and returns the
// persisted record. Create/generate/duplicate pass a freshly minted id (a plain
// insert); PUT/publish pass the existing id (an update). created_at is preserved.
func (d *Driver) UpsertSkill(ctx context.Context, orgID string, rec storage.SkillRecord) (*storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("upsert skill: %w", err)
	}
	id, err := skillUUID(rec.ID)
	if err != nil {
		return nil, fmt.Errorf("upsert skill: %w", err)
	}
	if !id.Valid {
		return nil, errors.New("upsert skill: id is required")
	}
	parentID, err := skillUUID(rec.ParentID)
	if err != nil {
		return nil, fmt.Errorf("upsert skill: %w", err)
	}

	row, err := d.q.UpsertSkill(ctx, gensqlc.UpsertSkillParams{
		OrgID:                   oid,
		ID:                      id,
		Slug:                    rec.Slug,
		Name:                    rec.Name,
		Description:             rec.Description,
		Type:                    rec.Type,
		Version:                 rec.Version,
		Visibility:              rec.Visibility,
		Tags:                    nonNilStrings(rec.Tags),
		Content:                 rec.Content,
		IsAiGenerated:           rec.IsAIGenerated,
		GeneratedFromSessionIds: nonNilStrings(rec.GeneratedFromSessionIDs),
		ParentID:                parentID,
		AuthorSubject:           rec.AuthorSubject,
		CreatedAt:               pgtype.Timestamptz{Time: rec.CreatedAt, Valid: true},
		UpdatedAt:               pgtype.Timestamptz{Time: rec.UpdatedAt, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("upsert skill: %w", err)
	}
	out := skillRecordFromRow(row)
	return &out, nil
}

// GetSkill returns a single skill by its org-scoped id, or nil if not found.
func (d *Driver) GetSkill(ctx context.Context, orgID, id string) (*storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get skill: %w", err)
	}
	sid, err := skillUUID(id)
	if err != nil || !sid.Valid {
		// A malformed/empty id is simply "not found" from the caller's view.
		return nil, nil //nolint:nilerr // invalid id == absent skill
	}
	row, err := d.q.GetSkillByID(ctx, gensqlc.GetSkillByIDParams{
		OrgID: oid,
		ID:    sid,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get skill: %w", err)
	}
	out := skillRecordFromRow(row)
	return &out, nil
}

// DeleteSkill removes a skill and its published history by id. Returns whether a
// skill row was actually deleted (false when the id was already absent).
func (d *Driver) DeleteSkill(ctx context.Context, orgID, id string) (bool, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return false, fmt.Errorf("delete skill: %w", err)
	}
	sid, err := skillUUID(id)
	if err != nil || !sid.Valid {
		return false, nil // invalid id == nothing to delete
	}

	// Drop history and the skill row in one transaction: skill_versions has no
	// FK cascade to skills, so two separate statements could destroy version
	// history and then fail to remove the skill, leaving a silent partial state.
	tx, err := d.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin delete skill tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	qtx := d.q.WithTx(tx)

	if err := qtx.DeleteSkillVersions(ctx, gensqlc.DeleteSkillVersionsParams{
		OrgID:   oid,
		SkillID: sid,
	}); err != nil {
		return false, fmt.Errorf("delete skill versions: %w", err)
	}
	n, err := qtx.DeleteSkill(ctx, gensqlc.DeleteSkillParams{
		OrgID: oid,
		ID:    sid,
	})
	if err != nil {
		return false, fmt.Errorf("delete skill: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit delete skill tx: %w", err)
	}
	return n > 0, nil
}

// skillRecordFromRow converts a sqlc-generated Skill row to the storage-level
// SkillRecord type.
func skillRecordFromRow(row gensqlc.Skill) storage.SkillRecord {
	return storage.SkillRecord{
		ID:                      uuidString(row.ID),
		Slug:                    row.Slug,
		Name:                    row.Name,
		Description:             row.Description,
		Type:                    row.Type,
		Version:                 row.Version,
		Visibility:              row.Visibility,
		Tags:                    row.Tags,
		Content:                 row.Content,
		IsAIGenerated:           row.IsAiGenerated,
		GeneratedFromSessionIDs: row.GeneratedFromSessionIds,
		ParentID:                uuidString(row.ParentID),
		AuthorSubject:           row.AuthorSubject,
		DownloadCount:           row.DownloadCount,
		CreatedAt:               row.CreatedAt.Time,
		UpdatedAt:               row.UpdatedAt.Time,
	}
}

// ListSkills returns one keyset page of skills for an org, honoring the optional
// search/scope filters, the requested sort, and the cursor in opts.
func (d *Driver) ListSkills(ctx context.Context, orgID string, opts storage.SkillListOpts) ([]storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}

	var cursorID pgtype.UUID
	if opts.CursorID != "" {
		cursorID, err = skillUUID(opts.CursorID)
		if err != nil {
			return nil, fmt.Errorf("list skills: %w", err)
		}
	}

	var rows []gensqlc.Skill
	if opts.Sort == storage.SkillSortDownloads {
		var cursorDownloads pgtype.Int8
		if opts.CursorDownloads != nil {
			cursorDownloads = pgtype.Int8{Int64: *opts.CursorDownloads, Valid: true}
		}
		rows, err = d.q.ListSkillsPageByDownloads(ctx, gensqlc.ListSkillsPageByDownloadsParams{
			OrgID:           oid,
			Query:           pgText(opts.Query),
			Author:          pgText(opts.Author),
			NotAuthor:       pgText(opts.NotAuthor),
			CursorDownloads: cursorDownloads,
			CursorID:        cursorID,
			Lim:             int32(limit), //nolint:gosec // bounded above by the API handler
		})
	} else {
		var cursorTs pgtype.Timestamptz
		if opts.CursorTs != nil {
			cursorTs = pgtype.Timestamptz{Time: *opts.CursorTs, Valid: true}
		}
		rows, err = d.q.ListSkillsPage(ctx, gensqlc.ListSkillsPageParams{
			OrgID:     oid,
			Query:     pgText(opts.Query),
			Author:    pgText(opts.Author),
			NotAuthor: pgText(opts.NotAuthor),
			CursorTs:  cursorTs,
			CursorID:  cursorID,
			Lim:       int32(limit), //nolint:gosec // bounded above by the API handler
		})
	}
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	out := make([]storage.SkillRecord, len(rows))
	for i, row := range rows {
		out[i] = skillRecordFromRow(row)
	}
	return out, nil
}

// ListSkillsBySession returns the skills generated from a given session (reverse
// lookup over the provenance array), newest-edited first.
func (d *Driver) ListSkillsBySession(ctx context.Context, orgID, sessionID string) ([]storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list session skills: %w", err)
	}
	rows, err := d.q.ListSessionSkills(ctx, gensqlc.ListSessionSkillsParams{
		OrgID:     oid,
		SessionID: sessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("list session skills: %w", err)
	}
	out := make([]storage.SkillRecord, len(rows))
	for i, row := range rows {
		out[i] = skillRecordFromRow(row)
	}
	return out, nil
}

// CountSkills returns the per-tab totals for a search (ignoring scope/cursor):
// every matching skill and how many the caller authored.
func (d *Driver) CountSkills(ctx context.Context, orgID, query, author string) (storage.SkillCounts, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return storage.SkillCounts{}, fmt.Errorf("count skills: %w", err)
	}
	row, err := d.q.CountSkills(ctx, gensqlc.CountSkillsParams{
		OrgID:  oid,
		Query:  pgText(query),
		Author: author,
	})
	if err != nil {
		return storage.SkillCounts{}, fmt.Errorf("count skills: %w", err)
	}
	return storage.SkillCounts{Total: row.Total, Mine: row.Mine}, nil
}

// NextSkillVersionNumber returns the next monotonic version number for a skill
// (1 when nothing is published yet).
func (d *Driver) NextSkillVersionNumber(ctx context.Context, orgID, skillID string) (int, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return 0, fmt.Errorf("next skill version: %w", err)
	}
	sid, err := skillUUID(skillID)
	if err != nil {
		return 0, fmt.Errorf("next skill version: %w", err)
	}
	maxN, err := d.q.MaxSkillVersionNumber(ctx, gensqlc.MaxSkillVersionNumberParams{
		OrgID:   oid,
		SkillID: sid,
	})
	if err != nil {
		return 0, fmt.Errorf("next skill version: %w", err)
	}
	return int(maxN) + 1, nil
}

// CreateSkillVersion appends an immutable published snapshot and returns it.
func (d *Driver) CreateSkillVersion(ctx context.Context, orgID string, rec storage.SkillVersionRecord) (*storage.SkillVersionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("create skill version: %w", err)
	}
	sid, err := skillUUID(rec.SkillID)
	if err != nil {
		return nil, fmt.Errorf("create skill version: %w", err)
	}
	row, err := d.q.CreateSkillVersion(ctx, gensqlc.CreateSkillVersionParams{
		OrgID:         oid,
		SkillID:       sid,
		VersionNumber: int32(rec.VersionNumber), //nolint:gosec // small monotonic counter
		Semver:        rec.Semver,
		Changelog:     rec.Changelog,
		Content:       rec.Content,
		AuthorSubject: rec.AuthorSubject,
		PublishedAt:   pgtype.Timestamptz{Time: rec.PublishedAt, Valid: true},
	})
	if err != nil {
		// A concurrent publish already claimed this version number; surface a
		// typed conflict so the handler can recompute and retry instead of 500.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, storage.ErrSkillVersionConflict
		}
		return nil, fmt.Errorf("create skill version: %w", err)
	}
	out := skillVersionFromRow(row)
	return &out, nil
}

// SetSkillVersion bumps a skill's current published semver and updated_at.
func (d *Driver) SetSkillVersion(ctx context.Context, orgID, skillID, semver string, updatedAt time.Time) error {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return fmt.Errorf("set skill version: %w", err)
	}
	sid, err := skillUUID(skillID)
	if err != nil {
		return fmt.Errorf("set skill version: %w", err)
	}
	return d.q.SetSkillVersion(ctx, gensqlc.SetSkillVersionParams{
		Version:   semver,
		UpdatedAt: pgtype.Timestamptz{Time: updatedAt, Valid: true},
		OrgID:     oid,
		ID:        sid,
	})
}

// IncrementSkillDownloads bumps the real download counter for a skill.
func (d *Driver) IncrementSkillDownloads(ctx context.Context, orgID, id string) error {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return fmt.Errorf("increment skill downloads: %w", err)
	}
	sid, err := skillUUID(id)
	if err != nil {
		return fmt.Errorf("increment skill downloads: %w", err)
	}
	return d.q.IncrementSkillDownloads(ctx, gensqlc.IncrementSkillDownloadsParams{
		OrgID: oid,
		ID:    sid,
	})
}

// ListSkillVersions returns a skill's published history, newest first.
func (d *Driver) ListSkillVersions(ctx context.Context, orgID, skillID string) ([]storage.SkillVersionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list skill versions: %w", err)
	}
	sid, err := skillUUID(skillID)
	if err != nil {
		return nil, fmt.Errorf("list skill versions: %w", err)
	}
	rows, err := d.q.ListSkillVersions(ctx, gensqlc.ListSkillVersionsParams{
		OrgID:   oid,
		SkillID: sid,
	})
	if err != nil {
		return nil, fmt.Errorf("list skill versions: %w", err)
	}
	out := make([]storage.SkillVersionRecord, len(rows))
	for i, row := range rows {
		out[i] = skillVersionFromRow(row)
	}
	return out, nil
}

func skillVersionFromRow(row gensqlc.SkillVersion) storage.SkillVersionRecord {
	rec := storage.SkillVersionRecord{
		SkillID:       uuidString(row.SkillID),
		VersionNumber: int(row.VersionNumber),
		Semver:        row.Semver,
		Changelog:     row.Changelog,
		Content:       row.Content,
		AuthorSubject: row.AuthorSubject,
	}
	if row.PublishedAt.Valid {
		rec.PublishedAt = row.PublishedAt.Time
	}
	return rec
}

// nonNilStrings returns a non-nil empty slice for nil input. The tags and
// generated_from_session_ids columns are NOT NULL, and an explicit INSERT
// supplying nil would write NULL (the column DEFAULT only applies when the
// column is omitted), so guard against it here.
func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

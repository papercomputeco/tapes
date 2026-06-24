package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// UpsertSkill inserts or replaces a skill keyed by (org_id, slug) and returns
// the persisted record. Re-generating the same slug overwrites the mutable
// fields; created_at is preserved.
func (d *Driver) UpsertSkill(ctx context.Context, orgID string, rec storage.SkillRecord) (*storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("upsert skill: %w", err)
	}

	var parentSlug pgtype.Text
	if rec.ParentSlug != "" {
		parentSlug = pgtype.Text{String: rec.ParentSlug, Valid: true}
	}

	row, err := d.q.UpsertSkill(ctx, gensqlc.UpsertSkillParams{
		OrgID:                   oid,
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
		ParentSlug:              parentSlug,
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

// GetSkill returns a single skill by its org-scoped slug, or nil if not found.
func (d *Driver) GetSkill(ctx context.Context, orgID, slug string) (*storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get skill: %w", err)
	}
	row, err := d.q.GetSkillBySlug(ctx, gensqlc.GetSkillBySlugParams{
		OrgID: oid,
		Slug:  slug,
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

// skillRecordFromRow converts a sqlc-generated Skill row to the storage-level
// SkillRecord type.
func skillRecordFromRow(row gensqlc.Skill) storage.SkillRecord {
	rec := storage.SkillRecord{
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
		AuthorSubject:           row.AuthorSubject,
		DownloadCount:           row.DownloadCount,
	}
	if row.ParentSlug.Valid {
		rec.ParentSlug = row.ParentSlug.String
	}
	if row.CreatedAt.Valid {
		rec.CreatedAt = row.CreatedAt.Time
	}
	if row.UpdatedAt.Valid {
		rec.UpdatedAt = row.UpdatedAt.Time
	}
	return rec
}

// ListSkills returns all skills for an org, newest-edited first (capped). The
// console filters/sorts client-side over this list.
func (d *Driver) ListSkills(ctx context.Context, orgID string, limit int) ([]storage.SkillRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}
	rows, err := d.q.ListSkills(ctx, gensqlc.ListSkillsParams{
		OrgID: oid,
		Lim:   int32(limit), //nolint:gosec // bounded above by the API handler
	})
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	out := make([]storage.SkillRecord, len(rows))
	for i, row := range rows {
		out[i] = skillRecordFromRow(row)
	}
	return out, nil
}

// NextSkillVersionNumber returns the next monotonic version number for a skill
// (1 when nothing is published yet).
func (d *Driver) NextSkillVersionNumber(ctx context.Context, orgID, slug string) (int, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return 0, fmt.Errorf("next skill version: %w", err)
	}
	maxN, err := d.q.MaxSkillVersionNumber(ctx, gensqlc.MaxSkillVersionNumberParams{
		OrgID:     oid,
		SkillSlug: slug,
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
	row, err := d.q.CreateSkillVersion(ctx, gensqlc.CreateSkillVersionParams{
		OrgID:         oid,
		SkillSlug:     rec.SkillSlug,
		VersionNumber: int32(rec.VersionNumber), //nolint:gosec // small monotonic counter
		Semver:        rec.Semver,
		Changelog:     rec.Changelog,
		Content:       rec.Content,
		AuthorSubject: rec.AuthorSubject,
		PublishedAt:   pgtype.Timestamptz{Time: rec.PublishedAt, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("create skill version: %w", err)
	}
	out := skillVersionFromRow(row)
	return &out, nil
}

// SetSkillVersion bumps a skill's current published semver and updated_at.
func (d *Driver) SetSkillVersion(ctx context.Context, orgID, slug, semver string, updatedAt time.Time) error {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return fmt.Errorf("set skill version: %w", err)
	}
	return d.q.SetSkillVersion(ctx, gensqlc.SetSkillVersionParams{
		Version:   semver,
		UpdatedAt: pgtype.Timestamptz{Time: updatedAt, Valid: true},
		OrgID:     oid,
		Slug:      slug,
	})
}

// IncrementSkillDownloads bumps the real download counter for a skill.
func (d *Driver) IncrementSkillDownloads(ctx context.Context, orgID, slug string) error {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return fmt.Errorf("increment skill downloads: %w", err)
	}
	return d.q.IncrementSkillDownloads(ctx, gensqlc.IncrementSkillDownloadsParams{
		OrgID: oid,
		Slug:  slug,
	})
}

// ListSkillVersions returns a skill's published history, newest first.
func (d *Driver) ListSkillVersions(ctx context.Context, orgID, slug string) ([]storage.SkillVersionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list skill versions: %w", err)
	}
	rows, err := d.q.ListSkillVersions(ctx, gensqlc.ListSkillVersionsParams{
		OrgID:     oid,
		SkillSlug: slug,
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
		SkillSlug:     row.SkillSlug,
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

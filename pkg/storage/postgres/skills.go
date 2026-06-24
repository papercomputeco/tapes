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

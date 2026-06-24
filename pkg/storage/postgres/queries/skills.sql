-- name: UpsertSkill :one
-- Insert-or-replace a skill keyed by (org_id, slug). Generate calls this with
-- the freshly extracted skill; re-generating the same slug overwrites the
-- mutable fields and bumps updated_at. created_at is preserved on conflict.
INSERT INTO skills (
    org_id,
    slug,
    name,
    description,
    type,
    version,
    visibility,
    tags,
    content,
    is_ai_generated,
    generated_from_session_ids,
    parent_slug,
    created_at,
    updated_at
) VALUES (
    sqlc.arg(org_id),
    sqlc.arg(slug),
    sqlc.arg(name),
    sqlc.arg(description),
    sqlc.arg(type),
    sqlc.arg(version),
    sqlc.arg(visibility),
    sqlc.arg(tags),
    sqlc.arg(content),
    sqlc.arg(is_ai_generated),
    sqlc.arg(generated_from_session_ids),
    sqlc.narg(parent_slug),
    sqlc.arg(created_at),
    sqlc.arg(updated_at)
)
ON CONFLICT (org_id, slug) DO UPDATE
SET name                       = EXCLUDED.name,
    description                = EXCLUDED.description,
    type                       = EXCLUDED.type,
    version                    = EXCLUDED.version,
    visibility                 = EXCLUDED.visibility,
    tags                       = EXCLUDED.tags,
    content                    = EXCLUDED.content,
    is_ai_generated            = EXCLUDED.is_ai_generated,
    generated_from_session_ids = EXCLUDED.generated_from_session_ids,
    parent_slug                = EXCLUDED.parent_slug,
    updated_at                 = EXCLUDED.updated_at
RETURNING *;

-- name: GetSkillBySlug :one
-- Org-scoped point read used by GET /v1/skills/:slug.
SELECT * FROM skills
WHERE org_id = sqlc.arg(org_id) AND slug = sqlc.arg(slug);

-- name: UpsertSkill :one
-- Insert-or-replace a skill keyed by (org_id, slug). Generate creates it and
-- PUT saves edits through the same path; created_at and author_subject are
-- preserved on conflict (original creation time and creator stay authoritative).
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
    author_subject,
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
    sqlc.arg(author_subject),
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

-- name: DeleteSkill :execrows
-- Remove a skill by its org-scoped slug. Returns the affected row count so the
-- handler can distinguish a real delete from a missing slug.
DELETE FROM skills
WHERE org_id = sqlc.arg(org_id) AND slug = sqlc.arg(slug);

-- name: DeleteSkillVersions :exec
-- Remove a skill's published history. skill_versions has no FK cascade to
-- skills, so deleting a skill must drop its versions explicitly.
DELETE FROM skill_versions
WHERE org_id = sqlc.arg(org_id) AND skill_slug = sqlc.arg(skill_slug);

-- name: IncrementSkillDownloads :exec
-- Bump the real download counter when a skill's SKILL.md is downloaded.
UPDATE skills SET download_count = download_count + 1
WHERE org_id = sqlc.arg(org_id) AND slug = sqlc.arg(slug);

-- name: ListSkills :many
-- All skills for an org, newest-edited first. The console filters/sorts
-- client-side over this list, so a single capped page is sufficient.
SELECT * FROM skills
WHERE org_id = sqlc.arg(org_id)
ORDER BY updated_at DESC, slug DESC
LIMIT sqlc.arg(lim);

-- name: SetSkillVersion :exec
-- Bump the skill's current published semver (and updated_at) at publish time.
UPDATE skills
   SET version = sqlc.arg(version), updated_at = sqlc.arg(updated_at)
 WHERE org_id = sqlc.arg(org_id) AND slug = sqlc.arg(slug);

-- name: CreateSkillVersion :one
-- Append an immutable published snapshot of a skill's content.
INSERT INTO skill_versions (
    org_id,
    skill_slug,
    version_number,
    semver,
    changelog,
    content,
    author_subject,
    published_at
) VALUES (
    sqlc.arg(org_id),
    sqlc.arg(skill_slug),
    sqlc.arg(version_number),
    sqlc.arg(semver),
    sqlc.arg(changelog),
    sqlc.arg(content),
    sqlc.arg(author_subject),
    sqlc.arg(published_at)
)
RETURNING *;

-- name: ListSkillVersions :many
SELECT * FROM skill_versions
WHERE org_id = sqlc.arg(org_id) AND skill_slug = sqlc.arg(skill_slug)
ORDER BY version_number DESC;

-- name: MaxSkillVersionNumber :one
-- Highest version_number for a skill (0 when none published yet).
SELECT COALESCE(MAX(version_number), 0)::int AS max_version
FROM skill_versions
WHERE org_id = sqlc.arg(org_id) AND skill_slug = sqlc.arg(skill_slug);

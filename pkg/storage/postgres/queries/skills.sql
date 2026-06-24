-- name: UpsertSkill :one
-- Insert-or-replace a skill keyed by (org_id, id). Create/generate/duplicate
-- mint a fresh id (a plain insert); PUT and publish pass the existing id (an
-- update). slug is a cosmetic, non-unique display label, so it is overwritten
-- on update like any other mutable field. created_at and author_subject are
-- preserved on conflict (original creation time and creator stay authoritative).
INSERT INTO skills (
    org_id,
    id,
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
    parent_id,
    author_subject,
    created_at,
    updated_at
) VALUES (
    sqlc.arg(org_id),
    sqlc.arg(id),
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
    sqlc.narg(parent_id),
    sqlc.arg(author_subject),
    sqlc.arg(created_at),
    sqlc.arg(updated_at)
)
ON CONFLICT (org_id, id) DO UPDATE
SET slug                       = EXCLUDED.slug,
    name                       = EXCLUDED.name,
    description                = EXCLUDED.description,
    type                       = EXCLUDED.type,
    version                    = EXCLUDED.version,
    visibility                 = EXCLUDED.visibility,
    tags                       = EXCLUDED.tags,
    content                    = EXCLUDED.content,
    is_ai_generated            = EXCLUDED.is_ai_generated,
    generated_from_session_ids = EXCLUDED.generated_from_session_ids,
    parent_id                  = EXCLUDED.parent_id,
    updated_at                 = EXCLUDED.updated_at
RETURNING *;

-- name: GetSkillByID :one
-- Org-scoped point read used by GET /v1/skills/:id and the write handlers.
SELECT * FROM skills
WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: DeleteSkill :execrows
-- Remove a skill by its org-scoped id. Returns the affected row count so the
-- handler can distinguish a real delete from a missing id.
DELETE FROM skills
WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: DeleteSkillVersions :exec
-- Remove a skill's published history. skill_versions has no FK cascade to
-- skills, so deleting a skill must drop its versions explicitly.
DELETE FROM skill_versions
WHERE org_id = sqlc.arg(org_id) AND skill_id = sqlc.arg(skill_id);

-- name: IncrementSkillDownloads :exec
-- Bump the real download counter when a skill's SKILL.md is downloaded.
UPDATE skills SET download_count = download_count + 1
WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: ListSkillsPage :many
-- One keyset page of skills for an org, newest-edited first, with optional
-- full-text search (name/description/tags) and author scope. The cursor is the
-- (updated_at, id) of the last row on the previous page; the tiebreak on id
-- keeps pagination stable when updated_at ties. Fetch lim+1 to detect has_more.
SELECT * FROM skills
WHERE org_id = sqlc.arg(org_id)
  AND (
    sqlc.narg(query)::text IS NULL
    OR name ILIKE '%' || sqlc.narg(query)::text || '%'
    OR description ILIKE '%' || sqlc.narg(query)::text || '%'
    OR EXISTS (SELECT 1 FROM unnest(tags) tag WHERE tag ILIKE '%' || sqlc.narg(query)::text || '%')
  )
  AND (sqlc.narg(author)::text IS NULL OR author_subject = sqlc.narg(author)::text)
  AND (sqlc.narg(not_author)::text IS NULL OR author_subject <> sqlc.narg(not_author)::text)
  AND (
    sqlc.narg(cursor_ts)::timestamptz IS NULL
    OR updated_at < sqlc.narg(cursor_ts)::timestamptz
    OR (updated_at = sqlc.narg(cursor_ts)::timestamptz AND id < sqlc.narg(cursor_id)::uuid)
  )
ORDER BY updated_at DESC, id DESC
LIMIT sqlc.arg(lim);

-- name: CountSkills :one
-- Tab counts for the current search: total matching, and how many are authored
-- by the caller. "team" is derived client-side as total - mine. Counts ignore
-- scope and cursor so every tab shows its full size for the active query.
SELECT
    COUNT(*)::bigint AS total,
    COUNT(*) FILTER (WHERE author_subject = sqlc.arg(author))::bigint AS mine
FROM skills
WHERE org_id = sqlc.arg(org_id)
  AND (
    sqlc.narg(query)::text IS NULL
    OR name ILIKE '%' || sqlc.narg(query)::text || '%'
    OR description ILIKE '%' || sqlc.narg(query)::text || '%'
    OR EXISTS (SELECT 1 FROM unnest(tags) tag WHERE tag ILIKE '%' || sqlc.narg(query)::text || '%')
  );

-- name: SetSkillVersion :exec
-- Bump the skill's current published semver (and updated_at) at publish time.
UPDATE skills
   SET version = sqlc.arg(version), updated_at = sqlc.arg(updated_at)
 WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: CreateSkillVersion :one
-- Append an immutable published snapshot of a skill's content.
INSERT INTO skill_versions (
    org_id,
    skill_id,
    version_number,
    semver,
    changelog,
    content,
    author_subject,
    published_at
) VALUES (
    sqlc.arg(org_id),
    sqlc.arg(skill_id),
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
WHERE org_id = sqlc.arg(org_id) AND skill_id = sqlc.arg(skill_id)
ORDER BY version_number DESC;

-- name: MaxSkillVersionNumber :one
-- Highest version_number for a skill (0 when none published yet).
SELECT COALESCE(MAX(version_number), 0)::int AS max_version
FROM skill_versions
WHERE org_id = sqlc.arg(org_id) AND skill_id = sqlc.arg(skill_id);

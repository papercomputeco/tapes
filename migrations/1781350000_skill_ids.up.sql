-- Give skills an opaque, immutable id and make it the identity / route key,
-- mirroring sessions (opaque UUID PK). The human slug is demoted to a cosmetic
-- display label and SKILL.md filename: it no longer has to be unique, so a
-- duplicate no longer needs an "-copy" suffix to stay distinct — two skills can
-- share a slug and are told apart by id. Version history and duplicate
-- provenance are repointed from slug to id.
--
-- This runs against an existing (clearing/dev) database that already has the
-- slug-keyed schema, so it backfills ids before swapping the keys. gen_random_uuid()
-- is core Postgres (>= 13); no extension needed.

ALTER TABLE skills ADD COLUMN IF NOT EXISTS id UUID;
UPDATE skills SET id = gen_random_uuid() WHERE id IS NULL;
ALTER TABLE skills ALTER COLUMN id SET NOT NULL;

-- Repoint duplicate provenance (parent_slug -> parent_id) before dropping the slug key.
ALTER TABLE skills ADD COLUMN IF NOT EXISTS parent_id UUID;
UPDATE skills child SET parent_id = parent.id
    FROM skills parent
    WHERE child.parent_slug IS NOT NULL
      AND child.org_id = parent.org_id
      AND child.parent_slug = parent.slug;

-- Repoint version history (skill_slug -> skill_id).
ALTER TABLE skill_versions ADD COLUMN IF NOT EXISTS skill_id UUID;
UPDATE skill_versions v SET skill_id = s.id
    FROM skills s
    WHERE v.org_id = s.org_id AND v.skill_slug = s.slug;
ALTER TABLE skill_versions ALTER COLUMN skill_id SET NOT NULL;

-- Swap the primary keys onto the id. slug is now free to collide within an org.
ALTER TABLE skills DROP CONSTRAINT skills_pkey;
ALTER TABLE skills ADD CONSTRAINT skills_pkey PRIMARY KEY (org_id, id);

ALTER TABLE skill_versions DROP CONSTRAINT skill_versions_pkey;
ALTER TABLE skill_versions ADD CONSTRAINT skill_versions_pkey PRIMARY KEY (org_id, skill_id, version_number);
DROP INDEX IF EXISTS skill_versions_skill_idx;
CREATE INDEX IF NOT EXISTS skill_versions_skill_idx
    ON skill_versions (org_id, skill_id, version_number DESC);

-- Keyset pagination support for the list page: ORDER BY updated_at DESC, id DESC.
CREATE INDEX IF NOT EXISTS skills_org_updated_idx
    ON skills (org_id, updated_at DESC, id DESC);

-- The slug-keyed columns are superseded by parent_id / skill_id.
ALTER TABLE skills DROP COLUMN IF EXISTS parent_slug;
ALTER TABLE skill_versions DROP COLUMN IF EXISTS skill_slug;

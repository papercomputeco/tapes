-- Reverse the id-identity swap, restoring the slug-keyed schema. Data in the
-- dropped slug columns is reconstructed from the surviving rows; if duplicates
-- now share a slug the restored (org_id, slug) primary key will reject the
-- rollback, which is the expected hazard of having relied on non-unique slugs.

ALTER TABLE skill_versions ADD COLUMN IF NOT EXISTS skill_slug TEXT NOT NULL DEFAULT '';
ALTER TABLE skills ADD COLUMN IF NOT EXISTS parent_slug TEXT;

UPDATE skill_versions v SET skill_slug = s.slug
    FROM skills s
    WHERE v.org_id = s.org_id AND v.skill_id = s.id;
UPDATE skills child SET parent_slug = parent.slug
    FROM skills parent
    WHERE child.parent_id = parent.id AND child.org_id = parent.org_id;

ALTER TABLE skill_versions DROP CONSTRAINT skill_versions_pkey;
ALTER TABLE skill_versions ADD CONSTRAINT skill_versions_pkey PRIMARY KEY (org_id, skill_slug, version_number);
DROP INDEX IF EXISTS skill_versions_skill_idx;
CREATE INDEX IF NOT EXISTS skill_versions_skill_idx
    ON skill_versions (org_id, skill_slug, version_number DESC);

ALTER TABLE skills DROP CONSTRAINT skills_pkey;
ALTER TABLE skills ADD CONSTRAINT skills_pkey PRIMARY KEY (org_id, slug);

DROP INDEX IF EXISTS skills_org_updated_idx;
ALTER TABLE skill_versions DROP COLUMN IF EXISTS skill_id;
ALTER TABLE skills DROP COLUMN IF EXISTS parent_id;
ALTER TABLE skills DROP COLUMN IF EXISTS id;

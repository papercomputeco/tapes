-- skill_versions stores immutable published snapshots of a skill's content.
-- The working/current content lives on skills.content; publishing copies it
-- here under a monotonic version_number and a semver, leaving the skill row's
-- content as the editable head. This keeps "current content" in exactly one
-- place (the skill row) and treats versions purely as history.
--
-- author_subject is added to skills here too: the WorkOS user id (JWT `sub`)
-- of the creator, stamped from the gateway-trusted x-paper-auth-subject header
-- the same way sessions.auth_subject is captured at ingest. Nullable/empty for
-- rows written before attribution (or when no header is present).

ALTER TABLE skills
    ADD COLUMN IF NOT EXISTS author_subject TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS skill_versions (
    org_id         UUID NOT NULL,
    skill_slug     TEXT NOT NULL,
    version_number INT  NOT NULL,
    semver         TEXT NOT NULL,
    changelog      TEXT NOT NULL DEFAULT '',
    content        TEXT NOT NULL DEFAULT '',
    author_subject TEXT NOT NULL DEFAULT '',
    published_at   TIMESTAMPTZ NOT NULL,

    CONSTRAINT skill_versions_pkey PRIMARY KEY (org_id, skill_slug, version_number)
);

CREATE INDEX IF NOT EXISTS skill_versions_skill_idx
    ON skill_versions (org_id, skill_slug, version_number DESC);

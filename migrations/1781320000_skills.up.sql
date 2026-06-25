-- The skills table stores skills generated from agent sessions. A skill is
-- a reusable workflow / domain-knowledge / prompt-template extracted from one
-- or more sessions by the LLM generator in pkg/skill, surfaced and edited in
-- the console. The row mirrors the console's SkillDraft shape so the API can
-- round-trip a generated draft without a separate projection.
--
-- org_id is UUID NOT NULL but unconstrained — there is no `orgs` table in this
-- repo (same convention as the sessions table). Reads/writes are scoped to the
-- caller's org via the composite PK; the nil-UUID sentinel is the bucket for
-- callers that send no X-Tapes-Org-Id header.
--
-- slug is the kebab-case identifier the console uses as the URL segment
-- (/api/skills/<slug>). The PK is (org_id, slug) so the same slug can exist
-- under different orgs, and generate is an idempotent upsert on that key.

CREATE TABLE IF NOT EXISTS skills (
    org_id                     UUID NOT NULL,
    slug                       TEXT NOT NULL,
    name                       TEXT NOT NULL,
    description                TEXT NOT NULL DEFAULT '',
    type                       TEXT NOT NULL DEFAULT 'workflow',
    version                    TEXT NOT NULL DEFAULT '0.1.0',
    visibility                 TEXT NOT NULL DEFAULT 'private',
    tags                       TEXT[] NOT NULL DEFAULT '{}',
    content                    TEXT NOT NULL DEFAULT '',
    is_ai_generated            BOOLEAN NOT NULL DEFAULT FALSE,
    generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
    parent_slug                TEXT,
    created_at                 TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,

    CONSTRAINT skills_pkey PRIMARY KEY (org_id, slug)
);

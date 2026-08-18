-- Recreate empty shells of the migration-managed skills tables.
--
-- Schema-rollback consistency only: the serving code was removed in the
-- same change that dropped the tables, so a rolled-back schema simply has
-- empty tables nothing writes. The DDL mirrors each table's final
-- pre-drop shape (after 1781340000's download counter and 1781350000's
-- UUID id rebuild).
--
-- The embedding tables (span_embeddings, span_embeddings_failures,
-- tapes_embeddings) are deliberately not recreated: they were never
-- migration-managed — the retired spanembed store created them at runtime
-- — and their vector columns require the pgvector extension, which this
-- schema no longer depends on.

CREATE TABLE skills (
    org_id uuid NOT NULL,
    slug text NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    type text DEFAULT 'workflow'::text NOT NULL,
    version text DEFAULT '0.1.0'::text NOT NULL,
    visibility text DEFAULT 'private'::text NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    content text DEFAULT ''::text NOT NULL,
    is_ai_generated boolean DEFAULT false NOT NULL,
    generated_from_session_ids text[] DEFAULT '{}'::text[] NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    author_subject text DEFAULT ''::text NOT NULL,
    download_count bigint DEFAULT 0 NOT NULL,
    id uuid NOT NULL,
    parent_id uuid,
    CONSTRAINT skills_pkey PRIMARY KEY (org_id, id)
);

CREATE INDEX skills_org_updated_idx ON skills USING btree (org_id, updated_at DESC, id DESC);

CREATE TABLE skill_versions (
    org_id uuid NOT NULL,
    version_number integer NOT NULL,
    semver text NOT NULL,
    changelog text DEFAULT ''::text NOT NULL,
    content text DEFAULT ''::text NOT NULL,
    author_subject text DEFAULT ''::text NOT NULL,
    published_at timestamp with time zone NOT NULL,
    skill_id uuid NOT NULL,
    CONSTRAINT skill_versions_pkey PRIMARY KEY (org_id, skill_id, version_number)
);

CREATE INDEX skill_versions_skill_idx ON skill_versions USING btree (org_id, skill_id, version_number DESC);

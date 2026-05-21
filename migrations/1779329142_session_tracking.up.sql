-- The sessions table is the resolution target for the session-tracking
-- envelope ingest accepts on each turn. Ingest UPSERTs a row keyed by
-- (org_id, harness_id, harness_session_id), then attaches
-- nodes.session_id and rolls up token/cost counters in the same
-- transaction as the node insert.
--
-- org_id is UUID NOT NULL but unconstrained: there is no `orgs` table
-- in this repo. Deployments that have one can layer the FK separately.
--
-- `id` is plain `UUID PRIMARY KEY` with no default. Postgres 17 has
-- no native UUIDv7, so ingest mints the value app-side before insert.
--
-- nodes.org_id + composite PK: the content-addressed nodes.hash is the
-- same SHA across every org that produces the same content, so a
-- single-column PK on hash would make the first org's writer "own" the
-- row and silently strand subsequent orgs' INSERT ... ON CONFLICT DO
-- NOTHING attempts on someone else's session_id. Composite PK
-- (org_id, hash) gives each org its own row even when content
-- collides. The self-referential parent_hash FK from baseline becomes
-- invalid (hash alone is no longer unique), so it is dropped:
-- parent_hash stays as plain metadata, the DAG-walk queries already
-- key on parent_hash directly.
--
-- nodes.session_id is nullable: new rows from the session-aware
-- ingest path populate it, legacy rows stay NULL until an offline
-- backfill runs.

CREATE TABLE IF NOT EXISTS sessions (
    id                       UUID PRIMARY KEY,
    org_id                   UUID NOT NULL,
    auth_subject             TEXT NOT NULL,
    harness_id               TEXT NOT NULL,
    harness_session_id       TEXT NOT NULL,
    name                     TEXT,
    cwd                      TEXT,
    harness_version          TEXT,
    parent_session_id        UUID REFERENCES sessions(id),
    started_at               TIMESTAMPTZ NOT NULL,
    last_seen_at             TIMESTAMPTZ NOT NULL,
    ended_at                 TIMESTAMPTZ,
    harness_metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
    total_input_tokens       BIGINT NOT NULL DEFAULT 0,
    total_output_tokens      BIGINT NOT NULL DEFAULT 0,
    total_cost_usd           NUMERIC(12,4) NOT NULL DEFAULT 0,
    turn_count               INT NOT NULL DEFAULT 0,

    CONSTRAINT sessions_harness_uq
        UNIQUE (org_id, harness_id, harness_session_id)
);

CREATE INDEX IF NOT EXISTS sessions_org_lastseen_idx ON sessions (org_id, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS sessions_auth_subject_idx ON sessions (auth_subject);
CREATE INDEX IF NOT EXISTS sessions_parent_idx       ON sessions (parent_session_id);

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id);

CREATE INDEX IF NOT EXISTS nodes_session_idx ON nodes (session_id);

-- Composite PK (org_id, hash) on nodes — see header comment for the
-- cross-org leak motivation. Existing rows take the nil-UUID
-- (00000000-0000-0000-0000-000000000000) sentinel so the column can be
-- NOT NULL with no downtime; legacy non-session writers (Driver.Put with
-- no envelope) keep landing on that bucket. Cloud deployments that
-- enforce per-org separation can filter on org_id != nil-UUID.
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS org_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000'::uuid;

ALTER TABLE nodes
    DROP CONSTRAINT IF EXISTS nodes_parent_hash_fkey;

ALTER TABLE nodes
    DROP CONSTRAINT IF EXISTS nodes_pkey;

ALTER TABLE nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (org_id, hash);

CREATE INDEX IF NOT EXISTS nodes_hash_idx ON nodes (hash);

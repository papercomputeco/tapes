-- Immutable raw-capture layer (Phase 1 of the reconciled conversation
-- tree; see design/agent-session-reconciliation.md).
--
-- raw_turns persists the FULL ingest envelope for every captured turn,
-- append-only, before any parsing or projection. The derived layer
-- (nodes + edges + typing) is a pure, re-runnable function of these
-- rows: every future data-model change (new offshoot kind, different
-- projection, new edge type) is a re-derive over existing raw data,
-- never a re-capture or a destructive migration.
--
-- source discriminates capture origins so the harness-transcript
-- ingest path (Phase 3) can feed the same substrate:
--   'wire'       — Envoy→extproc capture (request as sent on the wire,
--                  response reduced from the SSE stream by extproc)
--   'transcript' — harness on-disk transcript upload
--
-- raw_request / response / meta / session_envelope are stored VERBATIM
-- from the envelope JSON — not re-marshaled through parsed structs — so
-- fields unknown to this build survive for later derivers.
--
-- The partial unique index on (org_id, request_id) makes retried POSTs
-- of the same captured turn idempotent (extproc retries dispatch on
-- 5xx). Writers without a request_id (request_id = '') are exempt:
-- they get plain append semantics.

CREATE TABLE IF NOT EXISTS raw_turns (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id             UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000000'::uuid,
    source             TEXT NOT NULL DEFAULT 'wire',
    provider           TEXT NOT NULL DEFAULT '',
    agent_name         TEXT NOT NULL DEFAULT '',
    harness_id         TEXT NOT NULL DEFAULT '',
    harness_session_id TEXT NOT NULL DEFAULT '',
    request_id         TEXT NOT NULL DEFAULT '',
    raw_request        JSONB,
    response           JSONB,
    meta               JSONB NOT NULL DEFAULT '{}'::jsonb,
    session_envelope   JSONB,
    received_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS raw_turns_org_request_uq
    ON raw_turns (org_id, request_id)
    WHERE request_id <> '';

CREATE INDEX IF NOT EXISTS raw_turns_org_session_idx
    ON raw_turns (org_id, harness_session_id);

CREATE INDEX IF NOT EXISTS raw_turns_received_at_idx
    ON raw_turns (received_at);

-- Promote the request-envelope parameters onto nodes as cheap queryable
-- copies (raw_turns stays the source of truth). These are the
-- definitive discriminators for classifying harness shadow calls
-- (security monitor: max_tokens≈64 + tool_count=0 + stream=false;
-- title-gen: small max_tokens + tool_count=0; main conversation: full
-- tool set + stream=true + high max_tokens). They are stamped on the
-- nodes newly inserted by each captured call and are NOT part of the
-- content-addressed hash — existing node hashes are unchanged.
--
-- request_system is stored but stays hash-excluded: the system prompt
-- identifies the caller (e.g. "You are a security monitor…"), it is
-- not conversation content.

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS request_system      TEXT,
    ADD COLUMN IF NOT EXISTS request_max_tokens  INTEGER,
    ADD COLUMN IF NOT EXISTS request_temperature DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS request_stream      BOOLEAN,
    ADD COLUMN IF NOT EXISTS request_tool_count  INTEGER;

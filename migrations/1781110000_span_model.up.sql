-- Experimental Lapdog-style span read model.
--
-- This schema intentionally keeps conversation topology out of Merkle hashes:
-- sessions group harness runs, span_turns model one user-visible trace/turn,
-- spans model observed agent/step/llm/tool work, and span_links model causal
-- edges that do not fit strict parent/child containment.

CREATE TABLE IF NOT EXISTS span_turns (
    id                  UUID PRIMARY KEY,
    org_id              UUID NOT NULL,
    session_id          UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    trace_id            TEXT NOT NULL,
    harness_turn_id     TEXT,
    user_prompt         TEXT,
    status              TEXT NOT NULL DEFAULT 'ok',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at            TIMESTAMPTZ,
    duration_ns         BIGINT NOT NULL DEFAULT 0,
    total_input_tokens  BIGINT NOT NULL DEFAULT 0,
    total_output_tokens BIGINT NOT NULL DEFAULT 0,
    total_cost_usd      NUMERIC(12,4) NOT NULL DEFAULT 0,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT span_turns_org_trace_uq UNIQUE (org_id, trace_id)
);

CREATE INDEX IF NOT EXISTS span_turns_org_started_idx ON span_turns (org_id, started_at DESC, trace_id DESC);
CREATE INDEX IF NOT EXISTS span_turns_session_idx ON span_turns (session_id, started_at ASC);

CREATE TABLE IF NOT EXISTS spans (
    id              UUID PRIMARY KEY,
    org_id          UUID NOT NULL,
    session_id      UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    turn_id         UUID NOT NULL REFERENCES span_turns(id) ON DELETE CASCADE,
    trace_id        TEXT NOT NULL,
    span_id         TEXT NOT NULL,
    parent_span_id  TEXT,
    kind            TEXT NOT NULL,
    name            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ok',
    start_ns        BIGINT NOT NULL,
    duration_ns     BIGINT NOT NULL DEFAULT 0,
    input           JSONB NOT NULL DEFAULT '{}'::jsonb,
    output          JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics         JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT spans_org_trace_span_uq UNIQUE (org_id, trace_id, span_id),
    CONSTRAINT spans_kind_chk CHECK (kind IN ('agent', 'step', 'llm', 'tool', 'event'))
);

CREATE INDEX IF NOT EXISTS spans_org_trace_idx ON spans (org_id, trace_id, start_ns ASC);
CREATE INDEX IF NOT EXISTS spans_turn_idx ON spans (turn_id, start_ns ASC);
CREATE INDEX IF NOT EXISTS spans_kind_idx ON spans (kind);
CREATE INDEX IF NOT EXISTS spans_input_gin_idx ON spans USING GIN (input);
CREATE INDEX IF NOT EXISTS spans_output_gin_idx ON spans USING GIN (output);
CREATE INDEX IF NOT EXISTS spans_metadata_gin_idx ON spans USING GIN (metadata);

CREATE TABLE IF NOT EXISTS span_links (
    org_id        UUID NOT NULL,
    trace_id      TEXT NOT NULL,
    from_span_id  TEXT NOT NULL,
    to_span_id    TEXT NOT NULL,
    from_io       TEXT NOT NULL DEFAULT '',
    to_io         TEXT NOT NULL DEFAULT '',
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT span_links_pk PRIMARY KEY (org_id, trace_id, from_span_id, to_span_id, from_io, to_io)
);

CREATE INDEX IF NOT EXISTS span_links_trace_idx ON span_links (org_id, trace_id);

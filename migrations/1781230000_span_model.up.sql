-- Span model (RFD 00007): traces, spans, span links as a derived
-- projection of the immutable raw layer. The deriver emits these rows
-- (pkg/derive EmitSpans) with deterministic ids minted from wire
-- identity, so re-derivation is idempotent and prune-stable.
--
-- Deliberate divergences from the tapes-traces POC schema:
--   - no surrogate UUID row ids: span identity is deterministic
--     (org_id, trace_id, span_id), so re-derive upserts in place
--     instead of duplicating under fresh uuids.
--   - span_links carry (from_trace_id, to_trace_id): compaction seams
--     and agent causality cross trace boundaries, which a single
--     trace_id link key cannot represent.
--   - no verbatim payload copies: spans.input/output hold delta-only
--     content (payload dedup is a v1 hard requirement — every main
--     call re-sends the whole history, so verbatim storage is
--     O(conversation²)); raw_turn_id references the capturing raw row
--     for full-fidelity provenance.

CREATE TABLE IF NOT EXISTS span_turns (
    org_id              UUID NOT NULL,
    trace_id            TEXT NOT NULL,
    session_id          UUID REFERENCES sessions(id) ON DELETE CASCADE,
    user_prompt         TEXT NOT NULL DEFAULT '',
    synthetic           TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'ok',
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ,
    duration_ns         BIGINT NOT NULL DEFAULT 0,
    total_input_tokens  BIGINT NOT NULL DEFAULT 0,
    total_output_tokens BIGINT NOT NULL DEFAULT 0,
    total_cost_usd      NUMERIC(12,4) NOT NULL DEFAULT 0,

    PRIMARY KEY (org_id, trace_id)
);

CREATE INDEX IF NOT EXISTS span_turns_session_idx ON span_turns (session_id, started_at ASC);
CREATE INDEX IF NOT EXISTS span_turns_org_started_idx ON span_turns (org_id, started_at DESC, trace_id DESC);

CREATE TABLE IF NOT EXISTS spans (
    org_id         UUID NOT NULL,
    trace_id       TEXT NOT NULL,
    span_id        TEXT NOT NULL,
    parent_span_id TEXT NOT NULL DEFAULT '',
    session_id     UUID REFERENCES sessions(id) ON DELETE CASCADE,
    kind           TEXT NOT NULL,
    name           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'ok',
    call_kind      TEXT NOT NULL DEFAULT '',
    thread_id      TEXT NOT NULL DEFAULT '',
    model          TEXT NOT NULL DEFAULT '',
    stop_reason    TEXT NOT NULL DEFAULT '',
    started_at     TIMESTAMPTZ NOT NULL,
    duration_ns    BIGINT NOT NULL DEFAULT 0,
    input          JSONB,
    output         JSONB,
    usage          JSONB,
    -- provenance reference, deliberately unconstrained: the raw layer
    -- is append-only and the one sanctioned cleanup (nil-org transcript
    -- rows) must not cascade or block on the projection.
    raw_turn_id    BIGINT,
    node_hash      TEXT NOT NULL DEFAULT '',

    PRIMARY KEY (org_id, trace_id, span_id),
    FOREIGN KEY (org_id, trace_id) REFERENCES span_turns(org_id, trace_id) ON DELETE CASCADE,
    CONSTRAINT spans_kind_chk CHECK (kind IN ('agent', 'step', 'llm', 'tool', 'event'))
);

CREATE INDEX IF NOT EXISTS spans_session_idx ON spans (session_id, started_at ASC);
CREATE INDEX IF NOT EXISTS spans_org_call_kind_idx ON spans (org_id, call_kind);
CREATE INDEX IF NOT EXISTS spans_raw_turn_idx ON spans (raw_turn_id);

CREATE TABLE IF NOT EXISTS span_links (
    org_id        UUID NOT NULL,
    from_trace_id TEXT NOT NULL,
    from_span_id  TEXT NOT NULL,
    from_io       TEXT NOT NULL DEFAULT '',
    to_trace_id   TEXT NOT NULL,
    to_span_id    TEXT NOT NULL,
    to_io         TEXT NOT NULL DEFAULT '',
    kind          TEXT NOT NULL DEFAULT '',
    session_id    UUID REFERENCES sessions(id) ON DELETE CASCADE,

    PRIMARY KEY (org_id, from_trace_id, from_span_id, to_trace_id, to_span_id, from_io, to_io)
);

CREATE INDEX IF NOT EXISTS span_links_from_trace_idx ON span_links (org_id, from_trace_id);
CREATE INDEX IF NOT EXISTS span_links_to_trace_idx ON span_links (org_id, to_trace_id);
CREATE INDEX IF NOT EXISTS span_links_session_idx ON span_links (session_id);

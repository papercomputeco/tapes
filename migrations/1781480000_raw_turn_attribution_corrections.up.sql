-- Append-only attribution repairs for immutable raw turns. The latest row for
-- a raw turn is its effective attribution; raw_turns itself is never updated.
CREATE TABLE raw_turn_attribution_corrections (
    id                        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id                    UUID NOT NULL,
    raw_turn_id               BIGINT NOT NULL,
    harness_id                TEXT NOT NULL,
    harness_session_id        TEXT NOT NULL,
    thread_id                 TEXT NOT NULL DEFAULT '',
    parent_harness_session_id TEXT,
    reason                    TEXT NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX raw_turn_attribution_corrections_latest_idx
    ON raw_turn_attribution_corrections (org_id, raw_turn_id, id DESC);

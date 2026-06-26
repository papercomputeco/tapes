-- Generated columns make the two computed sort keys (tokens, duration) real,
-- indexable values. STORED so they are indexable; existing rows backfill
-- during the rewrite. duration_ns mirrors the console's computeDurationNs unit
-- (nanoseconds) and grows with last_seen_at for still-live sessions.
ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS total_tokens BIGINT
        GENERATED ALWAYS AS (total_input_tokens + total_output_tokens) STORED;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS duration_ns BIGINT
        GENERATED ALWAYS AS ((EXTRACT(EPOCH FROM (last_seen_at - started_at)) * 1e9)::bigint) STORED;

-- One composite index per sortable column, shaped (org_id, col DESC, id DESC)
-- so a single btree serves both directions (Postgres reverse-scans for ASC;
-- the id tiebreak direction always tracks the sort direction).
-- Note: CONCURRENTLY omitted because golang-migrate wraps each file in a
-- transaction; CREATE INDEX CONCURRENTLY cannot run inside a transaction.
CREATE INDEX IF NOT EXISTS sessions_org_lastseen_id_idx   ON sessions (org_id, last_seen_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_started_id_idx    ON sessions (org_id, started_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_turns_id_idx      ON sessions (org_id, turn_count DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_cost_id_idx       ON sessions (org_id, total_cost_usd DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_tokens_id_idx     ON sessions (org_id, total_tokens DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_duration_id_idx   ON sessions (org_id, duration_ns DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_status_id_idx     ON sessions (org_id, derived_status DESC, id DESC);
CREATE INDEX IF NOT EXISTS sessions_org_subject_id_idx    ON sessions (org_id, auth_subject DESC, id DESC);

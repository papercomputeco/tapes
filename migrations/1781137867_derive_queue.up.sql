-- Dirty-session queue for the derive worker (see design/
-- agent-session-reconciliation.md). Ingest marks a session dirty every
-- time it persists a raw turn (wire or transcript) for it; the worker
-- polls this table, debounces on dirtied_at, re-derives the session
-- from the raw layer, and deletes the row.
--
-- This is its own table, NOT columns on sessions, because:
--   * the queue is keyed by the deriver's natural unit — the harness
--     key (org_id, harness_id, harness_session_id) — and a sessions
--     row is NOT guaranteed to exist when a raw turn lands (transcript
--     ingest writes only a raw row; the deriver resolves sessions rows
--     lazily and skips unknown keys),
--   * dirty state is queue state, not session state: a DELETE-when-done
--     row makes the conditional clear ("only if dirtied_at is unchanged
--     since I read it") a single guarded DELETE, with no tombstone
--     booleans to sweep on the hot sessions row.
--
-- At-least-once by design: marking is idempotent (upsert bumps
-- dirtied_at), deriving is idempotent (re-run prunes 0), so a lost
-- clear or a duplicate mark only costs a redundant derive.

CREATE TABLE IF NOT EXISTS derive_queue (
    org_id             UUID NOT NULL,
    harness_id         TEXT NOT NULL,
    harness_session_id TEXT NOT NULL,
    dirtied_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (org_id, harness_id, harness_session_id)
);

-- The worker's poll is "oldest settled first": dirtied_at <= cutoff,
-- ordered ascending.
CREATE INDEX IF NOT EXISTS derive_queue_dirtied_idx ON derive_queue (dirtied_at);

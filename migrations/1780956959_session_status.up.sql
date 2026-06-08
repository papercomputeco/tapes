-- Adds per-session derived status to the sessions table so /v1/stats and
-- /v1/sessions can report session-grained, chain-aware completion without
-- re-walking the merkle DAG on every read (PCC-515, PCC-565).
--
-- derived_status mirrors pkg/sessions.DetermineStatus. It is recomputed at
-- ingest from two sticky signals plus the session's latest assistant leaf:
--   has_tool_error   — any tool_result with is_error in the session
--   has_git_activity — any `git commit` / `git push` tool_use in the session
-- Both default false and only ever flip to true, so status can be recomputed
-- incrementally per turn inside the ingest Tx without a chain walk. Existing
-- rows take the 'unknown' default until the one-shot backfill runs.
--
-- The index is (org_id, derived_status): /v1/stats filters completed_count
-- per org by status, and the session list queries are already org-scoped.

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS derived_status text NOT NULL DEFAULT 'unknown';

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS has_tool_error boolean NOT NULL DEFAULT false;

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS has_git_activity boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS sessions_derived_status_idx
    ON sessions (org_id, derived_status);

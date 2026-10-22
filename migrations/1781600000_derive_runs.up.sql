-- Asynchronous whole-store re-derive runs.
--
-- A run is an operator job that re-derives every persisted session from the
-- raw layer. It outlives the request that started it and the process that
-- executes it: the session set is snapshotted into derive_run_sessions when
-- the run is created, and each session is marked finished as it completes, so
-- a run interrupted by a restart resumes at the first unfinished session
-- instead of starting over.
--
-- Whichever API process holds the run's advisory lock executes it; the lock is
-- session-scoped, so it is released when that process or its connection goes
-- away and another process picks the run up.
CREATE TABLE IF NOT EXISTS derive_runs (
    id              UUID PRIMARY KEY,
    project         TEXT NOT NULL DEFAULT '',
    -- pending: created, not yet picked up; running: an executor has started
    -- it (and may have stopped — a restart leaves it running until resumed);
    -- completed: every snapshotted session finished, some possibly with an
    -- error; canceled: an operator stopped it.
    state           TEXT NOT NULL CHECK (state IN ('pending', 'running', 'completed', 'canceled')),
    sessions_total  INTEGER NOT NULL DEFAULT 0,
    sessions_done   INTEGER NOT NULL DEFAULT 0,
    sessions_failed INTEGER NOT NULL DEFAULT 0,
    -- Aggregated derive report across finished sessions (the same shape the
    -- synchronous derive run returns per org).
    report          JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Bounded sample of per-session failures, "harness/session: message".
    errors          JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Host name of the process that last advanced the run.
    worker          TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- At most one active run per store. A deployment serves one organization, so
-- this is one active run per organization.
CREATE UNIQUE INDEX IF NOT EXISTS derive_runs_one_active_idx
    ON derive_runs ((true)) WHERE state IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS derive_runs_created_idx
    ON derive_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS derive_run_sessions (
    run_id             UUID NOT NULL REFERENCES derive_runs(id) ON DELETE CASCADE,
    org_id             UUID NOT NULL,
    harness_id         TEXT NOT NULL,
    harness_session_id TEXT NOT NULL,
    finished_at        TIMESTAMPTZ,
    error              TEXT,
    PRIMARY KEY (run_id, org_id, harness_id, harness_session_id)
);

-- name: InsertDeriveRun :one
-- Create a pending run. The partial unique index on active runs makes a
-- concurrent second create fail with a unique violation, which the caller
-- resolves to the run already in flight.
INSERT INTO derive_runs (id, project, state)
VALUES ($1, $2, 'pending')
RETURNING *;

-- name: SnapshotDeriveRunSessions :execrows
-- Freeze the session set a run covers. Sessions are enumerated from the read
-- model, as the synchronous re-derive does, so a session whose raw turns were
-- repaired away is still covered and pruned. Sessions created after the
-- snapshot are derived by the derive worker as they arrive.
INSERT INTO derive_run_sessions (run_id, org_id, harness_id, harness_session_id)
SELECT $1, org_id, harness_id, harness_session_id
FROM sessions;

-- name: SetDeriveRunTotal :exec
UPDATE derive_runs SET sessions_total = $2, updated_at = now() WHERE id = $1;

-- name: GetActiveDeriveRun :one
SELECT * FROM derive_runs
WHERE state IN ('pending', 'running')
LIMIT 1;

-- name: GetDeriveRun :one
SELECT * FROM derive_runs WHERE id = $1;

-- name: ListDeriveRuns :many
SELECT * FROM derive_runs ORDER BY created_at DESC LIMIT $1;

-- name: ListActiveDeriveRunIDs :many
SELECT id FROM derive_runs
WHERE state IN ('pending', 'running')
ORDER BY created_at;

-- name: MarkDeriveRunRunning :one
-- Claim a run for execution. started_at keeps the first start; worker and
-- updated_at record the process resuming it.
UPDATE derive_runs
SET state = 'running',
    started_at = COALESCE(started_at, now()),
    worker = $2,
    updated_at = now()
WHERE id = $1 AND state IN ('pending', 'running')
RETURNING *;

-- name: ListPendingDeriveRunSessions :many
-- Keyset page of a run's unfinished sessions, in key order. The first page
-- starts at the beginning; later pages start after the last key read. A
-- resumed run starts again from the first page, which skips every finished
-- session.
SELECT org_id, harness_id, harness_session_id
FROM derive_run_sessions
WHERE run_id = sqlc.arg(run_id)
  AND finished_at IS NULL
  AND (sqlc.arg(first_page)::bool
       OR (org_id, harness_id, harness_session_id) > (sqlc.arg(after_org_id)::uuid, sqlc.arg(after_harness_id)::text, sqlc.arg(after_harness_session_id)::text))
ORDER BY org_id, harness_id, harness_session_id
LIMIT sqlc.arg(page_limit);

-- name: FinishDeriveRunSession :one
-- Mark one session finished and advance the run's counters in one statement.
-- The counters move only when this call is the one that finished the session,
-- so a session finished twice (a run resumed by two processes at once) is
-- counted once.
WITH finished AS (
    UPDATE derive_run_sessions s
    SET finished_at = now(), error = sqlc.narg(error)
    WHERE s.run_id = sqlc.arg(run_id)
      AND s.org_id = sqlc.arg(org_id)
      AND s.harness_id = sqlc.arg(harness_id)
      AND s.harness_session_id = sqlc.arg(harness_session_id)
      AND s.finished_at IS NULL
    RETURNING s.run_id
)
UPDATE derive_runs r
SET sessions_done = r.sessions_done + (SELECT count(*) FROM finished),
    sessions_failed = r.sessions_failed + CASE WHEN sqlc.narg(error)::text IS NULL THEN 0 ELSE (SELECT count(*) FROM finished) END,
    report = sqlc.arg(report),
    errors = sqlc.arg(errors),
    updated_at = now()
WHERE r.id = sqlc.arg(run_id)
RETURNING r.state;

-- name: CompleteDeriveRun :one
-- Close a run once no unfinished session remains. A canceled run stays
-- canceled.
UPDATE derive_runs
SET state = 'completed', finished_at = now(), updated_at = now()
WHERE id = $1
  AND state = 'running'
  AND NOT EXISTS (
      SELECT 1 FROM derive_run_sessions
      WHERE run_id = $1 AND finished_at IS NULL
  )
RETURNING *;

-- name: CancelDeriveRun :one
UPDATE derive_runs
SET state = 'canceled', finished_at = now(), updated_at = now()
WHERE id = $1 AND state IN ('pending', 'running')
RETURNING *;

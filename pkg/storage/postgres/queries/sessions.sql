-- name: UpsertSession :one
-- Insert-or-merge keyed by the natural identity
-- (org_id, harness_id, harness_session_id). The caller supplies the
-- UUID for `id` (Postgres 17 has no native UUIDv7, so ingest mints one
-- app-side); on conflict the existing row's id is preserved via
-- RETURNING.
--
-- On conflict, mutable fields are merged: last_seen_at is bumped to
-- the caller-supplied `now`, harness_metadata is JSON-merged
-- (last-write-wins per key), and name is updated only when the new
-- value is non-null. auth_subject is overwritten with the
-- caller-supplied value: when a child session's first turn arrived
-- before the parent's, InsertSessionPlaceholder wrote the parent row
-- carrying the *child's* auth_subject; the parent's real upsert here
-- is authoritative and must reclaim attribution. Counters are NOT
-- touched here — the derive-time span fold (FoldSessionRollupsFromSpans)
-- owns the token/turn/cost rollups.
INSERT INTO sessions (
    id,
    org_id,
    auth_subject,
    harness_id,
    harness_session_id,
    name,
    cwd,
    harness_version,
    parent_session_id,
    started_at,
    last_seen_at,
    harness_metadata
) VALUES (
    sqlc.arg(id),
    sqlc.arg(org_id),
    sqlc.arg(auth_subject),
    sqlc.arg(harness_id),
    sqlc.arg(harness_session_id),
    sqlc.narg(name),
    sqlc.narg(cwd),
    sqlc.narg(harness_version),
    sqlc.narg(parent_session_id),
    sqlc.arg(now),
    sqlc.arg(now),
    sqlc.arg(harness_metadata)
)
ON CONFLICT (org_id, harness_id, harness_session_id) DO UPDATE
SET last_seen_at     = sqlc.arg(now),
    auth_subject     = EXCLUDED.auth_subject,
    harness_metadata = sessions.harness_metadata || sqlc.arg(harness_metadata),
    name             = COALESCE(sqlc.narg(name), sessions.name),
    cwd              = COALESCE(sqlc.narg(cwd), sessions.cwd),
    harness_version  = COALESCE(sqlc.narg(harness_version), sessions.harness_version),
    parent_session_id = COALESCE(sqlc.narg(parent_session_id), sessions.parent_session_id)
RETURNING *;

-- name: GetSessionByNaturalKey :one
-- Lookup by the unique (org_id, harness_id, harness_session_id) index.
-- Used by the parent-FK resolution path in ingest: when an inbound
-- request carries a parent_harness_session_id hint, ingest reads the
-- parent's sessions.id this way and FKs it onto the new row.
SELECT * FROM sessions
WHERE org_id = $1
  AND harness_id = $2
  AND harness_session_id = $3;

-- name: InsertSessionPlaceholder :one
-- Insert the minimal-fields placeholder row used when the envelope
-- names a fork-parent whose own first turn hasn't landed yet. The
-- caller supplies the UUID; on natural-key conflict, the existing
-- row's id is returned so the FK back-fills naturally when the
-- parent's first real request lands.
--
-- Note: this uses `ON CONFLICT ... DO UPDATE SET last_seen_at =
-- last_seen_at` (a no-op write) instead of DO NOTHING so the
-- RETURNING clause still emits the existing row's id — sqlc/pgx
-- treats DO NOTHING RETURNING as "no rows" on conflict.
INSERT INTO sessions (
    id,
    org_id,
    auth_subject,
    harness_id,
    harness_session_id,
    started_at,
    last_seen_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(org_id),
    sqlc.arg(auth_subject),
    sqlc.arg(harness_id),
    sqlc.arg(harness_session_id),
    sqlc.arg(now),
    sqlc.arg(now)
)
ON CONFLICT (org_id, harness_id, harness_session_id) DO UPDATE
SET last_seen_at = sessions.last_seen_at
RETURNING id;

-- name: UpdateSessionStatus :exec
-- Persist the recomputed chain-aware status. has_git_activity is a sticky
-- flag and tool_result_count / tool_error_count are cumulative totals, all
-- accumulated across the session's turns and stems — the caller computes the
-- new values in Go from the prior row state plus this turn's new nodes, so
-- this query just writes them. derived_status mirrors
-- pkg/sessions.DetermineStatus over those signals and the session's latest
-- leaf. Called by ingest within the session-ingest Tx.
UPDATE sessions
   SET has_git_activity  = sqlc.arg(has_git_activity),
       tool_result_count = sqlc.arg(tool_result_count),
       tool_error_count  = sqlc.arg(tool_error_count),
       derived_status    = sqlc.arg(derived_status)
 WHERE id = sqlc.arg(id);

-- name: GetSessionRecord :one
SELECT * FROM sessions
WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: DeleteSession :execrows
-- Remove a session by its org-scoped id. Returns the affected row count so the
-- handler can distinguish a real delete from a missing id. Dependent rows
-- (subagent child sessions, spans/span_turns/span_links) are removed by the
-- session_id ON DELETE CASCADE foreign keys, so this single statement tears
-- down the whole subtree.
DELETE FROM sessions
WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: UpdateSessionDerivedTitle :exec
-- Fold the title-gen shadow call's output onto the session. Written at
-- capture time when the title call lands, and again on re-derive —
-- idempotent either way.
UPDATE sessions SET derived_title = sqlc.arg(derived_title) WHERE id = sqlc.arg(id);

-- name: UpdateSessionModelUsage :exec
-- Fold the per-model spend breakdown onto the session (#28). Unlike the
-- token/cost rollups (a pure SQL fold over span_turns), this is priced
-- per model in Go at derive time — the price table lives there, not in
-- SQL — so the deriver writes it directly as a JSONB array. Re-derive
-- overwrites it idempotently.
UPDATE sessions SET model_usage = sqlc.arg(model_usage) WHERE id = sqlc.arg(id);

-- name: UpdateSessionTasks :exec
-- Fold the TaskCreate/TaskUpdate replay onto the session. Like model_usage
-- this is a Go-side fold (it depends on regex id extraction SQL can't do),
-- written as a JSONB array. Re-derive overwrites it idempotently.
UPDATE sessions SET tasks = sqlc.arg(tasks) WHERE id = sqlc.arg(id);

-- name: UpdateSessionKindCounts :exec
-- Write the per-call_kind span tally onto the session as a JSONB object.
-- Re-derive overwrites it idempotently.
UPDATE sessions SET kind_counts = sqlc.arg(kind_counts) WHERE id = sqlc.arg(id);

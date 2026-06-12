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
-- touched here — UpdateSessionCounters handles that after the nodes
-- insert in the same Tx.
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

-- name: UpdateSessionCounters :exec
-- Roll the per-turn counters into the sessions row. Called by ingest
-- after the nodes insert, inside the same Tx.
UPDATE sessions
   SET last_seen_at        = sqlc.arg(now),
       turn_count          = turn_count + sqlc.arg(turn_count_delta),
       total_input_tokens  = total_input_tokens  + sqlc.arg(input_tokens_delta),
       total_output_tokens = total_output_tokens + sqlc.arg(output_tokens_delta),
       total_cost_usd      = total_cost_usd      + sqlc.arg(cost_usd_delta)
 WHERE id = sqlc.arg(id);

-- name: UpdateSessionStatus :exec
-- Persist the recomputed chain-aware status. has_git_activity is a sticky
-- flag and tool_result_count / tool_error_count are cumulative totals, all
-- accumulated across the session's turns and stems — the caller computes the
-- new values in Go from the prior row state plus this turn's new nodes, so
-- this query just writes them. derived_status mirrors
-- pkg/sessions.DetermineStatus over those signals and the session's latest
-- leaf. Called by ingest in the same Tx as UpdateSessionCounters.
UPDATE sessions
   SET has_git_activity  = sqlc.arg(has_git_activity),
       tool_result_count = sqlc.arg(tool_result_count),
       tool_error_count  = sqlc.arg(tool_error_count),
       derived_status    = sqlc.arg(derived_status)
 WHERE id = sqlc.arg(id);

-- name: ListSessionRecords :many
-- Paginated list of sessions for an org ordered newest-first (last_seen_at DESC, id DESC).
-- Pass NULL cursor values to start from the beginning. Pass a NULL
-- auth_subject to list every user's sessions; a non-NULL value is an
-- exact match against the gateway-stamped JWT subject captured at
-- ingest (sessions_auth_subject_idx).
SELECT * FROM sessions
WHERE org_id = sqlc.arg(org_id)
  AND (
    sqlc.narg(auth_subject)::text IS NULL
    OR auth_subject = sqlc.narg(auth_subject)::text
  )
  AND (
    sqlc.narg(cursor_ts)::timestamptz IS NULL
    OR last_seen_at < sqlc.narg(cursor_ts)::timestamptz
    OR (last_seen_at = sqlc.narg(cursor_ts)::timestamptz AND id < sqlc.narg(cursor_id)::uuid)
  )
ORDER BY last_seen_at DESC, id DESC
LIMIT sqlc.arg(lim);

-- name: GetSessionRecord :one
SELECT * FROM sessions
WHERE org_id = sqlc.arg(org_id) AND id = sqlc.arg(id);

-- name: ListNodesBySession :many
-- All nodes attributed to a session, ordered by capture time (chronological).
SELECT * FROM nodes
WHERE session_id = sqlc.arg(session_id)
ORDER BY created_at ASC;

-- name: SetNodeSessionID :exec
-- Stamp session_id onto an already-inserted nodes row. The existing
-- InsertNode query is left intact (additive ALTER added the column
-- with no NOT NULL constraint), so ingest can either use this safety
-- hatch or extend InsertNode in a follow-up. Scoped to (org_id, hash)
-- to match the composite PK introduced in this migration: a write that
-- only matched on hash would clobber rows for unrelated orgs.
UPDATE nodes
   SET session_id = $1
 WHERE org_id = $2 AND hash = $3;

-- name: MarkDeriveDirty :exec
-- Mark one harness session dirty for the derive worker. Upsert: a
-- session already queued just gets its dirtied_at bumped, which is
-- exactly the debounce signal (the worker waits for dirtied_at to
-- settle before deriving).
INSERT INTO derive_queue (org_id, harness_id, harness_session_id)
VALUES ($1, $2, $3)
ON CONFLICT (org_id, harness_id, harness_session_id)
DO UPDATE SET dirtied_at = CURRENT_TIMESTAMP;

-- name: ListDeriveDirty :many
-- The worker's poll: sessions whose dirty mark has settled (no new
-- raw turn since the debounce window) OR whose first mark has waited
-- past the max-lag bound — a streaming session re-marks continuously
-- and would otherwise never settle. Oldest first.
SELECT org_id, harness_id, harness_session_id, dirtied_at, first_dirtied_at
FROM derive_queue
WHERE dirtied_at <= sqlc.arg(dirtied_before)
   OR first_dirtied_at <= sqlc.arg(first_dirtied_before)
ORDER BY dirtied_at
LIMIT sqlc.arg(page_size);

-- name: GetDeriveDirty :one
-- Re-read one queue row (the worker does this under the advisory lock
-- to catch a concurrent worker having already derived + cleared it).
-- first_dirtied_at rides along so the re-read can honor the max-lag
-- bound: a continuously streaming session bumps dirtied_at past the
-- debounce cutoff on every poll, but its first mark is what crossed the
-- lag bound, and the worker must derive on that.
SELECT org_id, harness_id, harness_session_id, dirtied_at, first_dirtied_at
FROM derive_queue
WHERE org_id = $1
  AND harness_id = $2
  AND harness_session_id = $3;

-- name: ClearDeriveDirty :execrows
-- Conditional clear: only removes the row if dirtied_at is unchanged
-- since the worker read it. A raw turn landing mid-derive bumps
-- dirtied_at, the DELETE matches nothing, and the session survives for
-- the next poll — re-dirty during derive is never lost.
DELETE FROM derive_queue
WHERE org_id = $1
  AND harness_id = $2
  AND harness_session_id = $3
  AND dirtied_at = sqlc.arg(dirtied_at);

-- name: DeriveQueueStats :one
-- Queue depth plus the oldest dirty mark: the worker polls this for
-- its depth/lag gauges, and /readyz uses it as the "store reachable,
-- queue pollable" probe. oldest_dirtied_at is NULL when the queue is
-- empty.
SELECT COUNT(*) AS depth, MIN(dirtied_at)::timestamptz AS oldest_dirtied_at
FROM derive_queue;

-- name: SweepDeriveDirty :execrows
-- The worker's slow backstop: enqueue every harness session with raw
-- activity since active_since. Bounding to recently-active sessions
-- keeps a worker restart in a large org from stampeding the queue with
-- the entire raw-layer history; passing the zero time sweeps
-- everything (the unbounded escape hatch). Sessions already queued
-- keep their dirtied_at (DO NOTHING, not an upsert) so the sweep never
-- resets an in-flight debounce window. Everything still funnels
-- through the per-session locked derive path — the sweep itself never
-- writes nodes, which is what makes it safe to run concurrently with
-- session derives.
INSERT INTO derive_queue (org_id, harness_id, harness_session_id)
SELECT DISTINCT org_id, harness_id, harness_session_id
FROM raw_turns
WHERE harness_session_id <> ''
  AND received_at >= sqlc.arg(active_since)
ON CONFLICT (org_id, harness_id, harness_session_id) DO NOTHING;

-- name: ListRawTurnIndexBySession :many
-- Payload-free index of one harness session's raw turns, for the
-- session-scoped deriver's ordering pass. Full rows are then streamed
-- one at a time via GetRawTurn — same memory discipline as the
-- full-org pass.
SELECT id, org_id, source, harness_id, harness_session_id, received_at, meta
FROM raw_turns
WHERE org_id = $1
  AND harness_id = $2
  AND harness_session_id = $3
ORDER BY id;

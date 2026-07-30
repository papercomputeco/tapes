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
SELECT DISTINCT r.org_id,
       COALESCE(c.harness_id, r.harness_id),
       COALESCE(c.harness_session_id, r.harness_session_id)
FROM raw_turns r
LEFT JOIN LATERAL (
    SELECT harness_id, harness_session_id
    FROM raw_turn_attribution_corrections
    WHERE org_id = r.org_id AND raw_turn_id = r.id
    ORDER BY id DESC LIMIT 1
) c ON TRUE
WHERE COALESCE(c.harness_session_id, r.harness_session_id) <> ''
  AND r.received_at >= sqlc.arg(active_since)
ON CONFLICT (org_id, harness_id, harness_session_id) DO NOTHING;

-- name: ListRawTurnIndexBySession :many
-- Payload-free index of one harness session's raw turns, for the
-- session-scoped deriver's ordering pass. Full rows are then streamed
-- one at a time via GetRawTurn — same memory discipline as the
-- full-org pass.
SELECT r.id, r.org_id, r.source,
       COALESCE((SELECT c.harness_id
                 FROM raw_turn_attribution_corrections c
                 WHERE c.org_id = r.org_id AND c.raw_turn_id = r.id
                 ORDER BY c.id DESC LIMIT 1), r.harness_id) AS harness_id,
       COALESCE((SELECT c.harness_session_id
                 FROM raw_turn_attribution_corrections c
                 WHERE c.org_id = r.org_id AND c.raw_turn_id = r.id
                 ORDER BY c.id DESC LIMIT 1), r.harness_session_id) AS harness_session_id,
       r.received_at, r.meta
FROM raw_turns r
WHERE r.org_id = $1
  AND COALESCE((SELECT c.harness_id
                FROM raw_turn_attribution_corrections c
                WHERE c.org_id = r.org_id AND c.raw_turn_id = r.id
                ORDER BY c.id DESC LIMIT 1), r.harness_id) = $2
  AND COALESCE((SELECT c.harness_session_id
                FROM raw_turn_attribution_corrections c
                WHERE c.org_id = r.org_id AND c.raw_turn_id = r.id
                ORDER BY c.id DESC LIMIT 1), r.harness_session_id) = $3
ORDER BY r.id;

-- name: NextDeriveSeq :one
-- One cursor value per derive pass: the derive transaction's own id. Every row
-- the pass changes is stamped with it, so a consumer that has seen value N has
-- seen a whole pass, never half of one.
--
-- It is a transaction id rather than a sequence value on purpose. Any cursor
-- allocated inside a transaction is assigned at write time, not commit time,
-- so concurrent passes can commit out of cursor order — which makes a plain
-- `> cursor` poll lossy no matter what the cursor is drawn from. The
-- difference is that Postgres will tell you which *transactions* are still in
-- flight and will not tell you that about a sequence: everything below
-- pg_snapshot_xmin(pg_current_snapshot()) is committed by definition, so a
-- reader bounded by it can never be overtaken. That bound is only expressible
-- if the cursor is the transaction id, which is why this is not nextval().
--
-- Read through ListChangedSpanTurns / ListChangedSpans rather than rolling the
-- bound by hand. See the 1781470000 migration for the full argument.
SELECT pg_current_xact_id()::text::bigint;

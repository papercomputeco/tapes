-- name: SaveSession :one
-- Idempotent org-wide save, gated on the session actually belonging to the
-- caller's org: the INSERT..SELECT inserts nothing when (id, org_id) has no
-- match in sessions, and pgx surfaces that as ErrNoRows — the driver maps it
-- to "not found". The FK still guards dangling ids and cascades deletes.
-- On conflict the existing row is preserved via a no-op DO UPDATE (instead
-- of DO NOTHING) so RETURNING still emits it — sqlc/pgx treats DO NOTHING
-- RETURNING as "no rows" on conflict, the same trick
-- InsertSessionPlaceholder uses. First saver's attribution wins: saved_by
-- and saved_at are never overwritten.
INSERT INTO saved_sessions (org_id, session_id, saved_by, saved_at)
SELECT s.org_id, s.id, sqlc.arg(saved_by), sqlc.arg(now)
FROM sessions s
WHERE s.id = sqlc.arg(session_id) AND s.org_id = sqlc.arg(org_id)
ON CONFLICT (org_id, session_id) DO UPDATE
SET saved_at = saved_sessions.saved_at
RETURNING *;

-- name: UnsaveSession :execrows
-- Org-wide unsave: removes the shared marker for everyone. Idempotent —
-- the caller reads the row count to distinguish deleted from already-absent.
DELETE FROM saved_sessions
WHERE org_id = sqlc.arg(org_id) AND session_id = sqlc.arg(session_id);

-- name: ListSavedSessions :many
-- Every saved marker in the org, newest-saved-first. Small by construction
-- (a curated shortlist), so no pagination.
SELECT * FROM saved_sessions
WHERE org_id = sqlc.arg(org_id)
ORDER BY saved_at DESC, session_id DESC;

-- name: UpsertSessionRecap :one
-- Insert-or-replace a session's recap keyed by (org_id, session_id) — one
-- recap per session, regeneration overwrites (latest wins). Every field but
-- the key is replaced on conflict: a regenerated recap supersedes the prior
-- narrative/observation wholesale, and turn_count/generated_at move with it.
INSERT INTO session_recaps (
    org_id,
    session_id,
    narrative,
    observation,
    turn_count,
    model,
    generated_at
) VALUES (
    sqlc.arg(org_id),
    sqlc.arg(session_id),
    sqlc.arg(narrative),
    sqlc.arg(observation),
    sqlc.arg(turn_count),
    sqlc.arg(model),
    sqlc.arg(generated_at)
)
ON CONFLICT (org_id, session_id) DO UPDATE
SET narrative    = EXCLUDED.narrative,
    observation  = EXCLUDED.observation,
    turn_count   = EXCLUDED.turn_count,
    model        = EXCLUDED.model,
    generated_at = EXCLUDED.generated_at
RETURNING *;

-- name: GetSessionRecap :one
-- Org-scoped point read used by GET /v1/sessions/:id/recap and the generate
-- handler's cache check (a recap whose turn_count matches the session is
-- returned without an LLM call).
SELECT * FROM session_recaps
WHERE org_id = sqlc.arg(org_id) AND session_id = sqlc.arg(session_id);

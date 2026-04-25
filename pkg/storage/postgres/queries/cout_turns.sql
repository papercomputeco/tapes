-- name: CountTurns :one
SELECT COUNT(*)
FROM nodes n
WHERE (sqlc.narg(project_filter)::text IS NULL OR n.project = sqlc.narg(project_filter)::text)
  AND (sqlc.narg(agent_filter)::text IS NULL OR n.agent_name = sqlc.narg(agent_filter)::text)
  AND (sqlc.narg(model_filter)::text IS NULL OR n.model = sqlc.narg(model_filter)::text)
  AND (sqlc.narg(provider_filter)::text IS NULL OR n.provider = sqlc.narg(provider_filter)::text)
  AND (sqlc.narg(since_filter)::timestamptz IS NULL OR n.created_at >= sqlc.narg(since_filter)::timestamptz)
  AND (sqlc.narg(until_filter)::timestamptz IS NULL OR n.created_at < sqlc.narg(until_filter)::timestamptz);

-- name: ListSessions :many
SELECT n.hash, n.bucket, n.type, n.role, n.content, n.model, n.provider, n.agent_name, n.stop_reason,
       n.prompt_tokens, n.completion_tokens, n.total_tokens,
       n.cache_creation_input_tokens, n.cache_read_input_tokens,
       n.total_duration_ns, n.prompt_duration_ns, n.project, n.created_at, n.parent_hash
FROM nodes n
WHERE NOT EXISTS (
    SELECT 1 FROM nodes c WHERE c.parent_hash = n.hash
)
  AND (sqlc.narg(project_filter)::text IS NULL OR n.project = sqlc.narg(project_filter)::text)
  AND (sqlc.narg(agent_filter)::text IS NULL OR n.agent_name = sqlc.narg(agent_filter)::text)
  AND (sqlc.narg(model_filter)::text IS NULL OR n.model = sqlc.narg(model_filter)::text)
  AND (sqlc.narg(provider_filter)::text IS NULL OR n.provider = sqlc.narg(provider_filter)::text)
  AND (sqlc.narg(since_filter)::timestamptz IS NULL OR n.created_at >= sqlc.narg(since_filter)::timestamptz)
  AND (sqlc.narg(until_filter)::timestamptz IS NULL OR n.created_at < sqlc.narg(until_filter)::timestamptz)
  AND (
    sqlc.narg(cursor_created_at)::timestamptz IS NULL
    OR n.created_at < sqlc.narg(cursor_created_at)::timestamptz
    OR (n.created_at = sqlc.narg(cursor_created_at)::timestamptz AND n.hash < sqlc.narg(cursor_hash)::text)
  )
ORDER BY n.created_at DESC, n.hash DESC
LIMIT sqlc.arg(limit_count);

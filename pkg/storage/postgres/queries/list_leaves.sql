-- name: ListLeaves :many
SELECT n.hash, n.bucket, n.type, n.role, n.content, n.model, n.provider, n.agent_name, n.stop_reason,
       n.prompt_tokens, n.completion_tokens, n.total_tokens,
       n.cache_creation_input_tokens, n.cache_read_input_tokens,
       n.total_duration_ns, n.prompt_duration_ns, n.project, n.created_at, n.parent_hash,
       n.session_id, n.org_id,
       n.request_system, n.request_max_tokens, n.request_temperature, n.request_stream, n.request_tool_count
FROM nodes n
WHERE NOT EXISTS (
    SELECT 1 FROM nodes c WHERE c.parent_hash = n.hash
)
ORDER BY n.created_at ASC, n.hash ASC;

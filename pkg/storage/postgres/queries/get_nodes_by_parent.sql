-- name: GetNodesByParent :many
SELECT hash, bucket, type, role, content, model, provider, agent_name, stop_reason,
       prompt_tokens, completion_tokens, total_tokens,
       cache_creation_input_tokens, cache_read_input_tokens,
       total_duration_ns, prompt_duration_ns, project, created_at, parent_hash,
       session_id, org_id,
       request_system, request_max_tokens, request_temperature, request_stream, request_tool_count,
       node_kind, parent_tool_use_id, thread_id
FROM nodes
WHERE parent_hash = $1
ORDER BY created_at ASC, hash ASC;

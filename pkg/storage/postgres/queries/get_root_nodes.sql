-- name: GetRootNodes :many
SELECT hash, bucket, type, role, content, model, provider, agent_name, stop_reason,
       prompt_tokens, completion_tokens, total_tokens,
       cache_creation_input_tokens, cache_read_input_tokens,
       total_duration_ns, prompt_duration_ns, project, created_at, parent_hash,
       session_id, org_id,
       request_system, request_max_tokens, request_temperature, request_stream, request_tool_count
FROM nodes
WHERE parent_hash IS NULL
ORDER BY created_at ASC, hash ASC;

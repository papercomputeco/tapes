-- name: ListNodes :many
SELECT hash, bucket, type, role, content, model, provider, agent_name, stop_reason,
       prompt_tokens, completion_tokens, total_tokens,
       cache_creation_input_tokens, cache_read_input_tokens,
       total_duration_ns, prompt_duration_ns, project, created_at, parent_hash,
       session_id, org_id
FROM nodes
ORDER BY created_at ASC, hash ASC;

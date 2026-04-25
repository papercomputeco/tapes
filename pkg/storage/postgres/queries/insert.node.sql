-- name: InsertNode :execrows
INSERT INTO nodes (
    hash, bucket, type, role, content, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at, parent_hash
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9,
    $10, $11, $12,
    $13, $14,
    $15, $16, $17, $18, $19
)
ON CONFLICT (hash) DO NOTHING;

-- name: ListParentRefs :many
SELECT hash, parent_hash
FROM nodes
ORDER BY created_at ASC, hash ASC;

-- name: UpdateUsage :exec
UPDATE nodes
SET prompt_tokens = COALESCE($2, prompt_tokens),
    completion_tokens = COALESCE($3, completion_tokens),
    total_tokens = COALESCE($4, total_tokens),
    cache_creation_input_tokens = COALESCE($5, cache_creation_input_tokens),
    cache_read_input_tokens = COALESCE($6, cache_read_input_tokens),
    total_duration_ns = COALESCE($7, total_duration_ns),
    prompt_duration_ns = COALESCE($8, prompt_duration_ns)
WHERE hash = $1;

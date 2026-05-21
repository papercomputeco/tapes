-- name: InsertNode :execrows
-- nodes is keyed by composite PK (org_id, hash) so the same content
-- (e.g. a verbatim system prompt) can land for multiple orgs without
-- the first writer permanently owning the row. Legacy non-session
-- writers pass the nil-UUID (00000000-0000-0000-0000-000000000000)
-- per the migration default; session-aware writers (IngestTurn) pass
-- the validated cloud-trusted org_id.
INSERT INTO nodes (
    org_id, hash, bucket, type, role, content, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at, parent_hash
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13,
    $14, $15,
    $16, $17, $18, $19, $20
)
ON CONFLICT (org_id, hash) DO NOTHING;

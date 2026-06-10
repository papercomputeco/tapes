-- name: UpsertDerivedNode :execrows
-- The re-runnable deriver's write: insert a node derived from the raw
-- layer, or — when the content row already exists for this org —
-- refresh only the DERIVED columns (typing, edges, session
-- attribution, promoted request params). bucket/content/usage are part
-- of the content row and never change for a given hash; created_at is
-- preserved on conflict so capture-time chronology survives re-derives.
INSERT INTO nodes (
    org_id, hash, bucket, type, role, content, model, provider, agent_name, stop_reason,
    prompt_tokens, completion_tokens, total_tokens,
    cache_creation_input_tokens, cache_read_input_tokens,
    total_duration_ns, prompt_duration_ns, project, created_at, parent_hash,
    request_system, request_max_tokens, request_temperature, request_stream, request_tool_count,
    node_kind, parent_tool_use_id, session_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13,
    $14, $15,
    $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25,
    $26, $27, $28
)
ON CONFLICT (org_id, hash) DO UPDATE SET
    node_kind           = EXCLUDED.node_kind,
    parent_tool_use_id  = EXCLUDED.parent_tool_use_id,
    session_id          = COALESCE(nodes.session_id, EXCLUDED.session_id),
    request_system      = COALESCE(EXCLUDED.request_system, nodes.request_system),
    request_max_tokens  = COALESCE(EXCLUDED.request_max_tokens, nodes.request_max_tokens),
    request_temperature = COALESCE(EXCLUDED.request_temperature, nodes.request_temperature),
    request_stream      = COALESCE(EXCLUDED.request_stream, nodes.request_stream),
    request_tool_count  = COALESCE(EXCLUDED.request_tool_count, nodes.request_tool_count);

-- name: PruneDerivedNodes :execrows
-- Remove derived rows for the given sessions whose hashes are no
-- longer produced by re-deriving the raw layer (e.g. rows written
-- under a superseded projection). Scoped to sessions the deriver
-- actually rebuilt, so legacy sessions without raw coverage are
-- untouched.
DELETE FROM nodes
WHERE org_id = $1
  AND session_id = ANY(sqlc.arg(session_ids)::uuid[])
  AND NOT (hash = ANY(sqlc.arg(keep_hashes)::text[]));

-- name: SessionIDByHarnessKey :one
-- Resolve the sessions row for one raw turn's natural key. The deriver
-- only attributes to existing sessions; it never creates them.
SELECT id FROM sessions
WHERE org_id = $1
  AND harness_id = $2
  AND harness_session_id = $3;

-- name: ListRawTurnIndex :many
-- Lightweight scan for the deriver's ordering pass: identity and
-- timing only, no payloads. meta rides along because it carries the
-- original capture time for backfilled rows.
SELECT id, org_id, source, harness_id, harness_session_id, received_at, meta
FROM raw_turns
WHERE id > sqlc.arg(after_id)
ORDER BY id
LIMIT sqlc.arg(page_size);

-- name: GetRawTurn :one
SELECT id, org_id, source, provider, agent_name,
       harness_id, harness_session_id, request_id,
       raw_request, response, meta, session_envelope, received_at
FROM raw_turns
WHERE id = $1;

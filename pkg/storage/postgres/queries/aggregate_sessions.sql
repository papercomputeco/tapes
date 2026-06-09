-- name: AggregateSessions :one
--
-- Single-pass aggregate that powers /v1/stats. SUMs and turn/stem/root
-- counts apply to the set of nodes matching the supplied per-node filters.
--
-- session_count and completed_count are session-grained, keyed on the
-- first-class sessions table via nodes.session_id (PCC-565):
--   session_count   = distinct sessions touched by the matching nodes
--   completed_count = distinct sessions whose denormalized derived_status
--                     is 'completed' (chain-aware, computed at ingest by
--                     pkg/sessions.DetermineStatus — PCC-515)
-- Nodes with no session_id (legacy / non-session-tracked writers) are not
-- counted as sessions; COUNT(DISTINCT session_id) ignores their NULLs.
--
-- stem_count is the leaf count (one per Merkle chain, matching /v1/stems) —
-- the value session_count used to report before the sessions table existed.
--
-- total_duration_ns is wall-clock span MAX(created_at) - MIN(created_at)
-- across the matching set, NOT SUM(nodes.total_duration_ns). The
-- nodes.total_duration_ns column is currently never populated by the
-- proxy (verified against jason@'s local store: 0 of 1460 rows have a
-- non-zero value) — see PCC-514. Until that lands, SUMming the column
-- would always return 0, which is misleading; wall-clock span is a
-- meaningful "Agent Time" proxy that doesn't depend on the dead column.
--
-- The tool_calls subquery scans content JSONB per node to count tool_use
-- blocks. This is acceptable at current corpus sizes; if it becomes a
-- bottleneck the right next step is denormalizing tool_use_count onto
-- the nodes table at write time.
WITH filtered AS (
    SELECT
        n.parent_hash,
        n.role,
        n.stop_reason,
        n.content,
        n.created_at,
        n.prompt_tokens,
        n.completion_tokens,
        n.cache_creation_input_tokens,
        n.cache_read_input_tokens,
        n.session_id,
        s.derived_status,
        EXISTS (SELECT 1 FROM nodes c WHERE c.parent_hash = n.hash) AS has_child
    FROM nodes n
    LEFT JOIN sessions s ON s.id = n.session_id
    WHERE (sqlc.narg(project_filter)::text IS NULL OR n.project = sqlc.narg(project_filter)::text)
      AND (sqlc.narg(agent_filter)::text IS NULL OR n.agent_name = sqlc.narg(agent_filter)::text)
      AND (sqlc.narg(model_filter)::text IS NULL OR n.model = sqlc.narg(model_filter)::text)
      AND (sqlc.narg(provider_filter)::text IS NULL OR n.provider = sqlc.narg(provider_filter)::text)
      AND (sqlc.narg(since_filter)::timestamptz IS NULL OR n.created_at >= sqlc.narg(since_filter)::timestamptz)
      AND (sqlc.narg(until_filter)::timestamptz IS NULL OR n.created_at < sqlc.narg(until_filter)::timestamptz)
)
SELECT
    COUNT(*)::bigint                                                         AS turn_count,
    COUNT(DISTINCT session_id)::bigint                                       AS session_count,
    COUNT(*) FILTER (WHERE NOT has_child)::bigint                            AS stem_count,
    COUNT(*) FILTER (WHERE parent_hash IS NULL)::bigint                      AS root_count,
    COUNT(DISTINCT session_id) FILTER (
        WHERE derived_status = 'completed'
    )::bigint                                                                AS completed_count,
    COALESCE(SUM(prompt_tokens), 0)::bigint                                  AS input_tokens,
    COALESCE(SUM(completion_tokens), 0)::bigint                              AS output_tokens,
    COALESCE(SUM(cache_creation_input_tokens), 0)::bigint                    AS cache_creation_tokens,
    COALESCE(SUM(cache_read_input_tokens), 0)::bigint                        AS cache_read_tokens,
    COALESCE(
        EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) * 1e9,
        0
    )::bigint                                                                AS total_duration_ns,
    COALESCE(SUM((
        SELECT COUNT(*)
        FROM jsonb_array_elements(content) AS block
        WHERE block->>'type' = 'tool_use'
    )), 0)::bigint                                                           AS tool_calls
FROM filtered;

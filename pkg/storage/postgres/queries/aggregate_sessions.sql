-- name: AggregateSessions :one
--
-- Single-pass aggregate that powers /v1/stats. All counts and SUMs apply
-- to the set of nodes matching the supplied per-node filters; a "session"
-- is identified as a leaf node (no child references it as parent_hash).
--
-- completed_count is leaf-status-only: assistant role plus a terminal
-- stop_reason. It deliberately omits the chain-context overrides
-- (hasToolError / hasGitActivity) that pkg/sessions.DetermineStatus
-- applies, so a single SQL aggregate is sufficient. See StatsResponse
-- in api/v1_handlers.go for the rationale, and PCC-515 for the durable
-- chain-aware fix.
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
        EXISTS (SELECT 1 FROM nodes c WHERE c.parent_hash = n.hash) AS has_child
    FROM nodes n
    WHERE (sqlc.narg(project_filter)::text IS NULL OR n.project = sqlc.narg(project_filter)::text)
      AND (sqlc.narg(agent_filter)::text IS NULL OR n.agent_name = sqlc.narg(agent_filter)::text)
      AND (sqlc.narg(model_filter)::text IS NULL OR n.model = sqlc.narg(model_filter)::text)
      AND (sqlc.narg(provider_filter)::text IS NULL OR n.provider = sqlc.narg(provider_filter)::text)
      AND (sqlc.narg(since_filter)::timestamptz IS NULL OR n.created_at >= sqlc.narg(since_filter)::timestamptz)
      AND (sqlc.narg(until_filter)::timestamptz IS NULL OR n.created_at < sqlc.narg(until_filter)::timestamptz)
)
SELECT
    COUNT(*)::bigint                                                         AS turn_count,
    COUNT(*) FILTER (WHERE NOT has_child)::bigint                            AS session_count,
    COUNT(*) FILTER (WHERE parent_hash IS NULL)::bigint                      AS root_count,
    COUNT(*) FILTER (
        WHERE NOT has_child
          AND LOWER(role) = 'assistant'
          AND LOWER(stop_reason) IN ('stop', 'end_turn', 'end-turn', 'eos')
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

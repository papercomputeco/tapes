-- name: AggregateSessionsByModel :many
--
-- Per-model token rollup that powers the cost fold in /v1/stats.
-- Filters apply per-node, matching aggregate_sessions.sql. Nodes with a
-- NULL or empty model are excluded — they cannot be priced.
--
-- The API handler walks these rows, applies pkg/sessions.PricingForModel
-- and pkg/sessions.CostForTokensWithCache to each, and sums into
-- StatsResponse.TotalCost. Pricing intentionally lives in Go.
SELECT
    n.model                                                  AS model,
    COALESCE(SUM(n.prompt_tokens), 0)::bigint                AS input_tokens,
    COALESCE(SUM(n.completion_tokens), 0)::bigint            AS output_tokens,
    COALESCE(SUM(n.cache_creation_input_tokens), 0)::bigint  AS cache_creation_tokens,
    COALESCE(SUM(n.cache_read_input_tokens), 0)::bigint      AS cache_read_tokens
FROM nodes n
WHERE n.model IS NOT NULL AND n.model <> ''
  AND (sqlc.narg(project_filter)::text IS NULL OR n.project = sqlc.narg(project_filter)::text)
  AND (sqlc.narg(agent_filter)::text IS NULL OR n.agent_name = sqlc.narg(agent_filter)::text)
  AND (sqlc.narg(model_filter)::text IS NULL OR n.model = sqlc.narg(model_filter)::text)
  AND (sqlc.narg(provider_filter)::text IS NULL OR n.provider = sqlc.narg(provider_filter)::text)
  AND (sqlc.narg(since_filter)::timestamptz IS NULL OR n.created_at >= sqlc.narg(since_filter)::timestamptz)
  AND (sqlc.narg(until_filter)::timestamptz IS NULL OR n.created_at < sqlc.narg(until_filter)::timestamptz)
GROUP BY n.model;

-- Inverse of 1781117411_node_kind_edges.up.sql. Restores the previous
-- ancestry_chains_rows signature and drops the derived typing columns
-- (recomputable from raw_turns on a future re-derive).

DROP FUNCTION IF EXISTS ancestry_chains_rows(TEXT[], INTEGER);
CREATE OR REPLACE FUNCTION ancestry_chains_rows(input_hashes TEXT[], input_max_depth INTEGER)
RETURNS TABLE (
    start_hash TEXT,
    hash TEXT,
    parent_hash TEXT,
    bucket JSONB,
    type TEXT,
    role TEXT,
    content JSONB,
    model TEXT,
    provider TEXT,
    agent_name TEXT,
    stop_reason TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    cache_creation_input_tokens INTEGER,
    cache_read_input_tokens INTEGER,
    total_duration_ns BIGINT,
    prompt_duration_ns BIGINT,
    project TEXT,
    created_at TIMESTAMPTZ,
    depth INTEGER,
    has_usage BOOLEAN
)
LANGUAGE SQL
AS $$
    WITH RECURSIVE walk AS (
        SELECT
            n.hash AS start_hash,
            n.hash,
            n.parent_hash,
            0 AS depth
        FROM nodes n
        WHERE n.hash = ANY(input_hashes)

        UNION ALL

        SELECT
            walk.start_hash,
            n.hash,
            n.parent_hash,
            walk.depth + 1
        FROM nodes n
        JOIN walk ON n.hash = walk.parent_hash
        WHERE walk.depth < input_max_depth
    )
    SELECT
        walk.start_hash,
        n.hash,
        n.parent_hash,
        n.bucket,
        n.type,
        n.role,
        n.content,
        n.model,
        n.provider,
        n.agent_name,
        n.stop_reason,
        n.prompt_tokens,
        n.completion_tokens,
        n.total_tokens,
        n.cache_creation_input_tokens,
        n.cache_read_input_tokens,
        n.total_duration_ns,
        n.prompt_duration_ns,
        n.project,
        n.created_at,
        walk.depth,
        (
            n.prompt_tokens IS NOT NULL
            OR n.completion_tokens IS NOT NULL
            OR n.total_tokens IS NOT NULL
            OR n.cache_creation_input_tokens IS NOT NULL
            OR n.cache_read_input_tokens IS NOT NULL
            OR n.total_duration_ns IS NOT NULL
            OR n.prompt_duration_ns IS NOT NULL
        ) AS has_usage
    FROM walk
    JOIN nodes n ON n.hash = walk.hash;
$$;

DROP INDEX IF EXISTS nodes_parent_tool_use_idx;
DROP INDEX IF EXISTS nodes_node_kind_idx;

ALTER TABLE nodes
    DROP COLUMN IF EXISTS parent_tool_use_id,
    DROP COLUMN IF EXISTS node_kind;

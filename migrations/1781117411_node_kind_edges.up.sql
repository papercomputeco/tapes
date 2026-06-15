-- Phase 2 of the reconciled conversation tree (see
-- design/agent-session-reconciliation.md): semantic typing + edges on
-- the derived node layer.
--
-- node_kind classifies the API call (or injected block) that produced
-- each node. A "session" on the wire is many calls of different kinds:
-- the conversation spine ('main') plus the harness's shadow calls
-- (security monitor, title-gen, suggestion, web-summary, …) and
-- injected context blocks. The taxonomy is the design doc's §2g enum:
--   main
--   offshoot:permission-check:stage1 | :stage2
--   offshoot:title-gen | plan-name-gen | suggestion | web-summary
--   offshoot:probe | compaction          (provisional, prod-observed)
--   injected:session-wrapper | conversation | mcp-instructions |
--   injected:skills-list | mode-banner | …
--   unknown                              (surfaced for investigation)
-- The set is OPEN: the deriver is re-runnable, so newly-cataloged kinds
-- reclassify existing raw data without re-capture.
--
-- parent_tool_use_id is the semantic fork/attach edge: a permission
-- verdict points at the tool_use it judged; a subagent's nodes point at
-- the Task tool_use that forked it (Phase 3); a web-summary points at
-- its WebFetch/WebSearch call. It complements parent_hash (the chain
-- edge) rather than replacing it.
--
-- Both are DERIVED columns: recomputable from raw_turns at any time,
-- never part of the content-addressed hash.

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS node_kind          TEXT,
    ADD COLUMN IF NOT EXISTS parent_tool_use_id TEXT;

CREATE INDEX IF NOT EXISTS nodes_node_kind_idx
    ON nodes (node_kind);

CREATE INDEX IF NOT EXISTS nodes_parent_tool_use_idx
    ON nodes (parent_tool_use_id)
    WHERE parent_tool_use_id IS NOT NULL;

-- Carry the new typing through the recursive graph walk so the read
-- side (stems/:hash/graph and the Phase-4 tree endpoint) can emit it
-- without per-node lookups. The RETURNS TABLE shape changes, so the
-- function must be dropped first (CREATE OR REPLACE cannot alter a
-- return type).
DROP FUNCTION IF EXISTS ancestry_chains_rows(TEXT[], INTEGER);

CREATE FUNCTION ancestry_chains_rows(input_hashes TEXT[], input_max_depth INTEGER)
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
    has_usage BOOLEAN,
    node_kind TEXT,
    parent_tool_use_id TEXT
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
        ) AS has_usage,
        n.node_kind,
        n.parent_tool_use_id
    FROM walk
    JOIN nodes n ON n.hash = walk.hash;
$$;

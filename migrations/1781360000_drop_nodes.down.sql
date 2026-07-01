-- Recreate the empty nodes shell and its ancestry walk function.
--
-- This is for schema-rollback consistency only: nothing repopulates
-- nodes anymore (the deriver and ingest paths no longer write them), so
-- a rolled-back database simply has an empty table. The DDL mirrors the
-- table's final pre-drop shape.

CREATE TABLE nodes (
    hash text NOT NULL,
    bucket jsonb,
    type text,
    role text,
    content jsonb,
    model text,
    provider text,
    agent_name text,
    stop_reason text,
    prompt_tokens integer,
    completion_tokens integer,
    total_tokens integer,
    cache_creation_input_tokens integer,
    cache_read_input_tokens integer,
    total_duration_ns bigint,
    prompt_duration_ns bigint,
    project text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    parent_hash text,
    session_id uuid,
    org_id uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid NOT NULL,
    request_system text,
    request_max_tokens integer,
    request_temperature double precision,
    request_stream boolean,
    request_tool_count integer,
    node_kind text,
    parent_tool_use_id text,
    thread_id text,
    CONSTRAINT nodes_pkey PRIMARY KEY (org_id, hash),
    CONSTRAINT nodes_session_id_fkey FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE INDEX node_agent_name ON nodes USING btree (agent_name);
CREATE INDEX node_created_at ON nodes USING btree (created_at);
CREATE INDEX node_model ON nodes USING btree (model);
CREATE INDEX node_parent_hash ON nodes USING btree (parent_hash);
CREATE INDEX node_project ON nodes USING btree (project);
CREATE INDEX node_provider ON nodes USING btree (provider);
CREATE INDEX node_role ON nodes USING btree (role);
CREATE INDEX node_role_model ON nodes USING btree (role, model);
CREATE INDEX nodes_hash_idx ON nodes USING btree (hash);
CREATE INDEX nodes_node_kind_idx ON nodes USING btree (node_kind);
CREATE INDEX nodes_parent_tool_use_idx ON nodes USING btree (parent_tool_use_id) WHERE (parent_tool_use_id IS NOT NULL);
CREATE INDEX nodes_session_idx ON nodes USING btree (session_id);
CREATE INDEX nodes_thread_idx ON nodes USING btree (thread_id) WHERE (thread_id IS NOT NULL);

CREATE OR REPLACE FUNCTION ancestry_chains_rows(input_hashes text[], input_max_depth integer)
 RETURNS TABLE(start_hash text, hash text, parent_hash text, bucket jsonb, type text, role text, content jsonb, model text, provider text, agent_name text, stop_reason text, prompt_tokens integer, completion_tokens integer, total_tokens integer, cache_creation_input_tokens integer, cache_read_input_tokens integer, total_duration_ns bigint, prompt_duration_ns bigint, project text, created_at timestamp with time zone, depth integer, has_usage boolean, node_kind text, parent_tool_use_id text)
 LANGUAGE sql
AS $function$
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
$function$;

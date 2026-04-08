-- 001_baseline_schema: nodes table (PostgreSQL)

CREATE TABLE IF NOT EXISTS nodes (
    hash TEXT NOT NULL PRIMARY KEY,
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    parent_hash TEXT REFERENCES nodes(hash) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS node_parent_hash ON nodes(parent_hash);
CREATE INDEX IF NOT EXISTS node_role ON nodes(role);
CREATE INDEX IF NOT EXISTS node_model ON nodes(model);
CREATE INDEX IF NOT EXISTS node_provider ON nodes(provider);
CREATE INDEX IF NOT EXISTS node_agent_name ON nodes(agent_name);
CREATE INDEX IF NOT EXISTS node_role_model ON nodes(role, model);
CREATE INDEX IF NOT EXISTS node_project ON nodes(project);
CREATE INDEX IF NOT EXISTS node_created_at ON nodes(created_at);

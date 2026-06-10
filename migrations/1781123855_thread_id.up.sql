-- Harness sub-thread attribution, captured deterministically at the
-- wire (extproc resolves the harness's native header — Claude Code's
-- x-claude-code-agent-id — onto a neutral meta.thread_id). A non-empty
-- thread_id means the call was fired from a subagent context; the
-- value matches the harness transcript's agent id, so fork edges
-- attach by identity instead of content similarity.

ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS thread_id TEXT;

CREATE INDEX IF NOT EXISTS nodes_thread_idx
    ON nodes (thread_id)
    WHERE thread_id IS NOT NULL;

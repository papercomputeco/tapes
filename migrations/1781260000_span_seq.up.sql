-- Presentation order for spans. started_at cannot order spans within
-- one llm call: a parallel tool batch stamps every tool span with the
-- same instant, and span_id (provider tool_use_id) tie-breaks are
-- random — TaskCreate folds and conversation rendering both scrambled.
-- seq is the deriver's emit ordinal within the trace (the spine-walk
-- order), so ORDER BY seq is the turn as the conversation happened.
-- Re-derive repopulates it; rows written before this migration carry 0
-- until their next derive pass.
ALTER TABLE spans ADD COLUMN IF NOT EXISTS seq BIGINT NOT NULL DEFAULT 0;

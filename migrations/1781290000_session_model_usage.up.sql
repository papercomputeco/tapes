-- The session detail wants a per-model spend breakdown so the UI can
-- show a dominant model and per-model share without fetching spans. The
-- share basis is COST (priced at derive time), not call count: a
-- fan-out of cheap subagent calls would otherwise out-vote the
-- expensive main-spine model. Stored as a JSONB array of
-- {model, calls, input_tokens, output_tokens, cost_usd}, folded across
-- every thread (subagent models included). Re-derive repopulates it;
-- pre-migration rows carry NULL until their next derive pass.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS model_usage JSONB;

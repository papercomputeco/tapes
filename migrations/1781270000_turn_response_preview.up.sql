-- Turn cards render from trace summaries (no span payloads), so the
-- turn row needs the answer line folded at derive time — the response
-- counterpart of user_prompt: the closing conversation-spine llm
-- call's text output, truncated. Re-derive populates it; rows written
-- before this migration carry '' until their next derive pass.
ALTER TABLE span_turns ADD COLUMN IF NOT EXISTS response_preview TEXT NOT NULL DEFAULT '';

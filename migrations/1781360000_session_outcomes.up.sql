-- Outcomes: the artifacts a session produced (pull requests, repos,
-- issues), detected from tool spans at derive time (PCC-840). Stored as
-- a JSONB array of {kind, url, title, repo, trace_id, span_id,
-- detected_by, detected_at} — url is the artifact's identity (the fold
-- dedupes on it), trace_id/span_id point back at the detecting tool
-- span (the memory stream's source-pointer convention), and detected_at
-- is that span's start time so a re-derive reproduces the fold.
-- Re-derive repopulates it; pre-migration rows carry NULL until their
-- next derive pass. New sessions only by design — no backfill (PCC-837).
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS outcomes JSONB;

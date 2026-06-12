-- The session overview needs a model without fetching spans: fold the
-- dominant conversation-spine model at derive time, next to the other
-- derived_* session facts. Re-derive populates it; pre-migration rows
-- carry '' until their next derive pass.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS derived_model TEXT NOT NULL DEFAULT '';

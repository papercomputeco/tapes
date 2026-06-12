-- A continuously streaming session resets dirtied_at on every capture
-- and never settles — the debounce starves derives until a quiet gap.
-- first_dirtied_at survives re-marks, so the worker can bound the wait:
-- derive when settled OR when the mark has been pending too long.
ALTER TABLE derive_queue
    ADD COLUMN IF NOT EXISTS first_dirtied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;
UPDATE derive_queue SET first_dirtied_at = dirtied_at;

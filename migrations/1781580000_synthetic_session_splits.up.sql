-- Idle-split segments of synthetic (root-derived) sessions. A turn that
-- carries no harness session id is keyed by its conversation root; a
-- long-running conversation (a bot replying to one ever-growing history) is
-- split into a new session after an idle gap. Each row is one split: the
-- harness_session_id its turns are stamped with and the capture-time range
-- of the turns assigned to it. Ingest reads a conversation's rows to place
-- the next turn and widens the chosen row; nothing else writes here.
--
-- Rows are keyed by the split's natural session key, the same triple as
-- sessions, so a split is addressable exactly like the session it names.
CREATE TABLE IF NOT EXISTS synthetic_session_splits (
    org_id             UUID NOT NULL,
    harness_id         TEXT NOT NULL,
    harness_session_id TEXT NOT NULL,
    conversation_key   TEXT NOT NULL,
    first_captured_at  TIMESTAMPTZ NOT NULL,
    last_captured_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (org_id, harness_id, harness_session_id)
);

CREATE INDEX IF NOT EXISTS synthetic_session_splits_conversation_idx
    ON synthetic_session_splits (org_id, harness_id, conversation_key);

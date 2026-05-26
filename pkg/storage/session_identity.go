package storage

// SessionIdentity is the harness-level identity attached to a persisted
// sessions row. For Claude Code captures, HarnessID is "claude" and
// HarnessSessionID is Claude's own session id.
type SessionIdentity struct {
	HarnessID        string `json:"harness_id,omitempty"`
	HarnessSessionID string `json:"harness_session_id,omitempty"`
}

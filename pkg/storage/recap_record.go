package storage

import "time"

// SessionRecapRecord is the flat session_recaps-table row surfaced by the
// /v1/sessions/{id}/recap API (PCC-241): the on-demand LLM narrative of a
// session, one row per session (regeneration upserts, latest wins).
type SessionRecapRecord struct {
	SessionID string
	// Narrative is the 2-3 sentence LLM summary of what the person/agent set
	// out to do and how it went (present tense while the session is live).
	Narrative string
	// Observation is an optional transferable insight extracted by the same
	// LLM pass — a recap is a chance for an observation. Empty when the pass
	// found nothing noteworthy. Feedstock for the Dreams observational-memory
	// queue once that backend ships.
	Observation string
	// TurnCount is the staleness anchor: session.turn_count at generation
	// time. A recap whose TurnCount still matches the session is current
	// (and immutable — generate returns it without an LLM call); a live
	// session that kept working advances past it, and the console prompts
	// for an update.
	TurnCount int
	// Model is the LLM that authored the recap.
	Model       string
	GeneratedAt time.Time
}

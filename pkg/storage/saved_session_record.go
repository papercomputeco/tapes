package storage

import "time"

// SavedSessionRecord is one org-wide saved-session marker: a shared
// team-shortlist entry, not a per-user bookmark. SavedBy is the
// auth_subject of the first saver — attribution only, never an
// ownership gate (anyone in the org may unsave). Empty when the caller
// sent no subject header.
type SavedSessionRecord struct {
	SessionID string
	SavedBy   string
	SavedAt   time.Time
}

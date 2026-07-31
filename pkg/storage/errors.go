package storage

import "errors"

// ErrInvalidContent signals that a write failed because the payload
// content itself is unstorable — JSON carrying escape sequences or bytes
// a Postgres JSONB column rejects (SQLSTATE 22P05 unsupported Unicode
// escape, 22021 invalid byte sequence) — rather than any infrastructure
// fault.
//
// The distinction drives behavior at the boundaries: an HTTP handler maps
// it to a client 4xx (the data is the problem; retrying the identical
// bytes will never succeed) instead of a 502 that reads as a gateway
// outage, and the derive worker can quarantine such a row instead of
// re-attempting it on every poll forever. Drivers wrap the underlying pg
// error with this sentinel; callers test with errors.Is.
var ErrInvalidContent = errors.New("invalid content for storage")

var (
	// ErrRawTurnNotFound means the requested raw turn does not exist in the
	// caller's organization (including correlation selectors with no match).
	ErrRawTurnNotFound = errors.New("raw turn not found")

	// ErrRawTurnAmbiguous means a correlation selector matched more than one
	// immutable raw row and therefore cannot be repaired safely.
	ErrRawTurnAmbiguous = errors.New("raw turn selector is ambiguous")

	// ErrRepairProjectionsPending means an attribution correction committed —
	// it is already effective at read time — but at least one synchronous
	// projection rebuild failed afterwards. The affected sessions stay queued
	// for the derive worker (the correction transaction marks both dirty), so
	// the projections converge without operator action. Callers should treat
	// the repair as accepted-but-settling, not failed: the accompanying
	// result is valid and lists the still-stale sessions.
	ErrRepairProjectionsPending = errors.New("attribution correction recorded; projection rebuild pending")
)

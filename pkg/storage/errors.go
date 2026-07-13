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

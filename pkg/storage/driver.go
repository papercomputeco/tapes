// Package storage
package storage

import (
	"context"
)

// Driver is the lifecycle handle for a storage backend.
//
// The persisted merkle "node" DAG has been retired: the in-memory merkle
// chain is still built at derive time for provenance and dedup, but it is
// no longer written to or read from the store. What remains here is the
// open/close lifecycle. The actual read/write surfaces (raw-turn capture,
// session ingest, the derived sessions/traces/spans projection) are
// exposed as capability interfaces (RawTurnStore, SessionIngester, the
// derive/read interfaces) that callers type-assert off the concrete
// driver.
type Driver interface {
	// Open initializes the backing store and makes it ready for use.
	Open(ctx context.Context) error

	// Close closes the store and releases any resources.
	Close() error
}

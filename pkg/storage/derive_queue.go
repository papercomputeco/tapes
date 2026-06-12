package storage

import (
	"context"
	"time"
)

// DeriveQueueEntry is one dirty harness session awaiting re-derivation.
// The key is the deriver's natural unit — the harness triple — NOT a
// sessions-row id: a sessions row is not guaranteed to exist when a
// raw turn lands (transcript ingest writes only a raw row).
type DeriveQueueEntry struct {
	// OrgID is the canonical UUID string; empty means "no org context"
	// and maps to the nil-UUID sentinel, mirroring raw_turns.org_id.
	OrgID string

	HarnessID        string
	HarnessSessionID string

	// DirtiedAt is when the most recent raw turn dirtied the session.
	// The worker debounces on it and clears the entry only if it is
	// unchanged since read — a bump mid-derive survives the clear.
	DirtiedAt time.Time
}

// DeriveQueueStats is a point-in-time summary of the dirty-session
// queue, feeding the worker's depth/lag gauges and readiness probe.
type DeriveQueueStats struct {
	// Depth is the number of queued (dirty) sessions.
	Depth int64

	// OldestDirtiedAt is the oldest dirty mark still queued — "derive
	// lag" is now minus this. Zero when the queue is empty.
	OldestDirtiedAt time.Time
}

// DeriveQueue is an optional capability for a Driver: the dirty-session
// queue feeding the derive worker. Marking is at-least-once and
// idempotent (an upsert that bumps DirtiedAt); deriving is idempotent
// (re-run prunes 0) — together they make a lost clear or duplicate
// mark cost only a redundant derive, never lost data.
//
// Only drivers that host the raw layer implement this (Postgres does;
// in-memory intentionally does not). Callers MUST type-assert.
type DeriveQueue interface {
	// MarkDeriveDirty queues (or re-bumps) one harness session.
	MarkDeriveDirty(ctx context.Context, orgID, harnessID, harnessSessionID string) error

	// ListDeriveDirty returns sessions whose dirty mark has settled
	// (DirtiedAt <= dirtiedBefore), oldest first, capped at limit.
	ListDeriveDirty(ctx context.Context, dirtiedBefore, firstDirtiedBefore time.Time, limit int32) ([]DeriveQueueEntry, error)

	// GetDeriveDirty re-reads one queue entry. Returns nil (no error)
	// when the session is clean.
	GetDeriveDirty(ctx context.Context, orgID, harnessID, harnessSessionID string) (*DeriveQueueEntry, error)

	// ClearDeriveDirty removes the entry only if its DirtiedAt is
	// unchanged from e.DirtiedAt. Returns false when the session was
	// re-dirtied (or already cleared) in the meantime.
	ClearDeriveDirty(ctx context.Context, e DeriveQueueEntry) (bool, error)

	// SweepDeriveDirty enqueues every harness session with raw-layer
	// activity at or after activeSince (the worker's slow backstop for
	// lost marks, bounded so a restart doesn't stampede the queue with
	// all of history). The zero time sweeps every session. Sessions
	// already queued keep their DirtiedAt. Returns how many sessions
	// were newly enqueued.
	SweepDeriveDirty(ctx context.Context, activeSince time.Time) (int64, error)

	// DeriveQueueStats reports queue depth and the oldest dirty mark.
	// Cheap (one aggregate over the small dirty-queue table); the
	// worker calls it every poll and the readiness probe leans on it
	// as the "store reachable, queue pollable" check.
	DeriveQueueStats(ctx context.Context) (DeriveQueueStats, error)
}

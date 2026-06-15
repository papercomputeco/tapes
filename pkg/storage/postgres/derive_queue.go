package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Compile-time guarantee that the Postgres driver hosts the dirty-
// session queue capability — same rationale as the RawTurnStore
// assertion: callers type-assert at runtime, so a signature drift
// would silently disable the derive worker rather than fail the build.
var _ storage.DeriveQueue = (*Driver)(nil)

// MarkDeriveDirty implements storage.DeriveQueue.
func (d *Driver) MarkDeriveDirty(ctx context.Context, orgID, harnessID, harnessSessionID string) error {
	if d == nil || d.conn == nil {
		return errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgID)
	if err != nil {
		return fmt.Errorf("decode org_id: %w", err)
	}
	if err := d.q.MarkDeriveDirty(ctx, gensqlc.MarkDeriveDirtyParams{
		OrgID:            org,
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
	}); err != nil {
		return fmt.Errorf("mark derive dirty: %w", err)
	}
	return nil
}

// ListDeriveDirty implements storage.DeriveQueue.
func (d *Driver) ListDeriveDirty(ctx context.Context, dirtiedBefore, firstDirtiedBefore time.Time, limit int32) ([]storage.DeriveQueueEntry, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	rows, err := d.q.ListDeriveDirty(ctx, gensqlc.ListDeriveDirtyParams{
		DirtiedBefore:      pgtype.Timestamptz{Time: dirtiedBefore, Valid: true},
		FirstDirtiedBefore: pgtype.Timestamptz{Time: firstDirtiedBefore, Valid: true},
		PageSize:           limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list derive dirty: %w", err)
	}
	out := make([]storage.DeriveQueueEntry, 0, len(rows))
	for _, row := range rows {
		out = append(out, deriveQueueEntryFromRow(row))
	}
	return out, nil
}

// GetDeriveDirty implements storage.DeriveQueue.
func (d *Driver) GetDeriveDirty(ctx context.Context, orgID, harnessID, harnessSessionID string) (*storage.DeriveQueueEntry, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("decode org_id: %w", err)
	}
	row, err := d.q.GetDeriveDirty(ctx, gensqlc.GetDeriveDirtyParams{
		OrgID:            org,
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get derive dirty: %w", err)
	}
	entry := deriveQueueEntryFromRow(row)
	return &entry, nil
}

// ClearDeriveDirty implements storage.DeriveQueue. The DELETE is
// guarded on dirtied_at equality so a raw turn landing mid-derive
// (which bumps dirtied_at) keeps the session queued.
func (d *Driver) ClearDeriveDirty(ctx context.Context, e storage.DeriveQueueEntry) (bool, error) {
	if d == nil || d.conn == nil {
		return false, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(e.OrgID)
	if err != nil {
		return false, fmt.Errorf("decode org_id: %w", err)
	}
	rows, err := d.q.ClearDeriveDirty(ctx, gensqlc.ClearDeriveDirtyParams{
		OrgID:            org,
		HarnessID:        e.HarnessID,
		HarnessSessionID: e.HarnessSessionID,
		DirtiedAt:        pgtype.Timestamptz{Time: e.DirtiedAt, Valid: true},
	})
	if err != nil {
		return false, fmt.Errorf("clear derive dirty: %w", err)
	}
	return rows > 0, nil
}

// DeriveQueueStats implements storage.DeriveQueue.
func (d *Driver) DeriveQueueStats(ctx context.Context) (storage.DeriveQueueStats, error) {
	if d == nil || d.conn == nil {
		return storage.DeriveQueueStats{}, errors.New("postgres driver not open")
	}
	row, err := d.q.DeriveQueueStats(ctx)
	if err != nil {
		return storage.DeriveQueueStats{}, fmt.Errorf("derive queue stats: %w", err)
	}
	stats := storage.DeriveQueueStats{Depth: row.Depth}
	if row.OldestDirtiedAt.Valid {
		stats.OldestDirtiedAt = row.OldestDirtiedAt.Time
	}
	return stats, nil
}

// SweepDeriveDirty implements storage.DeriveQueue.
func (d *Driver) SweepDeriveDirty(ctx context.Context, activeSince time.Time) (int64, error) {
	if d == nil || d.conn == nil {
		return 0, errors.New("postgres driver not open")
	}
	enqueued, err := d.q.SweepDeriveDirty(ctx, pgtype.Timestamptz{Time: activeSince, Valid: true})
	if err != nil {
		return 0, fmt.Errorf("sweep derive dirty: %w", err)
	}
	return enqueued, nil
}

// TryDeriveSessionLock takes a session-scoped Postgres advisory lock so
// concurrent workers never double-derive one session. The lock is
// connection-scoped: the pooled connection is pinned for the lock's
// lifetime and returned by release. A false return with nil error means
// another holder has the session — skip it, this is not a failure.
//
// The key hashes the harness triple to 64 bits (FNV-1a). A cross-triple
// collision only serializes two unrelated sessions' derives — safe,
// merely slower — so probabilistic keying is fine here.
func (d *Driver) TryDeriveSessionLock(ctx context.Context, orgID, harnessID, harnessSessionID string) (release func(), acquired bool, err error) {
	if d == nil || d.conn == nil {
		return nil, false, errors.New("postgres driver not open")
	}

	key := deriveLockKey(orgID, harnessID, harnessSessionID)
	conn, err := d.conn.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire conn for advisory lock: %w", err)
	}

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("pg_try_advisory_lock: %w", err)
	}
	if !locked {
		conn.Release()
		return nil, false, nil
	}

	release = func() { //nolint:contextcheck // deliberate fresh context, see comment below
		// Unlock on a fresh background context: the caller's ctx may
		// already be canceled, and releasing the pooled connection
		// without unlocking would leak the lock for the connection's
		// lifetime.
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", key)
		conn.Release()
	}
	return release, true, nil
}

// deriveLockKey folds the harness triple into the 64-bit advisory-lock
// keyspace. NUL separators keep ("a","bc") and ("ab","c") distinct.
func deriveLockKey(orgID, harnessID, harnessSessionID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(orgID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(harnessID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(harnessSessionID))
	return int64(h.Sum64()) //nolint:gosec // deliberate wraparound into the signed advisory keyspace
}

// deriveQueueEntryFromRow maps one queue row to its storage entry. Both
// the list and re-read selects now return the full derive_queue row, so
// a single concrete mapping serves both.
func deriveQueueEntryFromRow(r gensqlc.DeriveQueue) storage.DeriveQueueEntry {
	return storage.DeriveQueueEntry{
		OrgID:            uuidString(r.OrgID),
		HarnessID:        r.HarnessID,
		HarnessSessionID: r.HarnessSessionID,
		DirtiedAt:        r.DirtiedAt.Time,
		FirstDirtiedAt:   r.FirstDirtiedAt.Time,
	}
}

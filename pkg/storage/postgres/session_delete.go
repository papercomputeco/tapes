package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// deleteSessionLockAttempts bounds how often DeleteSession re-reads a subtree
// that changed while it waited for its locks. A change needs a new subagent
// session to land under the victim inside that wait, so a second attempt
// essentially always settles it.
const deleteSessionLockAttempts = 5

// deleteSessionAfterRawTurns, when set, runs inside the delete transaction
// right after the subtree's raw turns are removed. Test seam only.
var deleteSessionAfterRawTurns func()

// DeleteSession removes a session, every subagent session beneath it, and the
// captured data behind all of them, and reports whether the session existed.
// A malformed id is a no-op delete, matching DeleteSkill.
//
// The derived rows go through the session_id ON DELETE CASCADE foreign keys.
// raw_turns has no foreign key to sessions — it is keyed by the harness
// session — so the delete removes each subtree session's raw turns by their
// effective attribution, with the attribution corrections recorded against
// them and the sessions' derive_queue marks. Everything runs in one
// transaction: a failure leaves the session and its capture whole.
//
// Before touching anything the transaction takes, for every session in the
// subtree:
//
//   - the per-session derive lock, as a transaction-scoped advisory lock in
//     the same order attribution repair takes it. That serializes the delete
//     behind an in-flight derive or repair, and keeps a derive that read the
//     raw turns before the delete from writing them into a session row a
//     later capture recreates.
//   - the per-session capture lock, exclusively. PutRawTurn holds it shared
//     for its own transaction, so a capture either commits before the delete
//     reads the raw turns (and is deleted with them) or waits for the delete
//     to commit (and is new capture).
//   - the session row, FOR UPDATE. A child session's foreign-key check needs
//     FOR KEY SHARE on its parent row, so no subagent session can attach to
//     the subtree until the delete commits, and the cascade removes exactly
//     the sessions whose capture the delete removed.
//
// The session cannot come back from a re-derive: the deriver never creates a
// session row, and once the raw turns are gone there is nothing left to
// project. A turn captured for the same harness session after the delete
// commits is new capture: ingest creates a new session row with a new id, and
// it holds only what was captured after the delete.
func (d *Driver) DeleteSession(ctx context.Context, orgID, id string) (bool, error) {
	if d == nil || d.conn == nil {
		return false, errors.New("postgres driver not open")
	}
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return false, fmt.Errorf("delete session: %w", err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return false, nil //nolint:nilerr // invalid id == nothing to delete
	}
	sessionID := pgtype.UUID{Bytes: parsed, Valid: true}

	for range deleteSessionLockAttempts {
		deleted, settled, err := d.deleteSessionOnce(ctx, oid, sessionID)
		if err != nil {
			return false, fmt.Errorf("delete session: %w", err)
		}
		if settled {
			return deleted, nil
		}
	}
	return false, fmt.Errorf("delete session: subtree kept changing across %d attempts", deleteSessionLockAttempts)
}

// sessionKey is a session's natural key within an org.
type sessionKey struct {
	harnessID        string
	harnessSessionID string
}

// deleteSessionOnce runs one delete transaction. settled is false when a
// subagent session joined the subtree while the transaction waited for its
// locks; the caller retries so the newcomer is locked in order rather than
// appended out of order.
func (d *Driver) deleteSessionOnce(ctx context.Context, orgID, sessionID pgtype.UUID) (deleted, settled bool, err error) {
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return false, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success
	qtx := d.q.WithTx(tx)

	rows, err := qtx.ListSessionSubtreeKeys(ctx, gensqlc.ListSessionSubtreeKeysParams{OrgID: orgID, ID: sessionID})
	if err != nil {
		return false, false, fmt.Errorf("list session subtree: %w", err)
	}
	if len(rows) == 0 {
		return false, true, nil
	}
	keys := make([]sessionKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, sessionKey{harnessID: row.HarnessID, harnessSessionID: row.HarnessSessionID})
	}

	// Lock in acquireRepairSessionLocks' order, compared in Go rather than by
	// the database collation, so a delete and a repair never wait on each
	// other in a cycle. Every derive lock is taken before any capture lock, so
	// captures are held off only for the delete itself, never behind a derive.
	slices.SortFunc(keys, func(a, b sessionKey) int {
		return strings.Compare(a.harnessID+"\x00"+a.harnessSessionID, b.harnessID+"\x00"+b.harnessSessionID)
	})
	org := uuidString(orgID)
	for _, key := range keys {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
			deriveLockKey(org, key.harnessID, key.harnessSessionID)); err != nil {
			return false, false, fmt.Errorf("lock session %s/%s: %w", key.harnessID, key.harnessSessionID, err)
		}
	}
	for _, key := range keys {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
			captureLockKey(orgID, key.harnessID, key.harnessSessionID)); err != nil {
			return false, false, fmt.Errorf("lock capture of %s/%s: %w", key.harnessID, key.harnessSessionID, err)
		}
	}

	lockedRows, err := qtx.LockSessionSubtree(ctx, gensqlc.LockSessionSubtreeParams{OrgID: orgID, ID: sessionID})
	if err != nil {
		return false, false, fmt.Errorf("lock session subtree: %w", err)
	}
	rowLocked := make(map[sessionKey]struct{}, len(lockedRows))
	for _, row := range lockedRows {
		rowLocked[sessionKey{harnessID: row.HarnessID, harnessSessionID: row.HarnessSessionID}] = struct{}{}
	}

	// Re-read with a fresh statement snapshot: a child whose ingest committed
	// while the row locks waited is visible here even though the locking
	// statement's snapshot predates it.
	current, err := qtx.ListSessionSubtreeKeys(ctx, gensqlc.ListSessionSubtreeKeysParams{OrgID: orgID, ID: sessionID})
	if err != nil {
		return false, false, fmt.Errorf("re-read session subtree: %w", err)
	}
	if len(current) == 0 {
		// A concurrent delete of this session or an ancestor won the race.
		return false, true, nil
	}
	harnessIDs := make([]string, 0, len(current))
	harnessSessionIDs := make([]string, 0, len(current))
	for _, row := range current {
		key := sessionKey{harnessID: row.HarnessID, harnessSessionID: row.HarnessSessionID}
		if _, ok := rowLocked[key]; !ok || !slices.Contains(keys, key) {
			return false, false, nil
		}
		harnessIDs = append(harnessIDs, key.harnessID)
		harnessSessionIDs = append(harnessSessionIDs, key.harnessSessionID)
	}

	rawTurns, err := qtx.DeleteSessionsRawTurns(ctx, gensqlc.DeleteSessionsRawTurnsParams{
		OrgID:             orgID,
		HarnessIds:        harnessIDs,
		HarnessSessionIds: harnessSessionIDs,
	})
	if err != nil {
		return false, false, fmt.Errorf("delete raw turns: %w", err)
	}
	if deleteSessionAfterRawTurns != nil {
		deleteSessionAfterRawTurns()
	}
	if err := qtx.DeleteDeriveDirtyForSessions(ctx, gensqlc.DeleteDeriveDirtyForSessionsParams{
		OrgID:             orgID,
		HarnessIds:        harnessIDs,
		HarnessSessionIds: harnessSessionIDs,
	}); err != nil {
		return false, false, fmt.Errorf("clear derive marks: %w", err)
	}

	n, err := qtx.DeleteSession(ctx, gensqlc.DeleteSessionParams{OrgID: orgID, ID: sessionID})
	if err != nil {
		return false, false, fmt.Errorf("delete session row: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, false, fmt.Errorf("commit: %w", err)
	}
	d.logger.Info("session deleted",
		"session_id", uuidString(sessionID),
		"sessions", len(current),
		"raw_turns", rawTurns)
	return n > 0, true, nil
}

// captureLockKey is the advisory-lock key that orders a capture of one harness
// session against a delete of it. It lives in its own namespace, apart from
// deriveLockKey, so a capture never waits behind a derive.
func captureLockKey(orgID pgtype.UUID, harnessID, harnessSessionID string) int64 {
	return deriveLockKeyRaw("capture\x00"+uuidString(orgID), harnessID, harnessSessionID)
}

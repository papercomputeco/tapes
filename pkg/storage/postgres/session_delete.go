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
// that changed while it waited for the derive locks. A change needs a new
// subagent session to land under the victim inside that wait, so a second
// attempt essentially always settles it.
const deleteSessionLockAttempts = 5

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
// The transaction first takes the per-session derive locks of the whole
// subtree, as transaction-scoped advisory locks in the same order attribution
// repair takes them. That serializes the delete behind an in-flight derive or
// repair of any of these sessions, and keeps a derive that read the raw turns
// before the delete from writing them into a session row a later capture
// recreates.
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

// deleteSessionOnce runs one delete transaction. settled is false when a
// subagent session joined the subtree while the transaction waited for the
// derive locks; the caller retries so the newcomer is locked in order rather
// than appended out of order.
func (d *Driver) deleteSessionOnce(ctx context.Context, orgID, sessionID pgtype.UUID) (deleted, settled bool, err error) {
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return false, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success
	qtx := d.q.WithTx(tx)

	keys, err := qtx.ListSessionSubtreeKeys(ctx, gensqlc.ListSessionSubtreeKeysParams{OrgID: orgID, ID: sessionID})
	if err != nil {
		return false, false, fmt.Errorf("list session subtree: %w", err)
	}
	if len(keys) == 0 {
		return false, true, nil
	}

	// Lock in acquireRepairSessionLocks' order, compared in Go rather than by
	// the database collation, so a delete and a repair never wait on each
	// other in a cycle.
	slices.SortFunc(keys, func(a, b gensqlc.ListSessionSubtreeKeysRow) int {
		return strings.Compare(a.HarnessID+"\x00"+a.HarnessSessionID, b.HarnessID+"\x00"+b.HarnessSessionID)
	})
	org := uuidString(orgID)
	for _, key := range keys {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
			deriveLockKey(org, key.HarnessID, key.HarnessSessionID)); err != nil {
			return false, false, fmt.Errorf("lock session %s/%s: %w", key.HarnessID, key.HarnessSessionID, err)
		}
	}

	locked, err := qtx.ListSessionSubtreeKeys(ctx, gensqlc.ListSessionSubtreeKeysParams{OrgID: orgID, ID: sessionID})
	if err != nil {
		return false, false, fmt.Errorf("re-read session subtree: %w", err)
	}
	if len(locked) == 0 {
		// A concurrent delete of this session or an ancestor won the race.
		return false, true, nil
	}
	for _, key := range locked {
		if !slices.Contains(keys, key) {
			return false, false, nil
		}
	}

	var rawTurns int64
	for _, key := range locked {
		n, err := qtx.DeleteSessionRawTurns(ctx, gensqlc.DeleteSessionRawTurnsParams{
			OrgID:            orgID,
			HarnessID:        key.HarnessID,
			HarnessSessionID: key.HarnessSessionID,
		})
		if err != nil {
			return false, false, fmt.Errorf("delete raw turns of %s/%s: %w", key.HarnessID, key.HarnessSessionID, err)
		}
		rawTurns += n
		if err := qtx.DeleteDeriveDirtyForSession(ctx, gensqlc.DeleteDeriveDirtyForSessionParams{
			OrgID:            orgID,
			HarnessID:        key.HarnessID,
			HarnessSessionID: key.HarnessSessionID,
		}); err != nil {
			return false, false, fmt.Errorf("clear derive mark of %s/%s: %w", key.HarnessID, key.HarnessSessionID, err)
		}
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
		"sessions", len(locked),
		"raw_turns", rawTurns)
	return n > 0, true, nil
}

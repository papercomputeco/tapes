package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Compile-time guarantee, for the same reason as the SessionIngester one:
// callers type-assert, so a signature drift would silently turn idle
// splitting off rather than fail the build.
var _ storage.SyntheticSessionAssigner = (*Driver)(nil)

// AssignSyntheticSession implements storage.SyntheticSessionAssigner.
//
// One transaction, serialized per conversation by an advisory lock, so two
// turns of the same conversation arriving together on different replicas
// cannot both open a split:
//
//  1. A turn already stored under req.RequestID keeps its session.
//  2. The conversation's splits are read. A conversation with none but a
//     session keyed by the bare conversation key (captured before splits
//     were tracked) is seeded with that session's range, so it splits at
//     its next idle gap instead of growing forever.
//  3. sessions.AssignSyntheticSplit places the turn and the chosen split's
//     range is widened to cover it.
func (d *Driver) AssignSyntheticSession(ctx context.Context, req storage.SyntheticSessionRequest) (string, error) {
	if d == nil || d.conn == nil {
		return "", errors.New("postgres driver not open")
	}
	if req.Conversation == "" {
		return "", errors.New("assign synthetic session: missing conversation key")
	}
	orgID, err := orgIDFromString(req.OrgID)
	if err != nil {
		return "", fmt.Errorf("decode org_id: %w", err)
	}
	at := req.CapturedAt
	if at.IsZero() {
		at = time.Now()
	}
	// Postgres keeps microseconds; compare at the precision that is stored
	// so a retry of the same instant lands inside the range it widened.
	at = at.UTC().Truncate(time.Microsecond)

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success
	qtx := d.q.WithTx(tx)

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
		deriveLockKeyRaw("synthetic-split\x00"+uuidString(orgID), req.HarnessID, req.Conversation)); err != nil {
		return "", fmt.Errorf("lock conversation: %w", err)
	}

	if req.RequestID != "" {
		stored, err := qtx.GetRawTurnSessionKeyByRequestID(ctx, gensqlc.GetRawTurnSessionKeyByRequestIDParams{
			OrgID: orgID, RequestID: req.RequestID,
		})
		switch {
		case err == nil:
			if stored.HarnessID == req.HarnessID && stored.HarnessSessionID != "" {
				return stored.HarnessSessionID, nil
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return "", fmt.Errorf("lookup stored turn: %w", err)
		}
	}

	rows, err := qtx.ListSyntheticSessionSplits(ctx, gensqlc.ListSyntheticSessionSplitsParams{
		OrgID: orgID, HarnessID: req.HarnessID, ConversationKey: req.Conversation,
	})
	if err != nil {
		return "", fmt.Errorf("list splits: %w", err)
	}
	splits := make([]sessions.SyntheticSplit, 0, len(rows)+1)
	for _, row := range rows {
		splits = append(splits, sessions.SyntheticSplit{
			HarnessSessionID: row.HarnessSessionID,
			FirstCapturedAt:  row.FirstCapturedAt.Time,
			LastCapturedAt:   row.LastCapturedAt.Time,
		})
	}
	if len(splits) == 0 {
		seed, err := qtx.GetSyntheticSessionSeed(ctx, gensqlc.GetSyntheticSessionSeedParams{
			OrgID: orgID, HarnessID: req.HarnessID, ConversationKey: req.Conversation,
		})
		switch {
		case err == nil:
			splits = append(splits, sessions.SyntheticSplit{
				HarnessSessionID: req.Conversation,
				FirstCapturedAt:  seed.StartedAt.Time.UTC().Truncate(time.Microsecond),
				LastCapturedAt:   seed.LastSeenAt.Time.UTC().Truncate(time.Microsecond),
			})
			if err := d.upsertSyntheticSplit(ctx, qtx, orgID, req, splits[0]); err != nil {
				return "", err
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return "", fmt.Errorf("lookup pre-split session: %w", err)
		}
	}

	split, _ := sessions.AssignSyntheticSplit(req.Conversation, splits, at, req.IdleGap)
	if err := d.upsertSyntheticSplit(ctx, qtx, orgID, req, split); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit split assignment: %w", err)
	}
	return split.HarnessSessionID, nil
}

func (d *Driver) upsertSyntheticSplit(ctx context.Context, qtx *gensqlc.Queries, orgID pgtype.UUID, req storage.SyntheticSessionRequest, split sessions.SyntheticSplit) error {
	if err := qtx.UpsertSyntheticSessionSplit(ctx, gensqlc.UpsertSyntheticSessionSplitParams{
		OrgID:            orgID,
		HarnessID:        req.HarnessID,
		HarnessSessionID: split.HarnessSessionID,
		ConversationKey:  req.Conversation,
		FirstCapturedAt:  pgtype.Timestamptz{Time: split.FirstCapturedAt, Valid: true},
		LastCapturedAt:   pgtype.Timestamptz{Time: split.LastCapturedAt, Valid: true},
	}); err != nil {
		return fmt.Errorf("record split %s: %w", split.HarnessSessionID, err)
	}
	return nil
}

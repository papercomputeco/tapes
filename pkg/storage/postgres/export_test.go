package postgres

import (
	"context"
	"math/big"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

func ToMigrateDSNForTest(dsn string) string {
	return toMigrateDSN(dsn)
}

// SetRepairRederiveForTest swaps the synchronous rebuild the attribution
// repair flow runs for each affected session, so a test can force a
// mid-flight rederive failure (correction committed, one projection rebuilt,
// the other stale) — a state with no black-box trigger. Returns a restore
// func; the ginkgo specs run sequentially, so swapping the package variable
// is race-free.
func SetRepairRederiveForTest(
	fn func(*Driver, context.Context, string, string, string, string) (*derive.RederiveReport, error),
) (restore func()) {
	prev := repairRederive
	repairRederive = fn
	return func() { repairRederive = prev }
}

// SetRepairSourceCleanupForTest swaps the best-effort deletion of the emptied
// repair source session, so a test can force a cleanup-only failure after the
// correction and both projection rebuilds applied — like the rederive seam, a
// state with no black-box trigger. Returns a restore func; the ginkgo specs
// run sequentially, so swapping the package variable is race-free.
func SetRepairSourceCleanupForTest(
	fn func(d *Driver, ctx context.Context, orgID pgtype.UUID, harnessID, harnessSessionID string) error,
) (restore func()) {
	prev := repairSourceCleanup
	repairSourceCleanup = func(d *Driver, ctx context.Context, orgID pgtype.UUID, key repairSessionKey) error {
		return fn(d, ctx, orgID, key.harnessID, key.harnessSessionID)
	}
	return func() { repairSourceCleanup = prev }
}

// SpanTurnUpsertForTest runs the real UpsertSpanTurn query so a test can
// exercise the change-feed semantics against the query that actually ships,
// rather than a copy of its SQL that could drift from it.
//
// content_hash is computed here exactly as the write path computes it, so a
// caller only supplies the content it wants to vary.
func (d *Driver) SpanTurnUpsertForTest(ctx context.Context, p gensqlc.UpsertSpanTurnParams) error {
	p.ContentHash = spanTurnContentHash(p)
	return d.q.UpsertSpanTurn(ctx, p)
}

// NextDeriveSeqForTest draws a cursor value the way a derive pass does.
func (d *Driver) NextDeriveSeqForTest(ctx context.Context) (int64, error) {
	return d.q.NextDeriveSeq(ctx)
}

// NewSpanTurnParamsForTest builds the minimum viable trace row.
func NewSpanTurnParamsForTest(orgID pgtype.UUID, traceID string, sessionID pgtype.UUID, prompt string, seq int64, fidelity string) gensqlc.UpsertSpanTurnParams {
	return gensqlc.UpsertSpanTurnParams{
		OrgID:      orgID,
		TraceID:    traceID,
		SessionID:  sessionID,
		UserPrompt: prompt,
		Synthetic:  "no",
		Status:     "ok",
		StartedAt:  pgtype.Timestamptz{Valid: true},
		// total_cost_usd is NOT NULL; the real writer always supplies a valid
		// Numeric via numericFromFloat, so the fixture must too.
		TotalCostUsd: pgtype.Numeric{Int: big.NewInt(0), Valid: true},
		DeriveSeq:    seq,
		Fidelity:     fidelity,
	}
}

// Exported for the change-feed unit tests.
var (
	RollupFidelityForTest      = rollupFidelity
	SpanFidelityForTest        = spanFidelity
	SpanContentHashForTest     = spanContentHash
	SpanTurnContentHashForTest = spanTurnContentHash
)

// ListChangedSpanTurnsForTest runs the real change-feed read so a test
// exercises the bound that ships, not a copy of it that could drift.
func (d *Driver) ListChangedSpanTurnsForTest(ctx context.Context, orgID pgtype.UUID, afterCursor int64, pageSize int32) ([]gensqlc.ListChangedSpanTurnsRow, error) {
	return d.q.ListChangedSpanTurns(ctx, gensqlc.ListChangedSpanTurnsParams{
		OrgID:       orgID,
		AfterCursor: afterCursor,
		PageSize:    pageSize,
	})
}

// RawTurnsForDeriveForTest reads rows back exactly the way the derive path
// does — the real GetRawTurn, the real column mapping, the real recovery step —
// so a test asserts on what the deriver would be handed rather than on a
// reimplementation of it.
func (d *Driver) RawTurnsForDeriveForTest(ctx context.Context, orgID pgtype.UUID) ([]storage.RawTurnRecord, error) {
	ids, err := d.conn.Query(ctx,
		`SELECT id FROM raw_turns WHERE org_id = $1 ORDER BY id`, orgID)
	if err != nil {
		return nil, err
	}
	defer ids.Close()

	var out []int64
	for ids.Next() {
		var id int64
		if err := ids.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	if err := ids.Err(); err != nil {
		return nil, err
	}

	recs := make([]storage.RawTurnRecord, 0, len(out))
	for _, id := range out {
		row, err := d.q.GetRawTurn(ctx, id)
		if err != nil {
			return nil, err
		}
		rec := rawTurnRecordFromEffectiveRow(row)
		recoverReduction(ctx, d.reducers, d.logger, &rec)
		recs = append(recs, rec)
	}
	return recs, nil
}

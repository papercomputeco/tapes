package postgres

import (
	"context"
	"math/big"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

func ToMigrateDSNForTest(dsn string) string {
	return toMigrateDSN(dsn)
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

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// ProbePublishedView verifies that this driver's own role can read the
// published view a filter claim names: one WHERE FALSE round trip that fails
// when the view is missing, when SELECT was never granted, or when the
// contract columns or the claim-declared value column are absent. WHERE
// FALSE keeps the probe planning-only — no rows are read, so its cost does
// not scale with the view's contents.
func (d *Driver) ProbePublishedView(
	ctx context.Context,
	view storage.PublishedViewName,
	column storage.PublishedColumnName,
) error {
	if d == nil || d.conn == nil {
		return errors.New("postgres driver is not open")
	}
	if view.IsZero() || column.IsZero() {
		return errors.New("published view probe requires a parsed view and column")
	}

	// Both identifiers were built by their validating parsers and render
	// through their single Quoted helpers — the same discipline the filter
	// rendering itself follows, so the probe and the probed query cannot
	// disagree about quoting.
	query := fmt.Sprintf("SELECT primitive_type, primitive_id, %s FROM %s WHERE FALSE",
		column.Quoted(), view.Quoted())
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return classifyProbeError(fmt.Errorf("probe published view %s: %w", view.String(), err))
	}
	defer rows.Close()
	for rows.Next() { // drain: WHERE FALSE yields no rows
	}
	// pgx pipelines: most execution errors (undefined table, insufficient
	// privilege) surface here rather than from Query itself.
	if err := rows.Err(); err != nil {
		return classifyProbeError(fmt.Errorf("probe published view %s: %w", view.String(), err))
	}

	return nil
}

// classifyProbeError separates the store's own verdict from a failure to
// ask it. Postgres answered exactly when a PgError is in the chain — any
// SQLSTATE: undefined table, insufficient privilege, undefined column all
// travel that way. Everything else (a context deadline, a dial error, a
// closed pool) never carried a verdict about the view and is marked
// transient so callers retain their prior arming state instead of treating
// a blip as a refusal.
func classifyProbeError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return err
	}

	return &storage.TransientProbeError{Err: err}
}

// compile-time proof the driver carries the probe capability.
var _ storage.PublishedViewProber = (*Driver)(nil)

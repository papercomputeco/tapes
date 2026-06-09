package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Compile-time guarantee the Postgres driver offers the status-backfill
// capability so api can type-assert it without depending on this package.
var _ storage.SessionStatusBackfiller = (*Driver)(nil)

// BackfillSessionStatus recomputes derived_status (plus the sticky
// has_git_activity flag and tool_result_count / tool_error_count) for sessions
// still at the default 'unknown' status — the rows that predate the
// ingest-time computation. It walks each session's nodes with the same signal
// helpers ingest uses (sessions.CountToolResults / CountToolResultErrors /
// BlocksHaveGitActivity over every node, sessions.DetermineStatus over the
// chronologically-last node as the leaf). Live ingest keeps status current on
// its own.
//
// Scoping to 'unknown' keeps re-runs cheap and idempotent — already-classified
// rows are skipped. Re-classifying already-decided rows after a classifier
// change is intentionally out of scope for this endpoint.
//
// Safe to run online: a concurrent live turn re-runs the same
// UpdateSessionStatus path, so the worst case is a redundant equal write.
// Each session's recompute is its own statement; there is no global lock.
func (d *Driver) BackfillSessionStatus(ctx context.Context) (storage.BackfillSessionStatusResult, error) {
	if d == nil || d.conn == nil {
		return storage.BackfillSessionStatusResult{}, errors.New("postgres driver not open")
	}

	rows, err := d.conn.Query(ctx, `SELECT id FROM sessions WHERE derived_status = 'unknown'`)
	if err != nil {
		return storage.BackfillSessionStatusResult{}, fmt.Errorf("list sessions: %w", err)
	}
	var ids []pgtype.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return storage.BackfillSessionStatusResult{}, fmt.Errorf("scan session id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return storage.BackfillSessionStatusResult{}, fmt.Errorf("iterate sessions: %w", err)
	}

	result := storage.BackfillSessionStatusResult{}
	for _, id := range ids {
		result.Scanned++

		nodeRows, err := d.q.ListNodesBySession(ctx, id)
		if err != nil {
			return result, fmt.Errorf("load session %s nodes: %w", uuidString(id), err)
		}
		if len(nodeRows) == 0 {
			// No nodes attributed yet (e.g. a fork-parent placeholder):
			// nothing to classify, leave the default 'unknown'.
			continue
		}
		nodes, err := merkleNodesFromRows(nodeRows)
		if err != nil {
			return result, fmt.Errorf("rebuild session %s nodes: %w", uuidString(id), err)
		}

		// ListNodesBySession returns every node once, so unlike the ingest
		// path (which sees the full chain re-sent each turn) we can count
		// straight over the set.
		hasGitActivity := false
		toolResultCount, toolErrorCount := 0, 0
		for _, n := range nodes {
			if n == nil {
				continue
			}
			if !hasGitActivity && sessions.BlocksHaveGitActivity(n.Bucket.Content) {
				hasGitActivity = true
			}
			toolResultCount += sessions.CountToolResults(n.Bucket.Content)
			toolErrorCount += sessions.CountToolResultErrors(n.Bucket.Content)
		}
		// ListNodesBySession orders by created_at ASC, so the last node is
		// the most recently captured — the same "latest leaf" the per-turn
		// ingest path classifies on.
		leaf := nodes[len(nodes)-1]
		status := sessions.DetermineStatus(leaf, hasGitActivity, toolResultCount, toolErrorCount)

		if err := d.q.UpdateSessionStatus(ctx, gensqlc.UpdateSessionStatusParams{
			HasGitActivity:  hasGitActivity,
			ToolResultCount: int32Count(toolResultCount),
			ToolErrorCount:  int32Count(toolErrorCount),
			DerivedStatus:   status,
			ID:              id,
		}); err != nil {
			return result, fmt.Errorf("update session %s status: %w", uuidString(id), err)
		}
		result.Updated++
	}

	return result, nil
}

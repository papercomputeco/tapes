package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// rederivePageSize bounds the raw-turn scan batches.
const rederivePageSize = 200

// rawTurnIndexEntry is the lightweight ordering record for one raw row.
type rawTurnIndexEntry struct {
	id         int64
	capturedAt time.Time
}

// RederiveFromRaw rebuilds the derived node layer from the immutable
// raw-turn store: every raw turn is re-parsed, re-classified, and
// re-chained with the CURRENT projection, then written back in one
// transaction per org — upserting nodes (refreshing derived columns on
// rows that already exist) and pruning rows in covered sessions whose
// hashes the current derivation no longer produces (e.g. chains built
// under a superseded projection).
//
// Memory discipline: the pass first scans a payload-free index (id +
// capture time), sorts per org chronologically, then streams full rows
// ONE AT A TIME through the deriver. Each turn's chain re-contains the
// whole conversation history, so holding all turns at once is O(N²) in
// content and OOMs a modestly-sized container; the deriver retains
// only the deduplicated (unique-content) node set.
//
// The pass is idempotent: re-running against an unchanged raw layer
// upserts the same set and prunes nothing. Raw rows are never written.
//
// CONCURRENCY (PCC-687 G, partial): unlike RederiveSessionLocked, this
// whole-org pass does NOT hold the per-session derive lock, so running it
// while the derive worker is live can still race — this pass reads the
// whole org's raw UP FRONT, and a turn the worker writes after that read
// but before this pass's prune is deleted as "not in my set". A write-only
// lock does not fix this: the stale READ is the defect, so a correct fix
// must read-and-write each session under its lock — i.e. reimplement this
// as a loop of RederiveSessionLocked calls, trading the org-wide single
// deriver (and its cross-session dedup + one-tx-per-org atomicity) for the
// worker's per-session model. That trade is a design decision left for a
// human; the session-scoped race (seed, ad-hoc re-derive) is closed by
// RederiveSessionLocked in the meantime.
func (d *Driver) RederiveFromRaw(ctx context.Context, project string) (map[string]*derive.RederiveReport, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}

	// Index scan: identity + timing only. Wire rows feed the chain
	// deriver in capture order; transcript rows are routed to the
	// reconciler, keeping only the LATEST version per (session, agent)
	// — transcript ingest appends a new row each time a file grows.
	byOrg := map[string][]rawTurnIndexEntry{}
	transcriptRows := map[string]map[string]int64{} // org → fileKey → latest raw id
	var afterID int64
	for {
		page, err := d.q.ListRawTurnIndex(ctx, gensqlc.ListRawTurnIndexParams{
			AfterID:  afterID,
			PageSize: rederivePageSize,
		})
		if err != nil {
			return nil, fmt.Errorf("list raw turn index: %w", err)
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			afterID = row.ID
			org := uuidString(row.OrgID)
			if row.Source == storage.RawTurnSourceTranscript {
				if transcriptRows[org] == nil {
					transcriptRows[org] = map[string]int64{}
				}
				fileKey := row.HarnessSessionID + "/" + transcriptAgentKey(row.Meta)
				if row.ID > transcriptRows[org][fileKey] {
					transcriptRows[org][fileKey] = row.ID
				}
				continue
			}
			rec := storage.RawTurnRecord{ID: row.ID, Meta: row.Meta, ReceivedAt: row.ReceivedAt.Time}
			byOrg[org] = append(byOrg[org], rawTurnIndexEntry{
				id:         row.ID,
				capturedAt: derive.CapturedAt(&rec),
			})
		}
	}

	reports := make(map[string]*derive.RederiveReport, len(byOrg))
	for orgKey, index := range byOrg {
		sort.SliceStable(index, func(i, j int) bool { return index[i].capturedAt.Before(index[j].capturedAt) })

		dv, err := derive.NewDeriver(project)
		if err != nil {
			return nil, fmt.Errorf("create deriver: %w", err)
		}
		for _, entry := range index {
			row, err := d.q.GetRawTurn(ctx, entry.id)
			if err != nil {
				return nil, fmt.Errorf("fetch raw turn %d: %w", entry.id, err)
			}
			rec := rawTurnRecordFromRow(row)
			dv.AddTurn(&rec)
		}
		set := dv.Finish()

		// Fuse the causal/fork skeleton from any transcript rows. The
		// rows come out of a map, so sort by raw id first: on no-thread-id
		// chains the reconciler's overlap tie-break is first-wins, and a
		// nondeterministic file order would flip which parent_tool_use_id
		// is stamped across re-derives.
		transcriptIDs := make([]int64, 0, len(transcriptRows[orgKey]))
		for _, id := range transcriptRows[orgKey] {
			transcriptIDs = append(transcriptIDs, id)
		}
		sort.SliceStable(transcriptIDs, func(i, j int) bool { return transcriptIDs[i] < transcriptIDs[j] })
		var files []*derive.TranscriptFile
		for _, id := range transcriptIDs {
			row, err := d.q.GetRawTurn(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("fetch transcript row %d: %w", id, err)
			}
			rec := rawTurnRecordFromRow(row)
			file, err := derive.ParseTranscriptFile(&rec)
			if err != nil {
				return nil, fmt.Errorf("parse transcript row %d: %w", id, err)
			}
			files = append(files, file)
		}
		set.Report.Reconcile = derive.ReconcileTranscripts(set, files)

		if err := d.writeDerivedSet(ctx, orgKey, set); err != nil {
			return nil, fmt.Errorf("write derived set for org %s: %w", orgKey, err)
		}
		reports[orgDisplayKey(orgKey)] = &set.Report
	}
	return reports, nil
}

// RederiveSession is the session-scoped sibling of RederiveFromRaw:
// re-derive ONE harness session from its raw turns and apply the
// result transactionally (upsert + prune scoped to that session). This
// is the derive worker's unit of work — memory stays bounded by one
// session's unique content, and the full rows stream through the
// deriver one at a time exactly like the full-org pass.
//
// Same idempotence contract: re-running against an unchanged raw layer
// upserts the same set and prunes nothing.
func (d *Driver) RederiveSession(ctx context.Context, project, orgID, harnessID, harnessSessionID string) (*derive.RederiveReport, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgKeyForLookup(orgID))
	if err != nil {
		return nil, fmt.Errorf("decode org_id: %w", err)
	}

	// Index scan: identity + timing only, no payloads. Transcript rows
	// keep only the LATEST version per agent — transcript ingest
	// appends a new row each time a file grows.
	index, err := d.q.ListRawTurnIndexBySession(ctx, gensqlc.ListRawTurnIndexBySessionParams{
		OrgID:            org,
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("list raw turn index for session: %w", err)
	}

	var wire []rawTurnIndexEntry
	transcriptRows := map[string]int64{} // agentKey → latest raw id
	for _, row := range index {
		if row.Source == storage.RawTurnSourceTranscript {
			agentKey := transcriptAgentKey(row.Meta)
			if row.ID > transcriptRows[agentKey] {
				transcriptRows[agentKey] = row.ID
			}
			continue
		}
		rec := storage.RawTurnRecord{ID: row.ID, Meta: row.Meta, ReceivedAt: row.ReceivedAt.Time}
		wire = append(wire, rawTurnIndexEntry{id: row.ID, capturedAt: derive.CapturedAt(&rec)})
	}
	sort.SliceStable(wire, func(i, j int) bool { return wire[i].capturedAt.Before(wire[j].capturedAt) })

	dv, err := derive.NewDeriver(project)
	if err != nil {
		return nil, fmt.Errorf("create deriver: %w", err)
	}
	for _, entry := range wire {
		row, err := d.q.GetRawTurn(ctx, entry.id)
		if err != nil {
			return nil, fmt.Errorf("fetch raw turn %d: %w", entry.id, err)
		}
		rec := rawTurnRecordFromRow(row)
		dv.AddTurn(&rec)
	}
	set := dv.Finish()

	// Fuse the causal/fork skeleton from the session's transcript rows.
	// The rows come out of a map, so sort by raw id first: on no-thread-id
	// chains the reconciler's overlap tie-break is first-wins, and a
	// nondeterministic file order would flip which parent_tool_use_id is
	// stamped across re-derives.
	transcriptIDs := make([]int64, 0, len(transcriptRows))
	for _, id := range transcriptRows {
		transcriptIDs = append(transcriptIDs, id)
	}
	sort.SliceStable(transcriptIDs, func(i, j int) bool { return transcriptIDs[i] < transcriptIDs[j] })
	var files []*derive.TranscriptFile
	for _, id := range transcriptIDs {
		row, err := d.q.GetRawTurn(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("fetch transcript row %d: %w", id, err)
		}
		rec := rawTurnRecordFromRow(row)
		file, err := derive.ParseTranscriptFile(&rec)
		if err != nil {
			return nil, fmt.Errorf("parse transcript row %d: %w", id, err)
		}
		files = append(files, file)
	}
	set.Report.Reconcile = derive.ReconcileTranscripts(set, files)

	if err := d.writeDerivedSet(ctx, uuidString(org), set); err != nil {
		return nil, fmt.Errorf("write derived set for session %s/%s: %w", harnessID, harnessSessionID, err)
	}
	return &set.Report, nil
}

// RederiveSessionLocked is the externally-safe entry point for a
// session-scoped re-derive that may run WHILE the derive worker is live:
// it holds the per-session advisory lock across the whole read-derive-write
// pass, so it cannot interleave with the worker's derive of the same
// session and prune a turn the worker just wrote. The worker's own path
// (RederiveSession) is already called under this lock — it takes it in
// processEntry — so RederiveSession stays lock-free and this wrapper is the
// one non-worker callers use. Blocking: it waits out a concurrent worker
// derive rather than skipping, since a manual re-derive must actually run.
func (d *Driver) RederiveSessionLocked(ctx context.Context, project, orgID, harnessID, harnessSessionID string) (*derive.RederiveReport, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	release, err := d.AcquireDeriveSessionLock(ctx, orgID, harnessID, harnessSessionID)
	if err != nil {
		return nil, fmt.Errorf("acquire derive lock %s/%s: %w", harnessID, harnessSessionID, err)
	}
	defer release()
	return d.RederiveSession(ctx, project, orgID, harnessID, harnessSessionID)
}

func orgDisplayKey(org string) string {
	if org == "" || org == "00000000-0000-0000-0000-000000000000" {
		return "default"
	}
	return org
}

// writeDerivedSet applies one org's derived set transactionally.
func (d *Driver) writeDerivedSet(ctx context.Context, orgKey string, set *derive.DerivedSet) error {
	orgID, err := orgIDFromString(orgKeyForLookup(orgKey))
	if err != nil {
		return fmt.Errorf("decode org_id: %w", err)
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success
	qtx := d.q.WithTx(tx)

	// Resolve covered sessions up front. Unknown keys — raw rows whose
	// session identity row never landed (a transient pre-ingest race;
	// ingest is the sole writer of that row) — are skipped below on
	// ErrNoRows: no session row, no projection.
	sessionIDs := map[derive.SessionKey]pgtype.UUID{}
	var coveredSessions []pgtype.UUID
	for _, key := range set.Sessions {
		id, err := qtx.SessionIDByHarnessKey(ctx, gensqlc.SessionIDByHarnessKeyParams{
			OrgID:            orgID,
			HarnessID:        key.HarnessID,
			HarnessSessionID: key.HarnessSessionID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("resolve session %s/%s: %w", key.HarnessID, key.HarnessSessionID, err)
		}
		sessionIDs[key] = id
		coveredSessions = append(coveredSessions, id)
	}

	// Node persistence is retired: the deriver still builds the merkle
	// DAG in memory (dedup, reconciliation, and the src.New[] delta
	// signal span emit depends on), but no longer writes or prunes the
	// `nodes` table. Spans are emitted from the in-memory nodes below and
	// are the sole derived read surface; session attribution was resolved
	// into sessionIDs/coveredSessions above for the span writer.

	// The span projection rides the same transaction: traces, spans,
	// and links are as derived as the nodes are, and a derive pass
	// either lands both layers or neither.
	if err := writeSpanSet(ctx, qtx, orgID, sessionIDs, coveredSessions, derive.EmitSpans(set)); err != nil {
		return fmt.Errorf("write span set: %w", err)
	}

	// Fold derived_title AFTER writeSpanSet: its FoldSessionRollupsFromSpans
	// pass reset derived_title to NULL for every covered session, so a
	// re-derive that drops a title clears the stale value. Re-writing here
	// overwrites only the sessions that still produce one.
	for key, title := range set.SessionTitles {
		id, ok := sessionIDs[key]
		if !ok {
			continue
		}
		if err := qtx.UpdateSessionDerivedTitle(ctx, gensqlc.UpdateSessionDerivedTitleParams{
			DerivedTitle: nullStringValue(title),
			ID:           id,
		}); err != nil {
			return fmt.Errorf("fold derived title for %s: %w", key.HarnessSessionID, err)
		}
	}

	return tx.Commit(ctx)
}

// transcriptAgentKey extracts the agent id from a transcript row's
// meta for latest-version grouping.
func transcriptAgentKey(meta []byte) string {
	var m struct {
		AgentID string `json:"agent_id"`
	}
	_ = json.Unmarshal(meta, &m)
	if m.AgentID == "" {
		return "main"
	}
	return m.AgentID
}

// orgKeyForLookup maps the record's display org back to the canonical
// lookup string ("" → nil-UUID sentinel handled by orgIDFromString).
func orgKeyForLookup(org string) string {
	if org == "00000000-0000-0000-0000-000000000000" {
		return ""
	}
	return org
}

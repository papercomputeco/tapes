package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// rederiveDiagnosticLimit matches the deriver's bound for sampled failures.
const rederiveDiagnosticLimit = 20

// rawTurnIndexEntry is the lightweight ordering record for one raw row.
type rawTurnIndexEntry struct {
	id         int64
	capturedAt time.Time
}

// RederiveFromRaw rebuilds every persisted session from its effective raw
// turns. Sessions are enumerated from the read model rather than from current
// raw attribution so a repair source that has become empty is still covered
// and pruned.
//
// Each session's full read-derive-write pass runs under the same advisory lock
// used by the derive worker and attribution repair. Holding one lock at a time
// avoids both stale-read races and lock cycles with repair's ordered two-lock
// acquisition. The pass is atomic per session, not per org; reports retain the
// existing per-org shape by aggregating session reports.
func (d *Driver) RederiveFromRaw(ctx context.Context, project string) (map[string]*derive.RederiveReport, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	keys, err := d.q.ListSessionsForRederive(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sessions for rederive: %w", err)
	}
	reports := make(map[string]*derive.RederiveReport)
	for _, key := range keys {
		orgID := uuidString(key.OrgID)
		report, err := d.RederiveSessionLocked(ctx, project, orgID, key.HarnessID, key.HarnessSessionID)
		if err != nil {
			return nil, fmt.Errorf("rederive session %s/%s/%s: %w", orgID, key.HarnessID, key.HarnessSessionID, err)
		}
		displayOrg := orgDisplayKey(orgID)
		if reports[displayOrg] == nil {
			reports[displayOrg] = newRederiveReport()
		}
		mergeRederiveReport(reports[displayOrg], report)
	}
	return reports, nil
}

func newRederiveReport() *derive.RederiveReport {
	return &derive.RederiveReport{CallKinds: map[string]int{}, NodeKinds: map[string]int{}}
}

func mergeRederiveReport(dst, src *derive.RederiveReport) {
	dst.RawTurns += src.RawTurns
	dst.ParsedTurns += src.ParsedTurns
	dst.RawOnlyTurns += src.RawOnlyTurns
	dst.Nodes += src.Nodes
	dst.JudgedActions += src.JudgedActions
	dst.AttachedVerdicts += src.AttachedVerdicts
	dst.WebSummaryAttached += src.WebSummaryAttached
	dst.PlansAttached += src.PlansAttached
	dst.ParseFailures = appendBounded(dst.ParseFailures, src.ParseFailures, rederiveDiagnosticLimit)
	dst.UnattachedActions = appendBounded(dst.UnattachedActions, src.UnattachedActions, rederiveDiagnosticLimit)
	for kind, count := range src.CallKinds {
		dst.CallKinds[kind] += count
	}
	for kind, count := range src.NodeKinds {
		dst.NodeKinds[kind] += count
	}
	if src.Reconcile != nil {
		if dst.Reconcile == nil {
			dst.Reconcile = &derive.ReconcileStats{}
		}
		dst.Reconcile.TranscriptFiles += src.Reconcile.TranscriptFiles
		dst.Reconcile.SubagentForks += src.Reconcile.SubagentForks
		dst.Reconcile.ForkedChains += src.Reconcile.ForkedChains
		dst.Reconcile.MainChainsJoined += src.Reconcile.MainChainsJoined
		dst.Reconcile.ConversationJoined += src.Reconcile.ConversationJoined
		dst.Reconcile.ConversationTotal += src.Reconcile.ConversationTotal
	}
}

func appendBounded(dst, src []string, limit int) []string {
	remaining := limit - len(dst)
	if remaining <= 0 {
		return dst
	}
	if len(src) > remaining {
		src = src[:remaining]
	}
	return append(dst, src...)
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
		rec := rawTurnRecordFromEffectiveRow(row)
		recoverReduction(ctx, d.reducers, d.logger, &rec)
		dv.AddTurn(&rec)
	}
	set := dv.Finish()
	requestedKey := derive.SessionKey{HarnessID: harnessID, HarnessSessionID: harnessSessionID}
	covered := slices.Contains(set.Sessions, requestedKey)
	if !covered {
		set.Sessions = append(set.Sessions, requestedKey)
	}

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
		rec := rawTurnRecordFromEffectiveRow(row)
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

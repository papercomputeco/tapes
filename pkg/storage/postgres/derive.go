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
	"github.com/papercomputeco/tapes/pkg/llm"
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

		// Fuse the causal/fork skeleton from any transcript rows.
		var files []*derive.TranscriptFile
		for _, id := range transcriptRows[orgKey] {
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
	var files []*derive.TranscriptFile
	for _, id := range transcriptRows {
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

	// Resolve covered sessions up front. Unknown keys (raw rows whose
	// session row never landed) derive with NULL attribution.
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

	keepHashes := make([]string, 0, len(set.Nodes))
	for _, dn := range set.Nodes {
		n := dn.Node
		bucketJSON, err := json.Marshal(n.Bucket)
		if err != nil {
			return fmt.Errorf("marshal bucket %s: %w", n.Hash, err)
		}
		contentJSON, err := json.Marshal(n.Bucket.Content)
		if err != nil {
			return fmt.Errorf("marshal content %s: %w", n.Hash, err)
		}
		createdAt := n.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		reqSystem, reqMaxTokens, reqTemperature, reqStream, reqToolCount := requestParamColumns(n.Request)

		sessionID := pgtype.UUID{}
		if id, ok := sessionIDs[dn.Session]; ok {
			sessionID = id
		}

		if _, err := qtx.UpsertDerivedNode(ctx, gensqlc.UpsertDerivedNodeParams{
			OrgID:                    orgID,
			Hash:                     n.Hash,
			Bucket:                   bucketJSON,
			Type:                     nullStringValue(n.Bucket.Type),
			Role:                     nullStringValue(n.Bucket.Role),
			Content:                  contentJSON,
			Model:                    nullStringValue(n.Bucket.Model),
			Provider:                 nullStringValue(n.Bucket.Provider),
			AgentName:                nullStringValue(n.Bucket.AgentName),
			StopReason:               nullStringValue(n.StopReason),
			PromptTokens:             nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.PromptTokens }),
			CompletionTokens:         nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CompletionTokens }),
			TotalTokens:              nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.TotalTokens }),
			CacheCreationInputTokens: nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CacheCreationInputTokens }),
			CacheReadInputTokens:     nullInt32FromUsage(n.Usage, func(u *llm.Usage) int { return u.CacheReadInputTokens }),
			TotalDurationNs:          nullInt64FromUsage(n.Usage, func(u *llm.Usage) int64 { return u.TotalDurationNs }),
			PromptDurationNs:         nullInt64FromUsage(n.Usage, func(u *llm.Usage) int64 { return u.PromptDurationNs }),
			Project:                  nullStringValue(n.Project),
			CreatedAt:                pgtype.Timestamptz{Time: createdAt, Valid: true},
			ParentHash:               nullStringPtr(n.ParentHash),
			RequestSystem:            reqSystem,
			RequestMaxTokens:         reqMaxTokens,
			RequestTemperature:       reqTemperature,
			RequestStream:            reqStream,
			RequestToolCount:         reqToolCount,
			NodeKind:                 nullStringValue(n.Kind),
			ParentToolUseID:          nullStringValue(n.ParentToolUseID),
			ThreadID:                 nullStringValue(n.ThreadID),
			SessionID:                sessionID,
		}); err != nil {
			return fmt.Errorf("upsert node %s: %w", n.Hash, err)
		}
		set.Report.Upserted++
		keepHashes = append(keepHashes, n.Hash)
	}

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

	if len(coveredSessions) > 0 && len(keepHashes) > 0 {
		pruned, err := qtx.PruneDerivedNodes(ctx, gensqlc.PruneDerivedNodesParams{
			OrgID:      orgID,
			SessionIds: coveredSessions,
			KeepHashes: keepHashes,
		})
		if err != nil {
			return fmt.Errorf("prune stale derived nodes: %w", err)
		}
		set.Report.Pruned = int(pruned)
	}

	// The span projection rides the same transaction: traces, spans,
	// and links are as derived as the nodes are, and a derive pass
	// either lands both layers or neither.
	if err := writeSpanSet(ctx, qtx, orgID, sessionIDs, coveredSessions, derive.EmitSpans(set)); err != nil {
		return fmt.Errorf("write span set: %w", err)
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

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// writeSpanSet persists the span projection inside the derive
// transaction: upsert every trace/span/link the emitter produced for
// the covered sessions, then prune rows a superseded projection wrote.
// Deterministic span identity makes the upserts idempotent — on
// unchanged raw, every row rewrites in place and prune removes zero.
func writeSpanSet(
	ctx context.Context,
	qtx *gensqlc.Queries,
	orgID pgtype.UUID,
	sessionIDs map[derive.SessionKey]pgtype.UUID,
	coveredSessions []pgtype.UUID,
	spans *derive.SpanSet,
) error {
	keepTraces := make([]string, 0, len(spans.Turns))
	// Parallel keep-key arrays for the tuple-membership prune (a '|'-
	// joined key would collide on wire ids that contain the delimiter).
	var keepSpanTraceIDs, keepSpanIDs []string
	var keepLinkFromTrace, keepLinkFromSpan, keepLinkToTrace, keepLinkToSpan, keepLinkFromIO, keepLinkToIO []string

	turnSession := map[string]pgtype.UUID{}
	for _, turn := range spans.Turns {
		sid, ok := sessionIDs[turn.Session]
		if !ok {
			// Unresolved session (raw rows whose sessions row never
			// landed). A NULL-session span row is unreachable by every
			// prune path (they scope by session_id = ANY(covered)), so it
			// would leak as an orphan — don't write it at all. Mirrors how
			// SessionTitles and ModelUsage skip unresolved keys.
			continue
		}
		turnSession[turn.TraceID] = sid
		keepTraces = append(keepTraces, turn.TraceID)
		costNumeric, err := numericFromFloat(turn.TotalCostUSD)
		if err != nil {
			return fmt.Errorf("encode trace cost %s: %w", turn.TraceID, err)
		}
		if err := qtx.UpsertSpanTurn(ctx, gensqlc.UpsertSpanTurnParams{
			OrgID:               orgID,
			TraceID:             turn.TraceID,
			SessionID:           sid,
			UserPrompt:          turn.UserPrompt,
			ResponsePreview:     turn.ResponsePreview,
			Synthetic:           turn.Synthetic,
			Status:              "ok",
			StartedAt:           pgtype.Timestamptz{Time: turn.StartedAt, Valid: true},
			EndedAt:             pgtype.Timestamptz{Time: turn.EndedAt, Valid: !turn.EndedAt.IsZero()},
			DurationNs:          turn.EndedAt.Sub(turn.StartedAt).Nanoseconds(),
			TotalInputTokens:    turn.TotalInputTokens,
			TotalOutputTokens:   turn.TotalOutputTokens,
			MainInputTokens:     turn.MainInputTokens,
			MainOutputTokens:    turn.MainOutputTokens,
			CacheReadTokens:     turn.CacheReadTokens,
			CacheCreationTokens: turn.CacheCreationTokens,
			TotalCostUsd:        costNumeric,
			Source:              turn.Source,
		}); err != nil {
			return fmt.Errorf("upsert span turn %s: %w", turn.TraceID, err)
		}

		for _, s := range turn.Spans {
			keepSpanTraceIDs = append(keepSpanTraceIDs, turn.TraceID)
			keepSpanIDs = append(keepSpanIDs, s.SpanID)
			input, err := contentJSON(s.Input)
			if err != nil {
				return fmt.Errorf("marshal span %s input: %w", s.SpanID, err)
			}
			output, err := contentJSON(s.Output)
			if err != nil {
				return fmt.Errorf("marshal span %s output: %w", s.SpanID, err)
			}
			var usage []byte
			if s.Usage != nil {
				if usage, err = json.Marshal(s.Usage); err != nil {
					return fmt.Errorf("marshal span %s usage: %w", s.SpanID, err)
				}
			}
			var verdict []byte
			if s.Verdict != nil {
				if verdict, err = json.Marshal(s.Verdict); err != nil {
					return fmt.Errorf("marshal span %s verdict: %w", s.SpanID, err)
				}
			}
			rawTurn := pgtype.Int8{}
			if s.RawTurnID != 0 {
				rawTurn = pgtype.Int8{Int64: s.RawTurnID, Valid: true}
			}
			if err := qtx.UpsertSpan(ctx, gensqlc.UpsertSpanParams{
				OrgID:        orgID,
				TraceID:      turn.TraceID,
				SpanID:       s.SpanID,
				ParentSpanID: s.ParentSpanID,
				SessionID:    sid,
				Kind:         s.Kind,
				Name:         s.Name,
				Status:       s.Status,
				CallKind:     s.CallKind,
				ThreadID:     s.ThreadID,
				Model:        s.Model,
				StopReason:   s.StopReason,
				StartedAt:    pgtype.Timestamptz{Time: s.StartedAt, Valid: true},
				DurationNs:   s.DurationNS,
				Seq:          s.Seq,
				Input:        input,
				Output:       output,
				Usage:        usage,
				RawTurnID:    rawTurn,
				NodeHash:     s.NodeHash,
				Verdict:      verdict,
			}); err != nil {
				return fmt.Errorf("upsert span %s/%s: %w", turn.TraceID, s.SpanID, err)
			}
		}
	}

	writeLink := func(l *derive.SpanLink) error {
		sid, ok := turnSession[l.FromTraceID]
		if !ok {
			// The from-trace's turn resolved to no session (or was
			// skipped above), so this link would carry a NULL session_id
			// no prune path can reach. Skip it for the same reason.
			return nil
		}
		keepLinkFromTrace = append(keepLinkFromTrace, l.FromTraceID)
		keepLinkFromSpan = append(keepLinkFromSpan, l.FromSpanID)
		keepLinkToTrace = append(keepLinkToTrace, l.ToTraceID)
		keepLinkToSpan = append(keepLinkToSpan, l.ToSpanID)
		keepLinkFromIO = append(keepLinkFromIO, l.FromIO)
		keepLinkToIO = append(keepLinkToIO, l.ToIO)
		return qtx.UpsertSpanLink(ctx, gensqlc.UpsertSpanLinkParams{
			OrgID:       orgID,
			FromTraceID: l.FromTraceID,
			FromSpanID:  l.FromSpanID,
			FromIo:      l.FromIO,
			ToTraceID:   l.ToTraceID,
			ToSpanID:    l.ToSpanID,
			ToIo:        l.ToIO,
			Kind:        l.Kind,
			SessionID:   sid,
		})
	}
	for _, turn := range spans.Turns {
		for _, l := range turn.Links {
			if err := writeLink(l); err != nil {
				return fmt.Errorf("upsert span link %s->%s: %w", l.FromSpanID, l.ToSpanID, err)
			}
		}
	}
	for _, l := range spans.Links {
		if err := writeLink(l); err != nil {
			return fmt.Errorf("upsert cross-trace link %s->%s: %w", l.FromSpanID, l.ToSpanID, err)
		}
	}

	if len(coveredSessions) == 0 {
		return nil
	}
	if len(keepTraces) > 0 {
		if _, err := qtx.PruneSpanLinks(ctx, gensqlc.PruneSpanLinksParams{
			OrgID:            orgID,
			SessionIds:       coveredSessions,
			KeepFromTraceIds: keepLinkFromTrace,
			KeepFromSpanIds:  keepLinkFromSpan,
			KeepToTraceIds:   keepLinkToTrace,
			KeepToSpanIds:    keepLinkToSpan,
			KeepFromIos:      keepLinkFromIO,
			KeepToIos:        keepLinkToIO,
		}); err != nil {
			return fmt.Errorf("prune span links: %w", err)
		}
		if _, err := qtx.PruneSpans(ctx, gensqlc.PruneSpansParams{
			OrgID:        orgID,
			SessionIds:   coveredSessions,
			KeepTraceIds: keepSpanTraceIDs,
			KeepSpanIds:  keepSpanIDs,
		}); err != nil {
			return fmt.Errorf("prune spans: %w", err)
		}
		if _, err := qtx.PruneSpanTurns(ctx, gensqlc.PruneSpanTurnsParams{
			OrgID:        orgID,
			SessionIds:   coveredSessions,
			KeepTraceIds: keepTraces,
		}); err != nil {
			return fmt.Errorf("prune span turns: %w", err)
		}
	}
	if err := qtx.FoldSessionRollupsFromSpans(ctx, coveredSessions); err != nil {
		return fmt.Errorf("fold session rollups: %w", err)
	}

	// Per-model spend breakdown (#28): the deriver priced it in Go (the
	// price table lives there, not in SQL), so it writes directly as a
	// JSONB array rather than riding the SQL rollup fold above.
	for key, usage := range spans.ModelUsage {
		sid, ok := sessionIDs[key]
		if !ok || !sid.Valid {
			continue
		}
		payload, err := json.Marshal(usage)
		if err != nil {
			return fmt.Errorf("marshal model usage: %w", err)
		}
		if err := qtx.UpdateSessionModelUsage(ctx, gensqlc.UpdateSessionModelUsageParams{
			ID:         sid,
			ModelUsage: payload,
		}); err != nil {
			return fmt.Errorf("update session model usage: %w", err)
		}
	}

	// Session-scoped task fold and call_kind tally (deriver-owned, folded
	// in Go for the same reason as model_usage). Written as JSONB so the
	// read/export paths never re-fold.
	for key, tasks := range spans.Tasks {
		sid, ok := sessionIDs[key]
		if !ok || !sid.Valid {
			continue
		}
		payload, err := json.Marshal(tasks)
		if err != nil {
			return fmt.Errorf("marshal session tasks: %w", err)
		}
		if err := qtx.UpdateSessionTasks(ctx, gensqlc.UpdateSessionTasksParams{ID: sid, Tasks: payload}); err != nil {
			return fmt.Errorf("update session tasks: %w", err)
		}
	}
	for key, counts := range spans.KindCounts {
		sid, ok := sessionIDs[key]
		if !ok || !sid.Valid {
			continue
		}
		payload, err := json.Marshal(counts)
		if err != nil {
			return fmt.Errorf("marshal session kind_counts: %w", err)
		}
		if err := qtx.UpdateSessionKindCounts(ctx, gensqlc.UpdateSessionKindCountsParams{ID: sid, KindCounts: payload}); err != nil {
			return fmt.Errorf("update session kind_counts: %w", err)
		}
	}
	return nil
}

// contentJSON marshals content blocks, keeping empty payloads as SQL
// NULL rather than a JSON empty-array blob.
func contentJSON(blocks []llm.ContentBlock) ([]byte, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	return json.Marshal(blocks)
}

// ListSessionSpanModel returns the stored span projection for one
// session: turns, spans, and links, each in stable presentation order.
// Implements storage.SpanModelReader.
func (d *Driver) ListSessionSpanModel(ctx context.Context, sessionID string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	if d == nil || d.conn == nil {
		return nil, nil, nil, errors.New("postgres driver not open")
	}
	parsed, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse session id: %w", err)
	}
	sid := pgtype.UUID{Bytes: parsed, Valid: true}

	turnRows, err := d.q.ListSpanTurnsBySession(ctx, sid)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list span turns: %w", err)
	}
	spanRows, err := d.q.ListSpansBySession(ctx, sid)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list spans: %w", err)
	}
	linkRows, err := d.q.ListSpanLinksBySession(ctx, sid)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list span links: %w", err)
	}

	turns := make([]storage.SpanTurnRecord, 0, len(turnRows))
	for _, row := range turnRows {
		rec := spanTurnRecordFromColumns(spanTurnColumns{
			traceID: row.TraceID, userPrompt: row.UserPrompt,
			responsePreview: row.ResponsePreview,
			synthetic:       row.Synthetic, status: row.Status,
			sessionID: row.SessionID, startedAt: row.StartedAt,
			endedAt: row.EndedAt, durationNs: row.DurationNs,
			totalIn: row.TotalInputTokens, totalOut: row.TotalOutputTokens,
			mainIn: row.MainInputTokens, mainOut: row.MainOutputTokens,
			cacheRead: row.CacheReadTokens, cacheCreation: row.CacheCreationTokens,
			cost: row.TotalCostUsd,
		})
		turns = append(turns, rec)
	}

	spans := make([]storage.SpanRecord, 0, len(spanRows))
	for _, row := range spanRows {
		spans = append(spans, spanRecordFromRow(row))
	}

	links := make([]storage.SpanLinkRecord, 0, len(linkRows))
	for _, row := range linkRows {
		links = append(links, storage.SpanLinkRecord{
			FromTraceID: row.FromTraceID,
			FromSpanID:  row.FromSpanID,
			FromIO:      row.FromIo,
			ToTraceID:   row.ToTraceID,
			ToSpanID:    row.ToSpanID,
			ToIO:        row.ToIo,
			Kind:        row.Kind,
		})
	}

	return turns, spans, links, nil
}

// spanTurnRecordFromRow converts a span_turns row to its flat record.
type spanTurnColumns struct {
	traceID, userPrompt, responsePreview      string
	synthetic, status                         string
	sessionID                                 pgtype.UUID
	startedAt, endedAt                        pgtype.Timestamptz
	durationNs, totalIn, totalOut             int64
	mainIn, mainOut, cacheRead, cacheCreation int64
	cost                                      pgtype.Numeric
}

func spanTurnRecordFromColumns(c spanTurnColumns) storage.SpanTurnRecord {
	rec := storage.SpanTurnRecord{
		TraceID:             c.traceID,
		UserPrompt:          c.userPrompt,
		ResponsePreview:     c.responsePreview,
		Synthetic:           c.synthetic,
		Status:              c.status,
		StartedAt:           c.startedAt.Time,
		DurationNS:          c.durationNs,
		TotalInputTokens:    c.totalIn,
		TotalOutputTokens:   c.totalOut,
		MainInputTokens:     c.mainIn,
		MainOutputTokens:    c.mainOut,
		CacheReadTokens:     c.cacheRead,
		CacheCreationTokens: c.cacheCreation,
	}
	sessionID, endedAt, cost := c.sessionID, c.endedAt, c.cost
	if sessionID.Valid {
		rec.SessionID = uuidString(sessionID)
	}
	if endedAt.Valid {
		t := endedAt.Time
		rec.EndedAt = &t
	}
	if cost.Valid {
		if f, err := cost.Float64Value(); err == nil && f.Valid {
			rec.TotalCostUSD = f.Float64
		}
	}
	return rec
}

// spanRecordFromRow converts a versioned spans row to its flat record.
func spanRecordFromRow(row gensqlc.Spans20260615) storage.SpanRecord {
	return storage.SpanRecord{
		TraceID:      row.TraceID,
		SpanID:       row.SpanID,
		ParentSpanID: row.ParentSpanID,
		Kind:         row.Kind,
		Name:         row.Name,
		Status:       row.Status,
		CallKind:     row.CallKind,
		ThreadID:     row.ThreadID,
		Model:        row.Model,
		StopReason:   row.StopReason,
		StartedAt:    row.StartedAt.Time,
		DurationNS:   row.DurationNs,
		Seq:          row.Seq,
		Input:        row.Input,
		Output:       row.Output,
		Usage:        row.Usage,
		RawTurnID:    row.RawTurnID.Int64,
		NodeHash:     row.NodeHash,
		Verdict:      row.Verdict,
	}
}

// ListTraceSummaries returns a session's turn headers with span counts
// — the lazy session-detail rows. Implements storage.SpanModelReader.
func (d *Driver) ListTraceSummaries(ctx context.Context, sessionID string) ([]storage.TraceSummaryRecord, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	parsed, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, fmt.Errorf("parse session id: %w", err)
	}
	rows, err := d.q.ListTraceSummariesBySession(ctx, pgtype.UUID{Bytes: parsed, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("list trace summaries: %w", err)
	}
	out := make([]storage.TraceSummaryRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, storage.TraceSummaryRecord{
			SpanTurnRecord: spanTurnRecordFromColumns(spanTurnColumns{
				traceID: row.TraceID, userPrompt: row.UserPrompt,
				responsePreview: row.ResponsePreview,
				synthetic:       row.Synthetic, status: row.Status,
				sessionID: row.SessionID, startedAt: row.StartedAt,
				endedAt: row.EndedAt, durationNs: row.DurationNs,
				totalIn: row.TotalInputTokens, totalOut: row.TotalOutputTokens,
				mainIn: row.MainInputTokens, mainOut: row.MainOutputTokens,
				cacheRead: row.CacheReadTokens, cacheCreation: row.CacheCreationTokens,
				cost: row.TotalCostUsd,
			}),
			SpanCount: int(row.SpanCount),
		})
	}
	return out, nil
}

// GetTraceDetail returns one turn with its spans and links. Implements
// storage.SpanModelReader.
func (d *Driver) GetTraceDetail(ctx context.Context, orgID, traceID string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	if d == nil || d.conn == nil {
		return nil, nil, nil, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgKeyForLookup(orgID))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode org_id: %w", err)
	}
	row, err := d.q.GetSpanTurn(ctx, gensqlc.GetSpanTurnParams{OrgID: org, TraceID: traceID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil, nil
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get span turn: %w", err)
	}
	turn := spanTurnRecordFromColumns(spanTurnColumns{
		traceID: row.TraceID, userPrompt: row.UserPrompt,
		responsePreview: row.ResponsePreview,
		synthetic:       row.Synthetic, status: row.Status,
		sessionID: row.SessionID, startedAt: row.StartedAt,
		endedAt: row.EndedAt, durationNs: row.DurationNs,
		totalIn: row.TotalInputTokens, totalOut: row.TotalOutputTokens,
		mainIn: row.MainInputTokens, mainOut: row.MainOutputTokens,
		cacheRead: row.CacheReadTokens, cacheCreation: row.CacheCreationTokens,
		cost: row.TotalCostUsd,
	})

	spanRows, err := d.q.ListSpansByTrace(ctx, gensqlc.ListSpansByTraceParams{OrgID: org, TraceID: traceID})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list spans by trace: %w", err)
	}
	spans := make([]storage.SpanRecord, 0, len(spanRows))
	for _, r := range spanRows {
		spans = append(spans, spanRecordFromRow(r))
	}

	linkRows, err := d.q.ListSpanLinksByTrace(ctx, gensqlc.ListSpanLinksByTraceParams{OrgID: org, FromTraceID: traceID})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list span links by trace: %w", err)
	}
	links := make([]storage.SpanLinkRecord, 0, len(linkRows))
	for _, r := range linkRows {
		links = append(links, storage.SpanLinkRecord{
			FromTraceID: r.FromTraceID, FromSpanID: r.FromSpanID, FromIO: r.FromIo,
			ToTraceID: r.ToTraceID, ToSpanID: r.ToSpanID, ToIO: r.ToIo,
			Kind: r.Kind,
		})
	}
	return &turn, spans, links, nil
}

// GetSpanRecord returns one span with full payloads. Implements
// storage.SpanModelReader.
func (d *Driver) GetSpanRecord(ctx context.Context, orgID, traceID, spanID string) (*storage.SpanRecord, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgKeyForLookup(orgID))
	if err != nil {
		return nil, fmt.Errorf("decode org_id: %w", err)
	}
	row, err := d.q.GetSpan(ctx, gensqlc.GetSpanParams{OrgID: org, TraceID: traceID, SpanID: spanID})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get span: %w", err)
	}
	rec := spanRecordFromRow(row)
	return &rec, nil
}

// AggregateSpanStats sums the trace-grain rollups over a time window —
// the span-layer aggregate behind /v1/stats. Implements
// storage.SpanStatsReader.
func (d *Driver) AggregateSpanStats(ctx context.Context, orgID string, since, until *time.Time) (storage.SpanStats, error) {
	if d == nil || d.conn == nil {
		return storage.SpanStats{}, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgKeyForLookup(orgID))
	if err != nil {
		return storage.SpanStats{}, fmt.Errorf("decode org_id: %w", err)
	}
	row, err := d.q.AggregateSpanStats(ctx, gensqlc.AggregateSpanStatsParams{
		OrgID:       org,
		SinceFilter: nullTimePtr(since),
		UntilFilter: nullTimePtr(until),
	})
	if err != nil {
		return storage.SpanStats{}, fmt.Errorf("aggregate span stats: %w", err)
	}
	stats := storage.SpanStats{
		TurnCount:           int(row.TurnCount),
		SessionCount:        int(row.SessionCount),
		CompletedCount:      int(row.CompletedCount),
		InputTokens:         row.InputTokens,
		OutputTokens:        row.OutputTokens,
		CacheCreationTokens: row.CacheCreationTokens,
		CacheReadTokens:     row.CacheReadTokens,
		TotalDurationNS:     row.TotalDurationNs,
		ToolCalls:           int(row.ToolCalls),
	}
	if row.TotalCostUsd.Valid {
		if f, err := row.TotalCostUsd.Float64Value(); err == nil && f.Valid {
			stats.TotalCostUSD = f.Float64
		}
	}
	return stats, nil
}

// ListRawTurnHeaders returns the wire log for one session: capture
// identity and payload sizes, no blobs. Implements
// storage.SpanModelReader.
func (d *Driver) ListRawTurnHeaders(ctx context.Context, orgID, harnessID, harnessSessionID string) ([]storage.RawTurnHeader, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	org, err := orgIDFromString(orgKeyForLookup(orgID))
	if err != nil {
		return nil, fmt.Errorf("decode org_id: %w", err)
	}
	rows, err := d.q.ListRawTurnHeadersBySession(ctx, gensqlc.ListRawTurnHeadersBySessionParams{
		OrgID:            org,
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("list raw turn headers: %w", err)
	}
	out := make([]storage.RawTurnHeader, 0, len(rows))
	for _, r := range rows {
		out = append(out, storage.RawTurnHeader{
			ID:            r.ID,
			Source:        r.Source,
			Provider:      r.Provider,
			AgentName:     r.AgentName,
			RequestID:     r.RequestID,
			ReceivedAt:    r.ReceivedAt.Time,
			Meta:          r.Meta,
			RequestBytes:  r.RequestBytes,
			ResponseBytes: r.ResponseBytes,
		})
	}
	return out, nil
}

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
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
	var keepSpans, keepLinks []string

	sessionOf := func(key derive.SessionKey) pgtype.UUID {
		if id, ok := sessionIDs[key]; ok {
			return id
		}
		return pgtype.UUID{}
	}

	turnSession := map[string]pgtype.UUID{}
	for _, turn := range spans.Turns {
		sid := sessionOf(turn.Session)
		turnSession[turn.TraceID] = sid
		keepTraces = append(keepTraces, turn.TraceID)
		if err := qtx.UpsertSpanTurn(ctx, gensqlc.UpsertSpanTurnParams{
			OrgID:             orgID,
			TraceID:           turn.TraceID,
			SessionID:         sid,
			UserPrompt:        turn.UserPrompt,
			Synthetic:         turn.Synthetic,
			Status:            "ok",
			StartedAt:         pgtype.Timestamptz{Time: turn.StartedAt, Valid: true},
			EndedAt:           pgtype.Timestamptz{Time: turn.EndedAt, Valid: !turn.EndedAt.IsZero()},
			DurationNs:        turn.EndedAt.Sub(turn.StartedAt).Nanoseconds(),
			TotalInputTokens:  turn.TotalInputTokens,
			TotalOutputTokens: turn.TotalOutputTokens,
		}); err != nil {
			return fmt.Errorf("upsert span turn %s: %w", turn.TraceID, err)
		}

		for _, s := range turn.Spans {
			keepSpans = append(keepSpans, turn.TraceID+"|"+s.SpanID)
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
				Input:        input,
				Output:       output,
				Usage:        usage,
				RawTurnID:    rawTurn,
				NodeHash:     s.NodeHash,
			}); err != nil {
				return fmt.Errorf("upsert span %s/%s: %w", turn.TraceID, s.SpanID, err)
			}
		}
	}

	writeLink := func(l *derive.SpanLink) error {
		keepLinks = append(keepLinks,
			l.FromTraceID+"|"+l.FromSpanID+"|"+l.ToTraceID+"|"+l.ToSpanID+"|"+l.FromIO+"|"+l.ToIO)
		return qtx.UpsertSpanLink(ctx, gensqlc.UpsertSpanLinkParams{
			OrgID:       orgID,
			FromTraceID: l.FromTraceID,
			FromSpanID:  l.FromSpanID,
			FromIo:      l.FromIO,
			ToTraceID:   l.ToTraceID,
			ToSpanID:    l.ToSpanID,
			ToIo:        l.ToIO,
			Kind:        l.Kind,
			SessionID:   turnSession[l.FromTraceID],
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
			OrgID:      orgID,
			SessionIds: coveredSessions,
			KeepKeys:   keepLinks,
		}); err != nil {
			return fmt.Errorf("prune span links: %w", err)
		}
		if _, err := qtx.PruneSpans(ctx, gensqlc.PruneSpansParams{
			OrgID:      orgID,
			SessionIds: coveredSessions,
			KeepKeys:   keepSpans,
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
		rec := storage.SpanTurnRecord{
			TraceID:           row.TraceID,
			SessionID:         sessionID,
			UserPrompt:        row.UserPrompt,
			Synthetic:         row.Synthetic,
			Status:            row.Status,
			StartedAt:         row.StartedAt.Time,
			DurationNS:        row.DurationNs,
			TotalInputTokens:  row.TotalInputTokens,
			TotalOutputTokens: row.TotalOutputTokens,
		}
		if row.EndedAt.Valid {
			t := row.EndedAt.Time
			rec.EndedAt = &t
		}
		if row.TotalCostUsd.Valid {
			if f, err := row.TotalCostUsd.Float64Value(); err == nil && f.Valid {
				rec.TotalCostUSD = f.Float64
			}
		}
		turns = append(turns, rec)
	}

	spans := make([]storage.SpanRecord, 0, len(spanRows))
	for _, row := range spanRows {
		spans = append(spans, storage.SpanRecord{
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
			Input:        row.Input,
			Output:       row.Output,
			Usage:        row.Usage,
			RawTurnID:    row.RawTurnID.Int64,
			NodeHash:     row.NodeHash,
		})
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

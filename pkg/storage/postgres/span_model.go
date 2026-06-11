package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

var (
	_ storage.SpanIngester = (*Driver)(nil)
	_ storage.SpanReader   = (*Driver)(nil)
)

type spanDraft struct {
	SpanID       string
	ParentSpanID string
	Kind         string
	Name         string
	Status       string
	StartNS      int64
	DurationNS   int64
	Input        any
	Output       any
	Metadata     any
	Metrics      any
	Raw          any
}

type spanLinkDraft struct {
	FromSpanID string
	ToSpanID   string
	FromIO     string
	ToIO       string
	Metadata   any
}

// IngestSpanTurn stores a completed provider turn using the experimental
// Lapdog-style span model. It is intentionally independent from Merkle nodes:
// continuity is provided by the harness session envelope plus generated trace
// and span ids; prompt snapshots are payloads on spans, not graph identity.
func (d *Driver) IngestSpanTurn(ctx context.Context, req storage.IngestSpanTurnRequest) (storage.IngestSpanTurnResult, error) {
	if d == nil || d.conn == nil {
		return storage.IngestSpanTurnResult{}, errors.New("postgres driver not open")
	}
	if req.Request == nil || req.Response == nil {
		return storage.IngestSpanTurnResult{}, errors.New("span ingest: missing request or response")
	}

	seedBytes, err := json.Marshal(struct {
		Provider string            `json:"provider"`
		Agent    string            `json:"agent"`
		Request  *llm.ChatRequest  `json:"request"`
		Response *llm.ChatResponse `json:"response"`
	}{Provider: req.Provider, Agent: req.AgentName, Request: req.Request, Response: req.Response})
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("span ingest seed: %w", err)
	}
	seed := hashHex(seedBytes)

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("begin span ingest tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	qtx := d.q.WithTx(tx)
	now := time.Now().UTC()
	nowTS := pgtype.Timestamptz{Time: now, Valid: true}
	envelope, harnessSessionID := resolveSpanHarnessSessionID(req.Session, seed)
	orgID, err := orgIDFromEnvelope(envelope)
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("span ingest org_id: %w", err)
	}
	parentSessionID, err := resolveParentSessionID(ctx, qtx, envelope, orgID, nowTS)
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("span ingest parent session: %w", err)
	}

	sessionUUID, err := newAppUUID()
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("mint session uuid: %w", err)
	}
	metadata := []byte(envelope.HarnessMetadata)
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}
	sessionRow, err := qtx.UpsertSession(ctx, gensqlc.UpsertSessionParams{
		ID:               sessionUUID,
		OrgID:            orgID,
		AuthSubject:      envelope.AuthSubject,
		HarnessID:        envelope.HarnessIDOrUnknown(),
		HarnessSessionID: harnessSessionID,
		Name:             nullStringValue(envelope.Name),
		Cwd:              nullStringValue(envelope.Cwd),
		HarnessVersion:   nullStringValue(envelope.HarnessVersion),
		ParentSessionID:  parentSessionID,
		Now:              nowTS,
		HarnessMetadata:  metadata,
	})
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("upsert span session: %w", err)
	}

	traceID := ""
	if req.SpanContext != nil {
		traceID = strings.TrimSpace(req.SpanContext.TraceID)
	}
	if traceID == "" {
		traceID, err = newTraceID()
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("mint trace id: %w", err)
		}
	}
	turnUUID, err := newAppUUID()
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("mint turn uuid: %w", err)
	}
	rootSpanID := ""
	llmSpanID := ""
	llmParentSpanID := ""
	if req.SpanContext != nil {
		rootSpanID = strings.TrimSpace(req.SpanContext.RootSpanID)
		llmSpanID = strings.TrimSpace(req.SpanContext.LLMSpanID)
		llmParentSpanID = strings.TrimSpace(req.SpanContext.ParentSpanID)
	}
	if rootSpanID == "" {
		rootSpanID, err = newSpanID("agent")
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("mint root span id: %w", err)
		}
	}
	if llmSpanID == "" {
		llmSpanID, err = newSpanID("llm")
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("mint llm span id: %w", err)
		}
	}
	if llmParentSpanID == "" {
		llmParentSpanID = rootSpanID
	}
	durationNS := usageDuration(req.Response.Usage)
	endTime := now
	startTime := endTime.Add(-time.Duration(durationNS))
	if durationNS == 0 {
		startTime = now
	}
	startNS := startTime.UnixNano()
	metrics := usageMetrics(req.Response.Usage, req.CostUSD)
	userPrompt := firstUserPrompt(req.Request)
	status := responseStatus(req.Response)
	inputTokens, outputTokens := usageTokenDeltas(req.Response.Usage)
	costNumeric, err := numericFromFloat(req.CostUSD)
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("span ingest cost: %w", err)
	}
	turnMetadata := map[string]any{
		"provider":   req.Provider,
		"agent_name": req.AgentName,
		"model":      req.Request.Model,
		"project":    req.Project,
		"source":     "tapes-span-poc",
	}
	turnMetadataJSON, err := json.Marshal(turnMetadata)
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal turn metadata: %w", err)
	}

	turnID := uuidString(turnUUID)
	insertedTurn := true
	err = tx.QueryRow(ctx, `
INSERT INTO span_turns (
    id, org_id, session_id, trace_id, harness_turn_id, user_prompt, status,
    started_at, ended_at, duration_ns, total_input_tokens, total_output_tokens,
    total_cost_usd, metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (org_id, trace_id) DO NOTHING
RETURNING id::text`, turnUUID, orgID, sessionRow.ID, traceID, harnessTurnID(req.SpanContext, traceID), userPrompt, status, pgtype.Timestamptz{Time: startTime, Valid: true}, pgtype.Timestamptz{Time: endTime, Valid: true}, durationNS, inputTokens, outputTokens, costNumeric, turnMetadataJSON).Scan(&turnID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("insert span turn: %w", err)
		}
		insertedTurn = false
		err = tx.QueryRow(ctx, `SELECT id::text FROM span_turns WHERE org_id = $1 AND trace_id = $2`, orgID, traceID).Scan(&turnID)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("lookup existing span turn: %w", err)
		}
	}
	turnUUIDParsed, err := uuid.Parse(turnID)
	if err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("parse turn uuid: %w", err)
	}
	turnIDPg := pgtype.UUID{Bytes: turnUUIDParsed, Valid: true}

	spans := []spanDraft{
		{
			SpanID:     rootSpanID,
			Kind:       storage.SpanKindAgent,
			Name:       agentSpanName(req.AgentName),
			Status:     status,
			StartNS:    startNS,
			DurationNS: durationNS,
			Input:      map[string]any{"value": userPrompt},
			Output:     map[string]any{"value": responseText(req.Response)},
			Metadata:   turnMetadata,
			Metrics:    metrics,
			Raw:        map[string]any{},
		},
		{
			SpanID:       llmSpanID,
			ParentSpanID: llmParentSpanID,
			Kind:         storage.SpanKindLLM,
			Name:         req.Request.Model,
			Status:       status,
			StartNS:      startNS,
			DurationNS:   durationNS,
			Input:        map[string]any{"messages": llmInputMessages(req.Request)},
			Output:       map[string]any{"messages": []llm.Message{req.Response.Message}, "stop_reason": req.Response.StopReason},
			Metadata: map[string]any{
				"model_name":     req.Request.Model,
				"model_provider": req.Provider,
				"stream":         req.Request.Stream != nil && *req.Request.Stream,
			},
			Metrics: metrics,
			Raw: map[string]any{
				"request":  req.Request,
				"response": req.Response,
			},
		},
	}

	links := make([]spanLinkDraft, 0)
	for i, block := range req.Response.Message.Content {
		if block.Type != "tool_use" && block.Type != "server_tool_use" {
			continue
		}
		toolSpanID, err := newSpanID("tool")
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("mint tool span id: %w", err)
		}
		toolName := block.ToolName
		if toolName == "" {
			toolName = "tool"
		}
		spans = append(spans, spanDraft{
			SpanID:       toolSpanID,
			ParentSpanID: rootSpanID,
			Kind:         storage.SpanKindTool,
			Name:         toolName,
			Status:       "ok",
			StartNS:      startNS + int64(i) + 1,
			DurationNS:   0,
			Input:        map[string]any{"arguments": block.ToolInput},
			Output:       map[string]any{},
			Metadata: map[string]any{
				"tool_id":   block.ToolUseID,
				"tool_name": toolName,
				"source":    "assistant_tool_use",
			},
			Metrics: map[string]any{},
			Raw:     block,
		})
		links = append(links, spanLinkDraft{
			FromSpanID: llmSpanID,
			ToSpanID:   toolSpanID,
			FromIO:     "output",
			ToIO:       "input",
			Metadata:   map[string]any{"tool_id": block.ToolUseID},
		})
	}

	spanCount := 0
	for _, draft := range spans {
		createdID, err := newAppUUID()
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("mint span uuid: %w", err)
		}
		inputJSON, err := json.Marshal(draft.Input)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal span input: %w", err)
		}
		outputJSON, err := json.Marshal(draft.Output)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal span output: %w", err)
		}
		metadataJSON, err := json.Marshal(draft.Metadata)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal span metadata: %w", err)
		}
		metricsJSON, err := json.Marshal(draft.Metrics)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal span metrics: %w", err)
		}
		rawJSON, err := json.Marshal(draft.Raw)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal span raw: %w", err)
		}
		tag, err := tx.Exec(ctx, `
INSERT INTO spans (
    id, org_id, session_id, turn_id, trace_id, span_id, parent_span_id, kind,
    name, status, start_ns, duration_ns, input, output, metadata, metrics, raw
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (org_id, trace_id, span_id) DO NOTHING`, createdID, orgID, sessionRow.ID, turnIDPg, traceID, draft.SpanID, nullStringValue(draft.ParentSpanID), draft.Kind, draft.Name, draft.Status, draft.StartNS, draft.DurationNS, inputJSON, outputJSON, metadataJSON, metricsJSON, rawJSON)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("insert span %s: %w", draft.SpanID, err)
		}
		spanCount += int(tag.RowsAffected())
	}

	for _, link := range links {
		metadataJSON, err := json.Marshal(link.Metadata)
		if err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("marshal span link metadata: %w", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO span_links (org_id, trace_id, from_span_id, to_span_id, from_io, to_io, metadata)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT DO NOTHING`, orgID, traceID, link.FromSpanID, link.ToSpanID, link.FromIO, link.ToIO, metadataJSON); err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("insert span link: %w", err)
		}
	}

	if insertedTurn {
		if err := qtx.UpdateSessionCounters(ctx, gensqlc.UpdateSessionCountersParams{
			Now:               nowTS,
			TurnCountDelta:    1,
			InputTokensDelta:  inputTokens,
			OutputTokensDelta: outputTokens,
			CostUsdDelta:      costNumeric,
			ID:                sessionRow.ID,
		}); err != nil {
			return storage.IngestSpanTurnResult{}, fmt.Errorf("update span session counters: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return storage.IngestSpanTurnResult{}, fmt.Errorf("commit span ingest tx: %w", err)
	}
	return storage.IngestSpanTurnResult{SessionID: uuidString(sessionRow.ID), TurnID: turnID, TraceID: traceID, SpanCount: spanCount}, nil
}

func resolveSpanHarnessSessionID(envelope *sessions.IngestEnvelope, seed string) (*sessions.IngestEnvelope, string) {
	if envelope != nil && !envelope.NeedsSyntheticHarnessSessionID() {
		return envelope, envelope.HarnessSessionID
	}
	prefix := seed
	if len(prefix) > syntheticHarnessSessionIDPrefixLen {
		prefix = prefix[:syntheticHarnessSessionIDPrefixLen]
	}
	out := &sessions.IngestEnvelope{}
	if envelope != nil {
		*out = *envelope
	}
	if out.HarnessID == "" {
		out.HarnessID = "unknown"
	}
	out.HarnessSessionID = prefix
	return out, prefix
}

func hashHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newTraceID() (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return "trc_" + strings.ReplaceAll(u.String(), "-", ""), nil
}

func newSpanID(prefix string) (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	compact := strings.ReplaceAll(u.String(), "-", "")
	if len(compact) > 16 {
		compact = compact[len(compact)-16:]
	}
	return prefix + "_" + compact, nil
}

func harnessTurnID(ctx *storage.SpanContext, traceID string) string {
	if ctx != nil && strings.TrimSpace(ctx.TurnID) != "" {
		return strings.TrimSpace(ctx.TurnID)
	}
	return "turn_" + traceID
}

func firstUserPrompt(req *llm.ChatRequest) string {
	if req == nil {
		return ""
	}
	for _, msg := range req.Messages {
		if msg.Role == "user" {
			return strings.TrimSpace(msg.GetText())
		}
	}
	return ""
}

func responseText(resp *llm.ChatResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.Message.GetText())
}

func llmInputMessages(req *llm.ChatRequest) []llm.Message {
	if req == nil {
		return nil
	}
	messages := make([]llm.Message, 0, len(req.Messages)+1)
	if strings.TrimSpace(req.System) != "" {
		messages = append(messages, llm.NewTextMessage("system", req.System))
	}
	messages = append(messages, req.Messages...)
	return messages
}

func responseStatus(resp *llm.ChatResponse) string {
	if resp == nil {
		return "error"
	}
	if strings.Contains(strings.ToLower(resp.StopReason), "error") {
		return "error"
	}
	return "ok"
}

func usageTokenDeltas(usage *llm.Usage) (int64, int64) {
	if usage == nil {
		return 0, 0
	}
	return int64(usage.PromptTokens), int64(usage.CompletionTokens)
}

func usageDuration(usage *llm.Usage) int64 {
	if usage == nil || usage.TotalDurationNs < 0 {
		return 0
	}
	return usage.TotalDurationNs
}

func usageMetrics(usage *llm.Usage, costUSD float64) map[string]any {
	m := map[string]any{"estimated_total_cost_usd": costUSD}
	if usage == nil {
		return m
	}
	m["input_tokens"] = usage.PromptTokens
	m["output_tokens"] = usage.CompletionTokens
	m["total_tokens"] = usage.TotalTokens
	m["cache_write_input_tokens"] = usage.CacheCreationInputTokens
	m["cache_read_input_tokens"] = usage.CacheReadInputTokens
	m["total_duration_ns"] = usage.TotalDurationNs
	m["prompt_duration_ns"] = usage.PromptDurationNs
	return m
}

func agentSpanName(agentName string) string {
	if strings.TrimSpace(agentName) == "" {
		return "agent-request"
	}
	return agentName + "-request"
}

// ListTraceRecords returns recent span-model turns for the supplied org.
func (d *Driver) ListTraceRecords(ctx context.Context, orgID string, limit int, cursorTs *time.Time, cursorTraceID *string) ([]storage.TraceRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list traces org_id: %w", err)
	}
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}
	var ts any
	var tid any
	if cursorTs != nil && cursorTraceID != nil && *cursorTraceID != "" {
		ts = *cursorTs
		tid = *cursorTraceID
	}
	rows, err := d.conn.Query(ctx, `
SELECT t.id::text, t.org_id::text, t.session_id::text, t.trace_id,
       COALESCE(t.harness_turn_id, ''), COALESCE(t.user_prompt, ''), t.status,
       t.started_at, t.ended_at, t.duration_ns, t.total_input_tokens,
       t.total_output_tokens, t.total_cost_usd::float8, t.metadata,
       s.harness_id, s.harness_session_id, COALESCE(s.name, ''), COALESCE(s.cwd, ''),
       COUNT(sp.id)::int
  FROM span_turns t
  JOIN sessions s ON s.id = t.session_id
  LEFT JOIN spans sp ON sp.turn_id = t.id
 WHERE t.org_id = $1
   AND ($2::timestamptz IS NULL OR t.started_at < $2::timestamptz OR (t.started_at = $2::timestamptz AND t.trace_id < $3::text))
 GROUP BY t.id, s.id
 ORDER BY t.started_at DESC, t.trace_id DESC
 LIMIT $4`, oid, ts, tid, int32(limit))
	if err != nil {
		return nil, fmt.Errorf("list traces: %w", err)
	}
	defer rows.Close()
	var out []storage.TraceRecord
	for rows.Next() {
		rec, err := scanTraceRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// GetTrace returns one trace plus all spans and links in chronological order.
func (d *Driver) GetTrace(ctx context.Context, orgID, traceID string) (*storage.TraceRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get trace org_id: %w", err)
	}
	row := d.conn.QueryRow(ctx, `
SELECT t.id::text, t.org_id::text, t.session_id::text, t.trace_id,
       COALESCE(t.harness_turn_id, ''), COALESCE(t.user_prompt, ''), t.status,
       t.started_at, t.ended_at, t.duration_ns, t.total_input_tokens,
       t.total_output_tokens, t.total_cost_usd::float8, t.metadata,
       s.harness_id, s.harness_session_id, COALESCE(s.name, ''), COALESCE(s.cwd, ''),
       (SELECT COUNT(*)::int FROM spans sp WHERE sp.turn_id = t.id)
  FROM span_turns t
  JOIN sessions s ON s.id = t.session_id
 WHERE t.org_id = $1 AND t.trace_id = $2`, oid, traceID)
	rec, err := scanTraceRecord(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, nil, nil
		}
		return nil, nil, nil, fmt.Errorf("get trace: %w", err)
	}
	spans, err := d.listSpansForTrace(ctx, oid, traceID)
	if err != nil {
		return nil, nil, nil, err
	}
	links, err := d.listLinksForTrace(ctx, oid, traceID)
	if err != nil {
		return nil, nil, nil, err
	}
	return &rec, spans, links, nil
}

type traceScanner interface {
	Scan(dest ...any) error
}

func scanTraceRecord(row traceScanner) (storage.TraceRecord, error) {
	var rec storage.TraceRecord
	var ended pgtype.Timestamptz
	var metadata []byte
	if err := row.Scan(&rec.ID, &rec.OrgID, &rec.SessionID, &rec.TraceID, &rec.HarnessTurnID, &rec.UserPrompt, &rec.Status, &rec.StartedAt, &ended, &rec.DurationNS, &rec.TotalInputTokens, &rec.TotalOutputTokens, &rec.TotalCostUSD, &metadata, &rec.HarnessID, &rec.HarnessSessionID, &rec.Name, &rec.Cwd, &rec.SpanCount); err != nil {
		return rec, err
	}
	if ended.Valid {
		t := ended.Time
		rec.EndedAt = &t
	}
	if len(metadata) > 0 {
		_ = json.Unmarshal(metadata, &rec.Metadata)
	}
	return rec, nil
}

func (d *Driver) listSpansForTrace(ctx context.Context, orgID pgtype.UUID, traceID string) ([]storage.SpanRecord, error) {
	rows, err := d.conn.Query(ctx, `
SELECT id::text, session_id::text, turn_id::text, trace_id, span_id,
       COALESCE(parent_span_id, ''), kind, name, status, start_ns, duration_ns,
       input, output, metadata, metrics, raw
  FROM spans
 WHERE org_id = $1 AND trace_id = $2
 ORDER BY start_ns ASC, id ASC`, orgID, traceID)
	if err != nil {
		return nil, fmt.Errorf("list trace spans: %w", err)
	}
	defer rows.Close()
	var out []storage.SpanRecord
	for rows.Next() {
		var rec storage.SpanRecord
		if err := rows.Scan(&rec.ID, &rec.SessionID, &rec.TurnID, &rec.TraceID, &rec.SpanID, &rec.ParentSpanID, &rec.Kind, &rec.Name, &rec.Status, &rec.StartNS, &rec.DurationNS, &rec.Input, &rec.Output, &rec.Metadata, &rec.Metrics, &rec.Raw); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (d *Driver) listLinksForTrace(ctx context.Context, orgID pgtype.UUID, traceID string) ([]storage.SpanLinkRecord, error) {
	rows, err := d.conn.Query(ctx, `
SELECT trace_id, from_span_id, to_span_id, COALESCE(from_io, ''), COALESCE(to_io, ''), metadata
  FROM span_links
 WHERE org_id = $1 AND trace_id = $2
 ORDER BY created_at ASC`, orgID, traceID)
	if err != nil {
		return nil, fmt.Errorf("list trace links: %w", err)
	}
	defer rows.Close()
	var out []storage.SpanLinkRecord
	for rows.Next() {
		var rec storage.SpanLinkRecord
		if err := rows.Scan(&rec.TraceID, &rec.FromSpanID, &rec.ToSpanID, &rec.FromIO, &rec.ToIO, &rec.Metadata); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

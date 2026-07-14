package api

import (
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Lazy trace browsing — the v2 read surface. Session detail loads turn
// summaries; spans arrive per trace on expand; full payloads per span
// on demand. Initial paint is O(turns), not O(session).

// TraceListResponse is the summaries list for one session.
type TraceListResponse struct {
	Items []TraceItem `json:"items"`
}

// RawTurnHeaderItem is one wire-log row: what crossed the wire (or
// arrived as a transcript push), without the payload blobs. The
// `source` field is the wire-vs-transcript distinction.
type RawTurnHeaderItem struct {
	ID            int64           `json:"id"`
	Source        string          `json:"source"`
	Provider      string          `json:"provider,omitempty"`
	AgentName     string          `json:"agent_name,omitempty"`
	RequestID     string          `json:"request_id,omitempty"`
	ReceivedAt    time.Time       `json:"received_at"`
	Meta          json.RawMessage `json:"meta,omitempty"`
	RequestBytes  int64           `json:"request_bytes"`
	ResponseBytes int64           `json:"response_bytes"`
}

// RawTurnListResponse is a session's wire log.
type RawTurnListResponse struct {
	Items []RawTurnHeaderItem `json:"items"`
}

func traceItemFromTurn(turn storage.SpanTurnRecord, spanCount int) TraceItem {
	meta := map[string]any{}
	if turn.Synthetic != "" {
		meta["synthetic"] = turn.Synthetic
	}
	return TraceItem{
		ID:                  turn.TraceID,
		TraceID:             turn.TraceID,
		SessionID:           turn.SessionID,
		UserPrompt:          turn.UserPrompt,
		ResponsePreview:     turn.ResponsePreview,
		Status:              turn.Status,
		StartedAt:           turn.StartedAt,
		EndedAt:             turn.EndedAt,
		DurationNS:          turn.DurationNS,
		TotalInputTokens:    turn.TotalInputTokens,
		TotalOutputTokens:   turn.TotalOutputTokens,
		MainInputTokens:     turn.MainInputTokens,
		MainOutputTokens:    turn.MainOutputTokens,
		CacheReadTokens:     turn.CacheReadTokens,
		CacheCreationTokens: turn.CacheCreationTokens,
		TotalCostUSD:        turn.TotalCostUSD,
		SpanCount:           spanCount,
		Metadata:            meta,
	}
}

// handleListTraceSummaries handles GET /v1/traces?session_id=.
//
//	@Summary		List a session's traces (summaries)
//	@ID			listTraces
//	@Description	Returns turn headers for a session — no span payloads. Fetch GET /v1/traces/{trace_id} per turn for spans and links.
//	@Tags			traces
//	@Produce		json
//	@Param			session_id	query		string	true	"Session id (UUID)"
//	@Success		200			{object}	TraceListResponse
//	@Failure		400			{object}	llm.ErrorResponse
//	@Failure		500			{object}	llm.ErrorResponse
//	@Failure		501			{object}	llm.ErrorResponse
//	@Router			/v1/traces [get]
func (s *Server) handleListTraceSummaries(c *fiber.Ctx) error {
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	sessionID := c.Query("session_id")
	if sessionID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "session_id parameter required"})
	}
	if _, err := uuid.Parse(sessionID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "session_id must be a valid UUID"})
	}
	orgID := orgIDFromCtx(c)
	sess, err := sessions.GetSessionRecord(c.Context(), orgID, sessionID)
	if err != nil {
		s.logger.Error("get session for trace summaries", "session_id", sessionID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}
	rows, err := reader.ListTraceSummaries(c.Context(), sessionID)
	if err != nil {
		s.logger.Error("list trace summaries", "session_id", sessionID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list traces"})
	}
	return c.JSON(BuildTraceList(rows))
}

// BuildTraceList renders the turn-summary rows for one session.
// Exported so `tapes dev trace-fixtures` emits byte-identical JSON to
// the handler.
func BuildTraceList(rows []storage.TraceSummaryRecord) TraceListResponse {
	items := make([]TraceItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, traceItemFromTurn(row.SpanTurnRecord, row.SpanCount))
	}
	return TraceListResponse{Items: items}
}

// handleGetTrace handles GET /v1/traces/:trace_id.
//
//	@Summary		Get one trace with spans and links
//	@ID			getTrace
//	@Description	Returns one user-visible turn: its spans nested by parent_span_id and its dataflow links (links touching other traces included).
//	@Tags			traces
//	@Produce		json
//	@Param			trace_id	path		string	true	"Trace id"
//	@Param			payload		query		string	false	"Span payload mode: full (default) or preview (strings truncated; fetch the span endpoint for full payloads)"
//	@Success		200			{object}	TraceDetail
//	@Failure		404			{object}	llm.ErrorResponse
//	@Failure		500			{object}	llm.ErrorResponse
//	@Failure		501			{object}	llm.ErrorResponse
//	@Router			/v1/traces/{trace_id} [get]
func (s *Server) handleGetTrace(c *fiber.Ctx) error {
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	traceID := c.Params("trace_id")
	turn, spans, links, err := reader.GetTraceDetail(c.Context(), orgIDFromCtx(c), traceID)
	if err != nil {
		s.logger.Error("get trace", "trace_id", traceID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get trace"})
	}
	if turn == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "trace not found"})
	}
	return c.JSON(BuildTraceDetail(*turn, spans, links, payloadModeFromQuery(c.Query("payload"))))
}

// BuildTraceDetail renders one turn with its spans and links. Exported
// so `tapes dev trace-fixtures` emits byte-identical JSON to the
// handler.
func BuildTraceDetail(turn storage.SpanTurnRecord, spans []storage.SpanRecord, links []storage.SpanLinkRecord, mode PayloadMode) TraceDetail {
	children := map[string][]string{}
	for _, sp := range spans {
		if sp.ParentSpanID != "" {
			children[sp.ParentSpanID] = append(children[sp.ParentSpanID], sp.SpanID)
		}
	}
	detail := TraceDetail{
		Trace: traceItemFromTurn(turn, len(spans)),
		Spans: make([]SpanItem, 0, len(spans)),
		Links: make([]SpanLinkItem, 0, len(links)),
	}
	for _, sp := range spans {
		detail.Spans = append(detail.Spans, spanItemFromRecord(sp, children[sp.SpanID], mode))
	}
	for _, l := range links {
		detail.Links = append(detail.Links, SpanLinkItem{
			FromTraceID: l.FromTraceID, FromSpanID: l.FromSpanID, FromIO: l.FromIO,
			ToTraceID: l.ToTraceID, ToSpanID: l.ToSpanID, ToIO: l.ToIO,
			Metadata: map[string]any{"kind": l.Kind},
		})
	}
	return detail
}

// handleGetSpan handles GET /v1/traces/:trace_id/spans/:span_id.
//
//	@Summary		Get one span with full payloads
//	@ID			getSpan
//	@Description	The payload drill-in: one span's complete input/output content.
//	@Tags			traces
//	@Produce		json
//	@Param			trace_id	path		string	true	"Trace id"
//	@Param			span_id		path		string	true	"Span id"
//	@Success		200			{object}	SpanItem
//	@Failure		404			{object}	llm.ErrorResponse
//	@Failure		500			{object}	llm.ErrorResponse
//	@Failure		501			{object}	llm.ErrorResponse
//	@Router			/v1/traces/{trace_id}/spans/{span_id} [get]
func (s *Server) handleGetSpan(c *fiber.Ctx) error {
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	traceID, spanID := c.Params("trace_id"), c.Params("span_id")
	rec, err := reader.GetSpanRecord(c.Context(), orgIDFromCtx(c), traceID, spanID)
	if err != nil {
		s.logger.Error("get span", "trace_id", traceID, "span_id", spanID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get span"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "span not found"})
	}
	item := spanItemFromRecord(*rec, nil, PayloadFull)
	return c.JSON(item)
}

// handleListSessionRawTurns handles GET /v1/sessions/:id/raw_turns.
//
//	@Summary		List a session's raw capture log (operator)
//	@ID			listRawTurns
//	@Description	The raw layer's wire log: one row per captured call or transcript push, identity and sizes only. `source` distinguishes what crossed the wire from what the harness pushed as its own account.
//	@Tags			sessions
//	@Produce		json
//	@Param			id	path		string	true	"Session id (UUID)"
//	@Success		200	{object}	RawTurnListResponse
//	@Failure		400	{object}	llm.ErrorResponse
//	@Failure		404	{object}	llm.ErrorResponse
//	@Failure		500	{object}	llm.ErrorResponse
//	@Failure		501	{object}	llm.ErrorResponse
//	@Router			/v1/sessions/{id}/raw_turns [get]
func (s *Server) handleListSessionRawTurns(c *fiber.Ctx) error {
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "raw turn log not supported by this backend"})
	}
	id := c.Params("id")
	if _, err := uuid.Parse(id); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
	}
	orgID := orgIDFromCtx(c)
	sess, err := sessions.GetSessionRecord(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get session for raw turns", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}
	rows, err := reader.ListRawTurnHeaders(c.Context(), orgID, sess.HarnessID, sess.HarnessSessionID)
	if err != nil {
		s.logger.Error("list raw turn headers", "session_id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list raw turns"})
	}
	items := make([]RawTurnHeaderItem, 0, len(rows))
	for _, r := range rows {
		items = append(items, RawTurnHeaderItem{
			ID: r.ID, Source: r.Source, Provider: r.Provider,
			AgentName: r.AgentName, RequestID: r.RequestID,
			ReceivedAt: r.ReceivedAt, Meta: r.Meta,
			RequestBytes: r.RequestBytes, ResponseBytes: r.ResponseBytes,
		})
	}
	return c.JSON(RawTurnListResponse{Items: items})
}

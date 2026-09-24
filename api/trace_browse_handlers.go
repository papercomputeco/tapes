package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Lazy trace browsing — the v2 read surface. Session detail loads turn
// summaries; spans arrive per trace on expand; full payloads per span
// on demand. Initial paint is O(turns), not O(session).

// TraceListResponse is the summaries list for one session. `schema`
// stamps the projection generation the rows were derived against — the
// same stamp the composite carries — so every trace-grain response is
// self-describing, not just the composite.
type TraceListResponse struct {
	Schema string      `json:"schema"`
	Items  []TraceItem `json:"items"`
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
	Meta          json.RawMessage `json:"meta,omitempty" oas:"type=object"`
	RequestBytes  int64           `json:"request_bytes"`
	ResponseBytes int64           `json:"response_bytes"`
}

// RawTurnListResponse is a session's wire log.
type RawTurnListResponse struct {
	Items []RawTurnHeaderItem `json:"items"`
}

func traceItemFromTurn(turn storage.SpanTurnRecord, spanCount int) TraceItem {
	return TraceItem{
		TraceID:         turn.TraceID,
		UserPrompt:      turn.UserPrompt,
		ResponsePreview: turn.ResponsePreview,
		Status:          turn.Status,
		Source:          turn.Source,
		StartedAt:       turn.StartedAt,
		EndedAt:         turn.EndedAt,
		DurationNS:      turn.DurationNS,
		SpanCount:       spanCount,
		Usage: TraceUsage{
			InputTokens:         turn.TotalInputTokens,
			OutputTokens:        turn.TotalOutputTokens,
			CacheReadTokens:     turn.CacheReadTokens,
			CacheCreationTokens: turn.CacheCreationTokens,
			CostUSD:             turn.TotalCostUSD,
		},
		MainUsage: MainUsage{
			InputTokens:  turn.MainInputTokens,
			OutputTokens: turn.MainOutputTokens,
		},
		Synthetic: turn.Synthetic,
	}
}

// handleListTraceSummaries handles GET /v1/traces?session_id=.
func (s *Server) handleListTraceSummaries(c fiber.Ctx) error {
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
	orgID := singleTenantOrgID
	sess, err := sessions.GetSessionRecord(c.RequestCtx(), orgID, sessionID)
	if err != nil {
		s.logger.Error("get session for trace summaries", "session_id", sessionID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}
	rows, err := reader.ListTraceSummaries(c.RequestCtx(), sessionID)
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
	return TraceListResponse{Schema: ProjectionSchema, Items: items}
}

// parseTraceSpansLimit reads the trace page's `limit` query: the number
// of spans a page holds, defaulting to 200 and clamped to 1000. Anything
// that is not a positive integer is rejected rather than defaulted.
func parseTraceSpansLimit(raw string) (int, error) {
	if raw == "" {
		return defaultTraceSpansLimit, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	return min(parsed, maxTraceSpansLimit), nil
}

// handleGetTrace handles GET /v1/traces/:trace_id.
func (s *Server) handleGetTrace(c fiber.Ctx) error {
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	traceID := c.Params("trace_id")

	limit, err := parseTraceSpansLimit(c.Query("limit"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	var after storage.SpanCursor
	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeTracePageCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		// A cursor is a boundary in one trace's span order; presented
		// with another trace it is a malformed request, not a transition.
		if cur.TraceID != traceID {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "cursor does not match trace"})
		}
		after = cur.spanCursor()
	}

	// Everything that can still turn into an error response is loaded
	// here, before the status is committed: the payload-free turn header
	// (with the span count the trace header carries ahead of its spans)
	// and the links touching the trace. The spans themselves stream.
	orgID := singleTenantOrgID
	turn, err := reader.GetTraceSummary(c.RequestCtx(), orgID, traceID)
	if err != nil {
		s.logger.Error("get trace", "trace_id", traceID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get trace"})
	}
	if turn == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "trace not found"})
	}
	links, err := reader.ListTraceLinks(c.RequestCtx(), orgID, traceID)
	if err != nil {
		s.logger.Error("list trace links", "trace_id", traceID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get trace"})
	}

	page := &tracePage{
		orgID:   orgID,
		traceID: traceID,
		turn:    *turn,
		links:   links,
		mode:    payloadModeFromQuery(c.Query("payload")),
		after:   after,
		limit:   limit,
		budget:  tracesPageByteBudget,
		spans:   reader,
	}

	// The body is written by a goroutine after this handler returns, so
	// the ctx is abandoned rather than recycled underneath it, and the
	// goroutine takes only what it needs from it now: the user context,
	// which is the one signal that can tell it to stop. Compression is
	// left to the middleware, which wraps a body stream incrementally.
	ctx := c.Context()
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	c.Abandon()
	return c.SendStreamWriter(func(w *bufio.Writer) {
		if err := page.write(ctx, w); err != nil {
			s.logger.Warn("trace stream cut short", "trace_id", traceID, "error", err)
		}
	})
}

// BuildTraceDetail renders one turn with its spans and links whole. The
// handler no longer calls it — it streams a page through tracePage — but
// this remains the reference shape: a page that holds the whole trace is
// byte-identical to json.Marshal of this, which `tapes dev trace-fixtures`
// relies on and the page specs prove.
func BuildTraceDetail(turn storage.SpanTurnRecord, spans []storage.SpanRecord, links []storage.SpanLinkRecord, mode PayloadMode) TraceDetail {
	detail := TraceDetail{
		Schema: ProjectionSchema,
		Trace:  traceItemFromTurn(turn, len(spans)),
		Spans:  make([]SpanItem, 0, len(spans)),
		Links:  make([]SpanLinkItem, 0, len(links)),
	}
	for _, sp := range spans {
		detail.Spans = append(detail.Spans, spanItemFromRecord(sp, mode))
	}
	for _, l := range links {
		detail.Links = append(detail.Links, spanLinkItem(l))
	}
	return detail
}

// StandaloneTraceDetail is the standalone trace lookup's response: a
// TraceDetail plus the owning session, which THIS caller — unlike the
// session-scoped composite's — does not already know and needs to
// navigate. A dedicated type (rather than an optional field on
// TraceDetail) lets generated clients see the guarantee as required
// here, while the composite schema advertises no field its payloads
// never emit.
type StandaloneTraceDetail struct {
	SessionID string `json:"session_id" oas:"required"`
	TraceDetail
}

// handleGetSpan handles GET /v1/traces/:trace_id/spans/:span_id.
func (s *Server) handleGetSpan(c fiber.Ctx) error {
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	traceID, spanID := c.Params("trace_id"), c.Params("span_id")
	rec, err := reader.GetSpanRecord(c.RequestCtx(), singleTenantOrgID, traceID, spanID)
	if err != nil {
		s.logger.Error("get span", "trace_id", traceID, "span_id", spanID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get span"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "span not found"})
	}
	item := spanItemFromRecord(*rec, PayloadFull)
	return c.JSON(item)
}

// handleListSessionRawTurns handles GET /v1/sessions/:id/raw_turns.
func (s *Server) handleListSessionRawTurns(c fiber.Ctx) error {
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
	orgID := singleTenantOrgID
	sess, err := sessions.GetSessionRecord(c.RequestCtx(), orgID, id)
	if err != nil {
		s.logger.Error("get session for raw turns", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}
	rows, err := reader.ListRawTurnHeaders(c.RequestCtx(), orgID, sess.HarnessID, sess.HarnessSessionID)
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

package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

type traceReader interface {
	ListTraceRecords(ctx context.Context, orgID string, limit int, cursorTs *time.Time, cursorTraceID *string) ([]storage.TraceRecord, error)
	GetTrace(ctx context.Context, orgID, traceID string) (*storage.TraceRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error)
}

const (
	defaultTracesLimit = 50
	maxTracesLimit     = 200
)

type TraceItem struct {
	ID                string         `json:"id"`
	TraceID           string         `json:"trace_id"`
	SessionID         string         `json:"session_id"`
	HarnessID         string         `json:"harness_id"`
	HarnessSessionID  string         `json:"harness_session_id"`
	Name              string         `json:"name,omitempty"`
	Cwd               string         `json:"cwd,omitempty"`
	UserPrompt        string         `json:"user_prompt,omitempty"`
	Status            string         `json:"status"`
	StartedAt         time.Time      `json:"started_at"`
	EndedAt           *time.Time     `json:"ended_at,omitempty"`
	DurationNS        int64          `json:"duration_ns"`
	TotalInputTokens  int64          `json:"total_input_tokens"`
	TotalOutputTokens int64          `json:"total_output_tokens"`
	TotalCostUSD      float64        `json:"total_cost_usd"`
	SpanCount         int            `json:"span_count"`
	Metadata          map[string]any `json:"metadata,omitempty"`
}

type TraceListResponse struct {
	Items      []TraceItem `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

type SpanItem struct {
	ID           string          `json:"id"`
	TraceID      string          `json:"trace_id"`
	SpanID       string          `json:"span_id"`
	ParentSpanID string          `json:"parent_span_id,omitempty"`
	Kind         string          `json:"kind"`
	Name         string          `json:"name"`
	Status       string          `json:"status"`
	StartNS      int64           `json:"start_ns"`
	DurationNS   int64           `json:"duration_ns"`
	Input        json.RawMessage `json:"input"`
	Output       json.RawMessage `json:"output"`
	Metadata     json.RawMessage `json:"metadata"`
	Metrics      json.RawMessage `json:"metrics"`
	Raw          json.RawMessage `json:"raw,omitempty"`
	ChildrenIDs  []string        `json:"children_ids,omitempty"`
}

type SpanLinkItem struct {
	TraceID    string          `json:"trace_id"`
	FromSpanID string          `json:"from_span_id"`
	ToSpanID   string          `json:"to_span_id"`
	FromIO     string          `json:"from_io,omitempty"`
	ToIO       string          `json:"to_io,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
}

type TraceDetailResponse struct {
	Trace TraceItem      `json:"trace"`
	Spans []SpanItem     `json:"spans"`
	Links []SpanLinkItem `json:"links"`
}

type tracesCursor struct {
	StartedAt time.Time `json:"ts"`
	TraceID   string    `json:"trace_id"`
}

func encodeTracesCursor(c tracesCursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("encoding traces cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeTracesCursor(token string) (tracesCursor, error) {
	if token == "" {
		return tracesCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return tracesCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c tracesCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return tracesCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if c.TraceID == "" {
		return tracesCursor{}, errors.New("invalid cursor: missing trace_id")
	}
	return c, nil
}

// handleListTraces handles GET /v1/traces.
//
//	@Summary		List span traces
//	@Description	Returns Lapdog-style span traces (one row per user turn), newest first. This is the experimental span read model; prompt snapshots live on child spans rather than Merkle node identity.
//	@Tags			traces
//	@Produce		json
//	@Param			limit	query		int		false	"Maximum number of traces to return (default 50, max 200)"
//	@Param			cursor	query		string	false	"Opaque pagination cursor returned by a previous response"
//	@Success		200		{object}	TraceListResponse
//	@Failure		400		{object}	llm.ErrorResponse
//	@Failure		500		{object}	llm.ErrorResponse
//	@Failure		501		{object}	llm.ErrorResponse
//	@Router			/v1/traces [get]
func (s *Server) handleListTraces(c *fiber.Ctx) error {
	reader, ok := s.driver.(traceReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	limit := defaultTracesLimit
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "limit must be a positive integer"})
		}
		if parsed > maxTracesLimit {
			parsed = maxTracesLimit
		}
		limit = parsed
	}
	var cursorTs *time.Time
	var cursorTraceID *string
	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeTracesCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		cursorTs = &cur.StartedAt
		cursorTraceID = &cur.TraceID
	}
	rows, err := reader.ListTraceRecords(c.Context(), orgIDFromCtx(c), limit+1, cursorTs, cursorTraceID)
	if err != nil {
		s.logger.Error("list traces", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list traces"})
	}
	var nextCursor string
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		nextCursor = encodeTracesCursor(tracesCursor{StartedAt: last.StartedAt, TraceID: last.TraceID})
	}
	items := make([]TraceItem, len(rows))
	for i, row := range rows {
		items[i] = traceItemFromStorage(row)
	}
	return c.JSON(TraceListResponse{Items: items, NextCursor: nextCursor})
}

// handleGetTrace handles GET /v1/traces/:trace_id.
//
//	@Summary		Get a span trace
//	@Description	Returns one Lapdog-style trace with parent/child span tree and causal span links.
//	@Tags			traces
//	@Produce		json
//	@Param			trace_id	path		string	true	"Trace id"
//	@Success		200			{object}	TraceDetailResponse
//	@Failure		404			{object}	llm.ErrorResponse
//	@Failure		500			{object}	llm.ErrorResponse
//	@Failure		501			{object}	llm.ErrorResponse
//	@Router			/v1/traces/{trace_id} [get]
func (s *Server) handleGetTrace(c *fiber.Ctx) error {
	reader, ok := s.driver.(traceReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	traceID := c.Params("trace_id")
	rec, spans, links, err := reader.GetTrace(c.Context(), orgIDFromCtx(c), traceID)
	if err != nil {
		s.logger.Error("get trace", "trace_id", traceID, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get trace"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "trace not found"})
	}
	return c.JSON(TraceDetailResponse{
		Trace: traceItemFromStorage(*rec),
		Spans: spanItemsFromStorage(spans),
		Links: spanLinksFromStorage(links),
	})
}

func traceItemFromStorage(row storage.TraceRecord) TraceItem {
	return TraceItem{
		ID:                row.ID,
		TraceID:           row.TraceID,
		SessionID:         row.SessionID,
		HarnessID:         row.HarnessID,
		HarnessSessionID:  row.HarnessSessionID,
		Name:              row.Name,
		Cwd:               row.Cwd,
		UserPrompt:        row.UserPrompt,
		Status:            row.Status,
		StartedAt:         row.StartedAt,
		EndedAt:           row.EndedAt,
		DurationNS:        row.DurationNS,
		TotalInputTokens:  row.TotalInputTokens,
		TotalOutputTokens: row.TotalOutputTokens,
		TotalCostUSD:      row.TotalCostUSD,
		SpanCount:         row.SpanCount,
		Metadata:          row.Metadata,
	}
}

func spanItemsFromStorage(rows []storage.SpanRecord) []SpanItem {
	children := map[string][]string{}
	for _, row := range rows {
		if row.ParentSpanID != "" {
			children[row.ParentSpanID] = append(children[row.ParentSpanID], row.SpanID)
		}
	}
	out := make([]SpanItem, len(rows))
	for i, row := range rows {
		out[i] = SpanItem{
			ID:           row.ID,
			TraceID:      row.TraceID,
			SpanID:       row.SpanID,
			ParentSpanID: row.ParentSpanID,
			Kind:         row.Kind,
			Name:         row.Name,
			Status:       row.Status,
			StartNS:      row.StartNS,
			DurationNS:   row.DurationNS,
			Input:        row.Input,
			Output:       row.Output,
			Metadata:     row.Metadata,
			Metrics:      row.Metrics,
			Raw:          row.Raw,
			ChildrenIDs:  children[row.SpanID],
		}
	}
	return out
}

func spanLinksFromStorage(rows []storage.SpanLinkRecord) []SpanLinkItem {
	out := make([]SpanLinkItem, len(rows))
	for i, row := range rows {
		out[i] = SpanLinkItem{
			TraceID:    row.TraceID,
			FromSpanID: row.FromSpanID,
			ToSpanID:   row.ToSpanID,
			FromIO:     row.FromIO,
			ToIO:       row.ToIO,
			Metadata:   row.Metadata,
		}
	}
	return out
}

package api

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Trace read model — the span projection rendered for the console.
// GET /v1/sessions/{id}/traces is the session-detail source: every
// user-visible turn as a trace, spans nested by parent_span_id, and
// dataflow links (cross-trace ones, e.g. compaction seams, at the
// response top level).

type spanModelReader interface {
	storage.SpanModelReader
}

// TraceItem is one user-visible turn's header.
type TraceItem struct {
	ID                string         `json:"id"`
	TraceID           string         `json:"trace_id"`
	SessionID         string         `json:"session_id"`
	HarnessID         string         `json:"harness_id"`
	HarnessSessionID  string         `json:"harness_session_id"`
	UserPrompt        string         `json:"user_prompt,omitempty"`
	Status            string         `json:"status"`
	StartedAt         time.Time      `json:"started_at"`
	EndedAt           *time.Time     `json:"ended_at,omitempty"`
	DurationNS        int64          `json:"duration_ns"`
	TotalInputTokens  int64          `json:"total_input_tokens"`
	TotalOutputTokens int64          `json:"total_output_tokens"`
	TotalCostUSD      float64        `json:"total_cost_usd"`
	SpanCount         int            `json:"span_count"`
	Metadata          map[string]any `json:"metadata"`
}

// SpanItem is one observed unit of work. start_ns is the epoch-ns
// start (kept for the pinned contract); started_at carries the same
// instant losslessly for JS clients, since 1.7e18 exceeds
// Number.MAX_SAFE_INTEGER.
type SpanItem struct {
	ID           string         `json:"id"`
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id,omitempty"`
	Kind         string         `json:"kind"`
	Name         string         `json:"name"`
	Status       string         `json:"status"`
	StartedAt    time.Time      `json:"started_at"`
	StartNS      int64          `json:"start_ns"`
	DurationNS   int64          `json:"duration_ns"`
	Input        map[string]any `json:"input"`
	Output       map[string]any `json:"output"`
	Metadata     map[string]any `json:"metadata"`
	// Metrics is always an object on the wire — the contract fixture
	// pins {} for usage-less spans (agent/tool/event), and the console
	// schema requires it.
	Metrics     json.RawMessage `json:"metrics"`
	ChildrenIDs []string        `json:"children_ids,omitempty"`
}

// SpanLinkItem is a dataflow edge; from/to trace ids differ on
// cross-trace causality.
type SpanLinkItem struct {
	FromTraceID string         `json:"from_trace_id"`
	FromSpanID  string         `json:"from_span_id"`
	FromIO      string         `json:"from_io,omitempty"`
	ToTraceID   string         `json:"to_trace_id"`
	ToSpanID    string         `json:"to_span_id"`
	ToIO        string         `json:"to_io,omitempty"`
	Metadata    map[string]any `json:"metadata"`
}

// TraceDetail is one trace with its spans and intra-trace links.
type TraceDetail struct {
	Trace TraceItem      `json:"trace"`
	Spans []SpanItem     `json:"spans"`
	Links []SpanLinkItem `json:"links"`
}

// SessionTracesResponse is the composite session view on the span
// model.
type SessionTracesResponse struct {
	Session    SessionItem    `json:"session"`
	Tasks      []TreeTask     `json:"tasks"`
	KindCounts map[string]int `json:"kind_counts"`
	Traces     []TraceDetail  `json:"traces"`
	Links      []SpanLinkItem `json:"links"`
}

// handleGetSessionTraces handles GET /v1/sessions/:id/traces.
//
//	@Summary		Get a session's trace/span projection
//	@Description	Returns the session's user-visible turns as traces with nested spans (llm calls, tools, subagents, shadow calls, injected context) and dataflow links. Cross-trace links (compaction seams) are at the response top level.
//	@Tags			sessions
//	@Produce		json
//	@Param			id	path		string	true	"Session id (UUID)"
//	@Success		200	{object}	SessionTracesResponse
//	@Failure		400	{object}	llm.ErrorResponse	"Missing or malformed id"
//	@Failure		404	{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to load session"
//	@Failure		501	{object}	llm.ErrorResponse	"Span traces not supported by this backend"
//	@Router			/v1/sessions/{id}/traces [get]
func (s *Server) handleGetSessionTraces(c *fiber.Ctx) error {
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}
	reader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}
	if _, err := uuid.Parse(id); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
	}

	orgID := orgIDFromCtx(c)
	sess, err := sessions.GetSessionRecord(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get session for traces", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	turns, spans, links, err := reader.ListSessionSpanModel(c.Context(), id)
	if err != nil {
		s.logger.Error("list span model", "session_id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session traces"})
	}

	resp := buildSessionTraces(sessionItemFromStorage(*sess), turns, spans, links)
	return c.JSON(resp)
}

// buildSessionTraces assembles the composite response. Pure rendering:
// every edge and kind here was computed by the deriver.
func buildSessionTraces(
	session SessionItem,
	turns []storage.SpanTurnRecord,
	spans []storage.SpanRecord,
	links []storage.SpanLinkRecord,
) *SessionTracesResponse {
	resp := &SessionTracesResponse{
		Session:    session,
		Tasks:      []TreeTask{},
		KindCounts: map[string]int{},
		Traces:     []TraceDetail{},
		Links:      []SpanLinkItem{},
	}

	spansByTrace := map[string][]storage.SpanRecord{}
	children := map[string][]string{}
	for _, sp := range spans {
		spansByTrace[sp.TraceID] = append(spansByTrace[sp.TraceID], sp)
		if sp.ParentSpanID != "" {
			children[sp.ParentSpanID] = append(children[sp.ParentSpanID], sp.SpanID)
		}
		if sp.CallKind != "" {
			resp.KindCounts[sp.CallKind]++
		}
	}

	linksByTrace := map[string][]SpanLinkItem{}
	for _, l := range links {
		item := SpanLinkItem{
			FromTraceID: l.FromTraceID,
			FromSpanID:  l.FromSpanID,
			FromIO:      l.FromIO,
			ToTraceID:   l.ToTraceID,
			ToSpanID:    l.ToSpanID,
			ToIO:        l.ToIO,
			Metadata:    map[string]any{"kind": l.Kind},
		}
		if l.FromTraceID == l.ToTraceID {
			linksByTrace[l.FromTraceID] = append(linksByTrace[l.FromTraceID], item)
		} else {
			resp.Links = append(resp.Links, item)
		}
	}

	for _, turn := range turns {
		detail := TraceDetail{
			Trace: TraceItem{
				ID:                turn.TraceID,
				TraceID:           turn.TraceID,
				SessionID:         turn.SessionID,
				HarnessID:         session.HarnessID,
				HarnessSessionID:  session.HarnessSessionID,
				UserPrompt:        turn.UserPrompt,
				Status:            turn.Status,
				StartedAt:         turn.StartedAt,
				EndedAt:           turn.EndedAt,
				DurationNS:        turn.DurationNS,
				TotalInputTokens:  turn.TotalInputTokens,
				TotalOutputTokens: turn.TotalOutputTokens,
				TotalCostUSD:      turn.TotalCostUSD,
				SpanCount:         len(spansByTrace[turn.TraceID]),
				Metadata:          map[string]any{},
			},
			Spans: make([]SpanItem, 0, len(spansByTrace[turn.TraceID])),
			Links: linksByTrace[turn.TraceID],
		}
		if detail.Links == nil {
			detail.Links = []SpanLinkItem{}
		}
		if turn.Synthetic != "" {
			detail.Trace.Metadata["synthetic"] = turn.Synthetic
		}
		for _, sp := range spansByTrace[turn.TraceID] {
			detail.Spans = append(detail.Spans, spanItemFromRecord(sp, children[sp.SpanID]))
		}
		resp.Traces = append(resp.Traces, detail)
	}

	resp.Tasks = foldTasksFromSpans(spans)
	return resp
}

// spanItemFromRecord renders one stored span. Tool spans unwrap their
// single tool_use/tool_result block into arguments/output; llm and
// event spans carry their content-block arrays.
func spanItemFromRecord(sp storage.SpanRecord, childIDs []string) SpanItem {
	item := SpanItem{
		ID:           sp.SpanID,
		TraceID:      sp.TraceID,
		SpanID:       sp.SpanID,
		ParentSpanID: sp.ParentSpanID,
		Kind:         sp.Kind,
		Name:         sp.Name,
		Status:       sp.Status,
		StartedAt:    sp.StartedAt,
		StartNS:      sp.StartedAt.UnixNano(),
		DurationNS:   sp.DurationNS,
		Input:        map[string]any{},
		Output:       map[string]any{},
		Metadata:     map[string]any{},
		Metrics:      emptyObjectIfNil(sp.Usage),
		ChildrenIDs:  childIDs,
	}

	if sp.CallKind != "" {
		item.Metadata["call_kind"] = sp.CallKind
	}
	if sp.ThreadID != "" {
		item.Metadata["thread_id"] = sp.ThreadID
	}
	if sp.Model != "" {
		item.Metadata["model"] = sp.Model
	}
	if sp.StopReason != "" {
		item.Metadata["stop_reason"] = sp.StopReason
	}
	if sp.RawTurnID != 0 {
		item.Metadata["raw_turn_id"] = sp.RawTurnID
	}
	if sp.NodeHash != "" {
		item.Metadata["node_hash"] = sp.NodeHash
	}

	switch sp.Kind {
	case "tool":
		if blocks := decodeBlocks(sp.Input); len(blocks) > 0 {
			args := blocks[0].ToolInput
			if args == nil {
				// no-argument tool calls (ExitPlanMode et al.) must
				// still serialize as a record, not null
				args = map[string]any{}
			}
			item.Input["arguments"] = args
		}
		if blocks := decodeBlocks(sp.Output); len(blocks) > 0 {
			item.Output["content"] = blocks[0].ToolOutput
			item.Output["is_error"] = blocks[0].IsError
		}
	default:
		if len(sp.Input) > 0 {
			item.Input["content"] = sp.Input
		}
		if len(sp.Output) > 0 {
			item.Output["content"] = sp.Output
		}
		if strings.HasPrefix(sp.CallKind, "offshoot:permission-check") {
			if v := verdictFromBlocks(sp.CallKind, decodeBlocks(sp.Output)); v != nil {
				item.Metadata["verdict"] = v
			}
		}
	}
	return item
}

// foldTasksFromSpans replays TaskCreate/TaskUpdate from tool spans —
// the same fold as the tree projection, sourced from span rows.
func foldTasksFromSpans(spans []storage.SpanRecord) []TreeTask {
	resultText := map[string]string{}
	var uses []llm.ContentBlock
	for _, sp := range spans {
		if sp.Kind != "tool" {
			continue
		}
		if in := decodeBlocks(sp.Input); len(in) > 0 {
			uses = append(uses, in[0])
		}
		if out := decodeBlocks(sp.Output); len(out) > 0 {
			if _, ok := resultText[sp.SpanID]; !ok {
				resultText[sp.SpanID] = out[0].ToolOutput
			}
		}
	}
	return foldTaskBlocks(uses, resultText)
}

// verdictFromBlocks extracts the security monitor's disposition from a
// permission-check span's output — same tells as verdictFromNode.
func verdictFromBlocks(callKind string, blocks []llm.ContentBlock) *TreeVerdict {
	var text strings.Builder
	for _, b := range blocks {
		if b.Text != "" {
			text.WriteString(b.Text)
		}
	}
	m := blockVerdictPattern.FindStringSubmatch(text.String())
	if m == nil {
		return nil
	}
	v := &TreeVerdict{Disposition: "ALLOW", Stage: 1}
	if strings.EqualFold(m[1], "yes") {
		v.Disposition = "BLOCK"
	}
	if strings.HasSuffix(callKind, "stage2") {
		v.Stage = 2
	}
	if strings.Contains(text.String(), "<thinking>") {
		v.Reasoned = true
	}
	return v
}

// emptyObjectIfNil keeps wire fields object-typed when the stored
// JSONB is NULL.
func emptyObjectIfNil(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("{}")
	}
	return raw
}

// decodeBlocks unmarshals a stored content-block array ("" / null →
// empty).
func decodeBlocks(raw json.RawMessage) []llm.ContentBlock {
	if len(raw) == 0 {
		return nil
	}
	var blocks []llm.ContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil
	}
	return blocks
}

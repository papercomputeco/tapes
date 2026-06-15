package api

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
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

// PayloadMode selects how much span payload a trace response carries.
// Full embeds the stored content verbatim; preview truncates long
// text so list-shaped reads stay O(structure), with the span drill-in
// endpoint serving the full payload on demand.
type PayloadMode string

const (
	PayloadFull    PayloadMode = "full"
	PayloadPreview PayloadMode = "preview"
)

// previewPayloadRunes bounds every string carried by a preview-mode
// payload. Long enough to read, short enough that a whole session of
// previews stays smaller than one full tool result.
const previewPayloadRunes = 512

// payloadModeFromQuery maps the ?payload= query param to a mode;
// anything but "preview" is the full default.
func payloadModeFromQuery(v string) PayloadMode {
	if v == string(PayloadPreview) {
		return PayloadPreview
	}
	return PayloadFull
}

// TraceItem is one user-visible turn's header.
type TraceItem struct {
	ID               string `json:"id"`
	TraceID          string `json:"trace_id"`
	SessionID        string `json:"session_id"`
	HarnessID        string `json:"harness_id"`
	HarnessSessionID string `json:"harness_session_id"`
	UserPrompt       string `json:"user_prompt,omitempty"`
	// ResponsePreview is the derive-time fold of the closing
	// conversation-spine llm call's text output — the answer line for
	// collapsed turn cards, so summary consumers never need spans.
	ResponsePreview   string     `json:"response_preview,omitempty"`
	Status            string     `json:"status"`
	StartedAt         time.Time  `json:"started_at"`
	EndedAt           *time.Time `json:"ended_at,omitempty"`
	DurationNS        int64      `json:"duration_ns"`
	TotalInputTokens  int64      `json:"total_input_tokens"`
	TotalOutputTokens int64      `json:"total_output_tokens"`
	// Main* counts conversation-spine calls only; Total − Main is the
	// harness's shadow spend on the turn.
	MainInputTokens     int64          `json:"main_input_tokens"`
	MainOutputTokens    int64          `json:"main_output_tokens"`
	CacheReadTokens     int64          `json:"cache_read_tokens"`
	CacheCreationTokens int64          `json:"cache_creation_tokens"`
	TotalCostUSD        float64        `json:"total_cost_usd"`
	SpanCount           int            `json:"span_count"`
	Metadata            map[string]any `json:"metadata"`
}

// SpanItem is one observed unit of work. start_ns is the epoch-ns
// start (kept for the pinned contract); started_at carries the same
// instant losslessly for JS clients, since 1.7e18 exceeds
// Number.MAX_SAFE_INTEGER.
type SpanItem struct {
	ID           string    `json:"id"`
	TraceID      string    `json:"trace_id"`
	SpanID       string    `json:"span_id"`
	ParentSpanID string    `json:"parent_span_id,omitempty"`
	Kind         string    `json:"kind"`
	Name         string    `json:"name"`
	Status       string    `json:"status"`
	StartedAt    time.Time `json:"started_at"`
	StartNS      int64     `json:"start_ns"`
	DurationNS   int64     `json:"duration_ns"`
	// Seq is the span's presentation ordinal within its trace; spans
	// arrive sorted by it. start_ns cannot order spans inside one llm
	// call (parallel tool batches share an instant).
	Seq      int64          `json:"seq"`
	Input    map[string]any `json:"input"`
	Output   map[string]any `json:"output"`
	Metadata map[string]any `json:"metadata"`
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
//	@Param			id		path		string	true	"Session id (UUID)"
//	@Param			payload	query		string	false	"Span payload mode: full (default) or preview (strings truncated; fetch the span endpoint for full payloads)"
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

	resp := BuildSessionTraces(sessionItemFromStorage(*sess), turns, spans, links, payloadModeFromQuery(c.Query("payload")))
	return c.JSON(resp)
}

// BuildSessionTraces assembles the composite response. Pure rendering:
// every edge and kind here was computed by the deriver. Exported so
// `tapes dev trace-fixtures` emits byte-identical JSON to the handler.
func BuildSessionTraces(
	session SessionItem,
	turns []storage.SpanTurnRecord,
	spans []storage.SpanRecord,
	links []storage.SpanLinkRecord,
	mode PayloadMode,
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
		item := traceItemFromTurn(turn, len(spansByTrace[turn.TraceID]))
		item.HarnessID = session.HarnessID
		item.HarnessSessionID = session.HarnessSessionID
		detail := TraceDetail{
			Trace: item,
			Spans: make([]SpanItem, 0, len(spansByTrace[turn.TraceID])),
			Links: linksByTrace[turn.TraceID],
		}
		if detail.Links == nil {
			detail.Links = []SpanLinkItem{}
		}
		for _, sp := range spansByTrace[turn.TraceID] {
			detail.Spans = append(detail.Spans, spanItemFromRecord(sp, children[sp.SpanID], mode))
		}
		resp.Traces = append(resp.Traces, detail)
	}

	resp.Tasks = foldTasksFromSpans(spans)
	return resp
}

// spanItemFromRecord renders one stored span. Tool spans unwrap their
// single tool_use/tool_result block into arguments/output; llm and
// event spans carry their content-block arrays. Preview mode truncates
// payload strings and marks the item so clients know to drill in.
func spanItemFromRecord(sp storage.SpanRecord, childIDs []string, mode PayloadMode) SpanItem {
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
		Seq:          sp.Seq,
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
			if mode == PayloadPreview {
				args = previewValue(args).(map[string]any)
			}
			item.Input["arguments"] = args
		}
		if blocks := decodeBlocks(sp.Output); len(blocks) > 0 {
			out := blocks[0].ToolOutput
			if mode == PayloadPreview {
				out = previewString(out)
			}
			item.Output["content"] = out
			item.Output["is_error"] = blocks[0].IsError
		}
	default:
		if len(sp.Input) > 0 {
			item.Input["content"] = payloadContent(sp.Input, mode)
		}
		if len(sp.Output) > 0 {
			item.Output["content"] = payloadContent(sp.Output, mode)
		}
		if strings.HasPrefix(sp.CallKind, "offshoot:permission-check") {
			if v := verdictFromBlocks(sp.CallKind, decodeBlocks(sp.Output)); v != nil {
				item.Metadata["verdict"] = v
			}
		}
	}
	if mode == PayloadPreview {
		item.Metadata["payload"] = string(PayloadPreview)
	}
	return item
}

// payloadContent renders a stored content-block array for the wire. In
// full mode the stored JSON passes through verbatim; preview mode
// decodes, truncates every string, and re-encodes. A blob that fails
// to decode passes through whole rather than silently vanishing.
func payloadContent(raw json.RawMessage, mode PayloadMode) any {
	if mode != PayloadPreview {
		return raw
	}
	blocks := decodeBlocks(raw)
	if blocks == nil {
		return raw
	}
	for i := range blocks {
		b := &blocks[i]
		b.Text = previewString(b.Text)
		b.Thinking = previewString(b.Thinking)
		b.ToolOutput = previewString(b.ToolOutput)
		// Previews never carry image bytes.
		b.ImageBase64 = ""
		if b.ToolInput != nil {
			b.ToolInput = previewValue(b.ToolInput).(map[string]any)
		}
	}
	return blocks
}

// previewString truncates one payload string to the preview bound.
func previewString(s string) string {
	r := []rune(s)
	if len(r) <= previewPayloadRunes {
		return s
	}
	return string(r[:previewPayloadRunes]) + "…"
}

// previewValue truncates every string reachable in a decoded JSON
// value, preserving structure (tool arguments nest arbitrarily).
func previewValue(v any) any {
	switch t := v.(type) {
	case string:
		return previewString(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = previewValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = previewValue(val)
		}
		return out
	default:
		return v
	}
}

// TreeTask is one task folded from the session's TaskCreate/TaskUpdate
// calls.
type TreeTask struct {
	ID          string `json:"id"`
	Subject     string `json:"subject"`
	Description string `json:"description,omitempty"`
	Status      string `json:"status"`
	Updates     int    `json:"updates"`
}

// TreeVerdict is a security-monitor disposition.
type TreeVerdict struct {
	Disposition string `json:"disposition"` // ALLOW | BLOCK
	Stage       int    `json:"stage"`
	Reasoned    bool   `json:"reasoned"`
}

var blockVerdictPattern = regexp.MustCompile(`(?i)<block>\s*(yes|no)`)

// taskCreatedPattern extracts the task id the harness reports back in
// the TaskCreate tool_result ("Task #3 created successfully: …").
var taskCreatedPattern = regexp.MustCompile(`#(\d+)`)

// foldTaskBlocks replays TaskCreate/TaskUpdate tool_use blocks (in
// capture order) against their results. The fold is a function of the
// calls, not of the storage model.
func foldTaskBlocks(uses []llm.ContentBlock, resultText map[string]string) []TreeTask {
	byID := map[string]*TreeTask{}
	var order []*TreeTask
	{
		for _, b := range uses {
			switch b.ToolName {
			case "TaskCreate":
				subject, _ := b.ToolInput["subject"].(string)
				description, _ := b.ToolInput["description"].(string)
				id := ""
				if m := taskCreatedPattern.FindStringSubmatch(resultText[b.ToolUseID]); m != nil {
					id = m[1]
				}
				task := &TreeTask{ID: id, Subject: subject, Description: description, Status: "pending"}
				if id != "" {
					if _, dup := byID[id]; dup {
						continue
					}
					byID[id] = task
				}
				order = append(order, task)
			case "TaskUpdate":
				id, _ := b.ToolInput["taskId"].(string)
				if id == "" {
					if f, ok := b.ToolInput["taskId"].(float64); ok {
						id = strconv.Itoa(int(f))
					}
				}
				task, ok := byID[id]
				if !ok {
					continue
				}
				task.Updates++
				if status, ok := b.ToolInput["status"].(string); ok && status != "" {
					task.Status = status
				}
				if subject, ok := b.ToolInput["subject"].(string); ok && subject != "" {
					task.Subject = subject
				}
			}
		}
	}
	out := make([]TreeTask, 0, len(order))
	for _, t := range order {
		if t.Status == "deleted" {
			continue
		}
		out = append(out, *t)
	}
	return out
}

// foldTasksFromSpans replays TaskCreate/TaskUpdate from tool spans —
// the same fold as the retired tree projection, sourced from span
// rows. The replay must run in event order: storage hands spans back
// sorted by trace_id, which is lexicographic, not chronological.
func foldTasksFromSpans(spans []storage.SpanRecord) []TreeTask {
	tools := make([]storage.SpanRecord, 0, len(spans))
	for _, sp := range spans {
		if sp.Kind == "tool" {
			tools = append(tools, sp)
		}
	}
	sort.SliceStable(tools, func(i, j int) bool {
		if !tools[i].StartedAt.Equal(tools[j].StartedAt) {
			return tools[i].StartedAt.Before(tools[j].StartedAt)
		}
		return tools[i].Seq < tools[j].Seq
	})

	resultText := map[string]string{}
	var uses []llm.ContentBlock
	for _, sp := range tools {
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
// permission-check span's output.
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

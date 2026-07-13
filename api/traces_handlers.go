package api

import (
	"encoding/json"
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

// TraceItem is one user-visible turn's header. session_id / harness ids
// are not duplicated here — they belong to the session; a trace's
// post-compaction status is read from compaction-seam links, not a
// metadata flag.
type TraceItem struct {
	TraceID    string `json:"trace_id"`
	UserPrompt string `json:"user_prompt,omitempty"`
	// ResponsePreview is the derive-time fold of the closing
	// conversation-spine llm call's text output — the answer line for
	// collapsed turn cards, so summary consumers never need spans.
	ResponsePreview string `json:"response_preview,omitempty"`
	Status          string `json:"status"`
	// Source is the capture origin of the turn's rows ("wire" |
	// "transcript"), promoted from raw_turns.source. Per-trace, so a
	// session can mix live wire capture and transcript backfill.
	Source     string     `json:"source"`
	StartedAt  time.Time  `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
	DurationNS int64      `json:"duration_ns"`
	SpanCount  int        `json:"span_count"`
	// Usage is the trace's total token/cost spend over ALL llm spans,
	// shadow calls included; MainUsage is the conversation-spine slice
	// (Usage − MainUsage is the harness's shadow spend on the turn).
	Usage     TraceUsage `json:"usage"`
	MainUsage MainUsage  `json:"main_usage"`
	// Synthetic is a typed deriver signal ("post-compaction" for a
	// compaction continuation, "shadow-opener" for a shadow-only opener),
	// promoted out of the old metadata grab-bag. Absent for genuine
	// prompt-opened turns.
	Synthetic string `json:"synthetic,omitempty"`
}

// TraceUsage is a trace's total token/cost rollup. Fields are pinned
// (no omitempty) so the object shape is uniform across traces.
type TraceUsage struct {
	InputTokens         int64   `json:"input_tokens"`
	OutputTokens        int64   `json:"output_tokens"`
	CacheReadTokens     int64   `json:"cache_read_tokens"`
	CacheCreationTokens int64   `json:"cache_creation_tokens"`
	CostUSD             float64 `json:"cost_usd"`
}

// MainUsage is the conversation-spine token slice of a trace — spine
// calls only, no cache split or cost (those live on the total Usage).
type MainUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// SpanItem is one observed unit of work. Every field is a deriver
// output, formatting-only: the harness-taxonomy fields (call_kind, model,
// stop_reason, thread_id, verdict) are typed rather than bagged in a
// metadata map, and input/output are uniform content-block arrays for
// ALL kinds — the console owns per-kind rendering.
type SpanItem struct {
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	// Seq is the span's presentation ordinal within its trace; spans
	// arrive sorted by it (started_at ties inside one llm call — parallel
	// tool batches share an instant).
	Seq        int64     `json:"seq"`
	Kind       string    `json:"kind"`
	Name       string    `json:"name"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	DurationNS int64     `json:"duration_ns"`
	// Deriver-written taxonomy, promoted from the old metadata grab-bag.
	CallKind   string `json:"call_kind"`
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	ThreadID   string `json:"thread_id"`
	RawTurnID  int64  `json:"raw_turn_id,omitempty"`
	// Verdict is the typed security-monitor disposition (null off
	// permission-check spans), deriver-written.
	Verdict json.RawMessage `json:"verdict"`
	// Input/Output are content-block arrays, uniform for every kind
	// (tool spans included — no unwrapping). Pinned to [] when empty.
	Input  json.RawMessage `json:"input"`
	Output json.RawMessage `json:"output"`
	// Usage (was `metrics`) is always an object on the wire — {}-pinned
	// for usage-less spans.
	Usage json.RawMessage `json:"usage"`
	// Payload marks a preview-truncated span so the console drills in for
	// the full payload; absent in full mode.
	Payload string `json:"payload,omitempty"`
}

// SpanLinkItem is a dataflow edge. kind is a typed top-level field
// (rejoin / verdict / compaction-seam / emits / feeds); from/to trace ids
// differ on cross-trace causality.
type SpanLinkItem struct {
	Kind        string `json:"kind"`
	FromTraceID string `json:"from_trace_id"`
	FromSpanID  string `json:"from_span_id"`
	FromIO      string `json:"from_io,omitempty"`
	ToTraceID   string `json:"to_trace_id"`
	ToSpanID    string `json:"to_span_id"`
	ToIO        string `json:"to_io,omitempty"`
}

// spanLinkItem renders a stored link with its kind as a typed field.
func spanLinkItem(l storage.SpanLinkRecord) SpanLinkItem {
	return SpanLinkItem{
		Kind:        l.Kind,
		FromTraceID: l.FromTraceID,
		FromSpanID:  l.FromSpanID,
		FromIO:      l.FromIO,
		ToTraceID:   l.ToTraceID,
		ToSpanID:    l.ToSpanID,
		ToIO:        l.ToIO,
	}
}

// TraceDetail is one trace with its spans. In the composite session
// response links are session-scoped (top level); the single-trace
// endpoint sets Links to the edges touching that trace.
type TraceDetail struct {
	Trace TraceItem      `json:"trace"`
	Spans []SpanItem     `json:"spans"`
	Links []SpanLinkItem `json:"links,omitempty"`
}

// SessionTracesResponse is the composite session view on the span
// model. `schema` stamps the projection generation the rows were derived
// against, so the presentational shape can version independently.
type SessionTracesResponse struct {
	Schema     string         `json:"schema"`
	Session    SessionItem    `json:"session"`
	Tasks      []TreeTask     `json:"tasks"`
	KindCounts map[string]int `json:"kind_counts"`
	Traces     []TraceDetail  `json:"traces"`
	Links      []SpanLinkItem `json:"links"`
}

// ProjectionSchema is the compatibility date of the derived projection
// generation currently served (the dated *_20260615 table family). It is
// stamped onto the wire `schema` field; a future generation bumps this in
// lockstep with a new dated table family (derived_projection_schemas).
const ProjectionSchema = "2026-06-15"

// handleGetSessionTraces handles GET /v1/sessions/:id/traces.
//
//	@Summary		Get a session's trace/span projection
//	@Description	Returns the session's user-visible turns as traces with nested spans (llm calls, tools, subagents, shadow calls, injected context) and dataflow links. Cross-trace links (compaction seams) are at the response top level.
//	@Tags			sessions
//	@Produce		json
//	@Param			id		path		string	true	"Session id (UUID)"
//	@Param			payload	query		string	false	"Span payload mode: full (default) or preview (strings truncated; fetch the span endpoint for full payloads)"
//	@Success		200		{object}	SessionTracesResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Missing or malformed id"
//	@Failure		404		{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to load session"
//	@Failure		501		{object}	llm.ErrorResponse	"Span traces not supported by this backend"
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
		Schema:     ProjectionSchema,
		Session:    session,
		Tasks:      []TreeTask{},
		KindCounts: map[string]int{},
		Traces:     []TraceDetail{},
		Links:      []SpanLinkItem{},
	}

	spansByTrace := map[string][]storage.SpanRecord{}
	for _, sp := range spans {
		spansByTrace[sp.TraceID] = append(spansByTrace[sp.TraceID], sp)
	}

	// Tasks and kind_counts are deriver-owned session rollups, read from
	// the session record (sessions.tasks / sessions.kind_counts). Empty
	// defaults survive an un-derived or JSON-drifted session.
	if len(session.Tasks) > 0 {
		_ = json.Unmarshal(session.Tasks, &resp.Tasks)
	}
	if len(session.KindCounts) > 0 {
		_ = json.Unmarshal(session.KindCounts, &resp.KindCounts)
	}

	// ALL links live in one flat session-scoped list — containment nests
	// (spans in traces), graph edges don't. An edge may touch one trace
	// (emits/feeds/rejoin/verdict) or two (compaction seams, rejoins).
	for _, l := range links {
		resp.Links = append(resp.Links, spanLinkItem(l))
	}

	for _, turn := range turns {
		detail := TraceDetail{
			Trace: traceItemFromTurn(turn, len(spansByTrace[turn.TraceID])),
			Spans: make([]SpanItem, 0, len(spansByTrace[turn.TraceID])),
		}
		for _, sp := range spansByTrace[turn.TraceID] {
			detail.Spans = append(detail.Spans, spanItemFromRecord(sp, mode))
		}
		resp.Traces = append(resp.Traces, detail)
	}

	return resp
}

// spanItemFromRecord renders one stored span as uniform content-block
// input/output for every kind — no tool unwrapping — with the taxonomy
// fields promoted to typed columns. Preview mode truncates payload
// strings and marks the item so clients drill in for the full payload.
func spanItemFromRecord(sp storage.SpanRecord, mode PayloadMode) SpanItem {
	item := SpanItem{
		TraceID:      sp.TraceID,
		SpanID:       sp.SpanID,
		ParentSpanID: sp.ParentSpanID,
		Seq:          sp.Seq,
		Kind:         sp.Kind,
		Name:         sp.Name,
		Status:       sp.Status,
		StartedAt:    sp.StartedAt,
		DurationNS:   sp.DurationNS,
		CallKind:     sp.CallKind,
		Model:        sp.Model,
		StopReason:   sp.StopReason,
		ThreadID:     sp.ThreadID,
		RawTurnID:    sp.RawTurnID,
		Verdict:      sp.Verdict, // already json.RawMessage; nil → null on the wire
		Input:        contentArray(sp.Input, mode),
		Output:       contentArray(sp.Output, mode),
		Usage:        emptyObjectIfNil(sp.Usage),
	}
	if mode == PayloadPreview {
		item.Payload = string(PayloadPreview)
	}
	return item
}

// contentArray renders a stored content-block array for the wire, pinned
// to [] when empty. Full mode passes the stored JSON through verbatim;
// preview mode truncates every string.
func contentArray(raw json.RawMessage, mode PayloadMode) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("[]")
	}
	if mode != PayloadPreview {
		return raw
	}
	b, err := json.Marshal(payloadContent(raw, mode))
	if err != nil {
		return raw
	}
	return b
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

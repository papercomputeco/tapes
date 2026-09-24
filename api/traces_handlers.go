package api

import (
	"bufio"
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v3"
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
// Full embeds the stored content verbatim; preview serves the
// deriver-written previews (derive.PreviewBlocks: strings truncated,
// image bytes dropped) so list-shaped reads stay O(structure), with the
// span drill-in endpoint serving the full payload on demand. It is the
// storage mode: the same value picks the columns the read selects.
type PayloadMode = storage.PayloadMode

const (
	PayloadFull    = storage.PayloadFull
	PayloadPreview = storage.PayloadPreview
	// PayloadPending marks a preview-mode span whose row carries no stored
	// preview yet (derived before the preview columns existed and not yet
	// backfilled). Its input and output are served as [] rather than
	// computed from the payload — a preview read never touches it.
	PayloadPending PayloadMode = "preview_pending"
)

// payloadModeFromQuery maps the ?payload= query param to a mode;
// anything but "preview" is the full default.
func payloadModeFromQuery(v string) PayloadMode {
	if v == string(PayloadPreview) {
		return PayloadPreview
	}
	return PayloadFull
}

// TraceItem is one user-visible turn's header. session_id / harness ids
// are not duplicated here — they belong to the session. A trace's
// post-compaction status is the typed Synthetic field below (promoted out
// of the old metadata grab-bag); the same seam is also recoverable from
// the session's compaction-seam links.
type TraceItem struct {
	TraceID string `json:"trace_id"`
	// UserPrompt is served explicitly (not omitempty): a synthetic opener
	// has an empty prompt, and dropping the key turns the empty string
	// into `undefined` on the wire, which breaks consumers that expect a
	// string (e.g. the console's stripHarnessTags). Empty means synthetic.
	UserPrompt string `json:"user_prompt"`
	// ResponsePreview is the derive-time fold of the closing
	// conversation-spine llm call's text output — the answer line for
	// collapsed turn cards, so summary consumers never need spans.
	ResponsePreview string `json:"response_preview,omitempty"`
	Status          string `json:"status"`
	// Source is the projection provenance ("wire" | "transcript"). A
	// transcript-only session uses fallback until one usable wire call exists;
	// then its complete projection is wire-derived and transcripts only
	// reconcile structure. Spans inherit this trace-level provenance.
	Source     string     `json:"source"`
	StartedAt  time.Time  `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
	DurationNS int64      `json:"duration_ns"`
	SpanCount  int        `json:"span_count"`
	// Usage is the trace's total token/cost spend over ALL llm spans,
	// shadow calls included; MainUsage is the task slice — the main agent
	// and its subagents (every call_kind=main span, across threads). The
	// difference (Usage − MainUsage) is the harness's shadow spend
	// (permission checks, title-gen, web summaries) on the turn.
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

// MainUsage is the task token slice of a trace: the main agent and its
// subagents (call_kind=main across every thread), no cache split or cost
// (those live on the total Usage). Deliberately not spine-only — a
// subagent doing the user's work is task spend, not shadow.
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
	// permission-check spans), deriver-written. It is a Verdict object or
	// null on the wire; the oas tag states that, because a json.RawMessage
	// carries no shape a reflector could recover.
	Verdict json.RawMessage `json:"verdict" oas:"type=object,nullable"`
	// Input/Output are content-block arrays (llm.ContentBlock), uniform for
	// every kind (tool spans included — no unwrapping). Pinned to [] when
	// empty.
	Input  json.RawMessage `json:"input" oas:"type=array:object"`
	Output json.RawMessage `json:"output" oas:"type=array:object"`
	// Usage (was `metrics`) is an llm.Usage object on the wire — {}-pinned
	// for usage-less spans.
	Usage json.RawMessage `json:"usage" oas:"type=object"`
	// Payload marks a preview-mode span so the console drills in for the
	// full payload: "preview" when input/output are the stored previews,
	// "preview_pending" when the row has no stored preview yet (input and
	// output are then []). Absent in full mode.
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
// endpoint sets Links to the edges touching that trace. `schema` stamps
// the projection generation on the STANDALONE /v1/traces/{id} response
// (omitempty — the composite embeds TraceDetail and already carries one
// stamp at the top level, so the embedded copies stay unstamped).
type TraceDetail struct {
	Schema string         `json:"schema,omitempty"`
	Trace  TraceItem      `json:"trace"`
	Spans  []SpanItem     `json:"spans"`
	Links  []SpanLinkItem `json:"links,omitempty"`
	// NextCursor continues the STANDALONE /v1/traces/{id} walk from the
	// last span of this page (pass it as `cursor`). Absent once the page
	// reached the trace's last span, and always absent on the copies the
	// composite embeds — there a trace is served whole, so the field
	// never appears on that wire.
	NextCursor string `json:"next_cursor,omitempty"`
}

// SessionTracesResponse is the composite session view on the span
// model — one page of it. `schema` stamps the projection generation the
// rows were derived against, so the presentational shape can version
// independently. The page is bounded in traces (`limit`) and in bytes;
// `session` and `links` are whole on every page.
type SessionTracesResponse struct {
	Schema  string         `json:"schema"`
	Session SessionItem    `json:"session"`
	Traces  []TraceDetail  `json:"traces"`
	Links   []SpanLinkItem `json:"links"`
	// NextCursor continues the walk from the last trace of this page
	// (pass it as `cursor`). Absent once the page reached the session's
	// last trace. A page may close short of `limit` on its byte budget, so
	// its absence — not the page's length — is what means "no more".
	NextCursor string `json:"next_cursor,omitempty"`
}

// ProjectionSchema is the compatibility date of the derived projection
// generation currently served (the dated *_20260615 table family). It is
// stamped onto the wire `schema` field; a future generation bumps this in
// lockstep with a new dated table family (derived_projection_schemas).
const ProjectionSchema = "2026-06-15"

// handleGetSessionTraces handles GET /v1/sessions/:id/traces.
func (s *Server) handleGetSessionTraces(c fiber.Ctx) error {
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

	limit, err := parseTracesLimit(c.Query("limit"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	var cursor *tracesPageCursor
	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeTracesPageCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		// A cursor is a boundary in one session's trace order; presented
		// with another session it is a malformed request, not a transition.
		if cur.Session != id {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "cursor does not match session"})
		}
		cursor = &cur
	}

	orgID := singleTenantOrgID
	sess, err := sessions.GetSessionRecord(c.RequestCtx(), orgID, id)
	if err != nil {
		s.logger.Error("get session for traces", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	// Everything that can still turn into an error response is loaded
	// here, before the status is committed: the payload-free turn headers
	// (with their span counts, which the trace header carries ahead of its
	// spans) and the session's links. The spans themselves stream.
	turns, err := reader.ListTraceSummaries(c.RequestCtx(), id)
	if err != nil {
		s.logger.Error("list trace summaries", "session_id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session traces"})
	}
	links, err := reader.ListSessionLinks(c.RequestCtx(), id)
	if err != nil {
		s.logger.Error("list session links", "session_id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session traces"})
	}
	if cursor != nil {
		// Turn headers arrive in emit order, so the traces an earlier
		// page served are a prefix.
		start := 0
		for start < len(turns) && cursor.covers(turns[start].SpanTurnRecord) {
			start++
		}
		turns = turns[start:]
	}

	page := &tracesPage{
		sessionID: id,
		orgID:     orgID,
		session:   sessionItemFromStorage(*sess, time.Now()),
		turns:     turns,
		links:     links,
		mode:      payloadModeFromQuery(c.Query("payload")),
		limit:     limit,
		budget:    tracesPageByteBudget,
		spans:     reader,
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
			s.logger.Warn("session traces stream cut short", "session_id", id, "error", err)
		}
	})
}

// parseTracesLimit reads the composite's `limit` query: the number of
// traces a page holds, defaulting to 50 and clamped to 200. Anything
// that is not a positive integer is rejected rather than defaulted.
func parseTracesLimit(raw string) (int, error) {
	return parseLimit(raw, defaultTracesLimit, maxTracesLimit)
}

// BuildSessionTraces assembles the composite response whole. Pure
// rendering: every edge and kind here was computed by the deriver. The
// handler no longer calls it — it streams a page through tracesPage — but
// this remains the reference shape: a page that fits in one is
// byte-identical to json.Marshal of this, which `tapes dev trace-fixtures`
// relies on and the stream specs prove.
func BuildSessionTraces(
	session SessionItem,
	turns []storage.SpanTurnRecord,
	spans []storage.SpanRecord,
	links []storage.SpanLinkRecord,
	mode PayloadMode,
) *SessionTracesResponse {
	resp := &SessionTracesResponse{
		Schema:  ProjectionSchema,
		Session: session,
		Traces:  []TraceDetail{},
		Links:   []SpanLinkItem{},
	}

	spansByTrace := map[string][]storage.SpanRecord{}
	for _, sp := range spans {
		spansByTrace[sp.TraceID] = append(spansByTrace[sp.TraceID], sp)
	}

	// Tasks and kind_counts are deriver-owned session rollups; they ride
	// in session.rollup now, folded by sessionItemFromStorage — the
	// composite no longer carries a top-level copy.

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
// fields promoted to typed columns. Preview mode serves the stored
// previews as they are and marks the item so clients drill in for the
// full payload; a row without stored previews is marked pending and
// served with empty content, never a preview computed here.
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
		Usage:        emptyObjectIfNil(sp.Usage),
	}
	switch {
	case mode != PayloadPreview:
		item.Input = contentArray(sp.Input)
		item.Output = contentArray(sp.Output)
	case sp.HasPreview:
		item.Input = contentArray(sp.InputPreview)
		item.Output = contentArray(sp.OutputPreview)
		item.Payload = string(PayloadPreview)
	default:
		item.Input = json.RawMessage("[]")
		item.Output = json.RawMessage("[]")
		item.Payload = string(PayloadPending)
	}
	return item
}

// contentArray renders a stored content-block array for the wire
// verbatim, pinned to [] when empty.
func contentArray(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("[]")
	}
	return raw
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

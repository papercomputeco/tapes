package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// sessionsReader is the capability interface for the product sessions API
// (the sessions-table-backed surface at /v1/sessions). The Postgres driver
// implements it; the handler returns 501 for drivers that don't.
type sessionsReader interface {
	ListSessionRecords(ctx context.Context, orgID string, opts storage.SessionListOpts) ([]storage.SessionRecord, error)
	GetSessionRecord(ctx context.Context, orgID, id string) (*storage.SessionRecord, error)
	GetSessionRecordByHarness(ctx context.Context, orgID string, harnessID string, harnessSessionID string) (*storage.SessionRecord, error)
	// UpdateSessionName sets (or, when name is nil or trims empty, clears)
	// the user-editable session title. The org_id predicate lives in the
	// implementation's storage query (CC-2): a cross-org id must affect
	// zero rows. Returns the number of rows affected so the handler can
	// distinguish "updated" from "not in this org / unknown id" (404).
	UpdateSessionName(ctx context.Context, orgID, id string, name *string) (int64, error)
}

// sessionsWriter is the capability interface for mutating sessions (DELETE
// /v1/sessions/:id). The Postgres driver implements it; the handler returns
// 501 for drivers that don't.
type sessionsWriter interface {
	DeleteSession(ctx context.Context, orgID, id string) (bool, error)
}

const (
	defaultSessionsLimit = 50
	maxSessionsLimit     = 200

	// maxSessionNameLength bounds the user-editable session title after
	// trimming (CC-3). Chosen to comfortably fit a human-authored title
	// while still being a hard, server-enforced ceiling.
	maxSessionNameLength = 200
)

// SessionItem is the per-session shape: capture identity at the top
// level, the deriver-owned projection nested under `rollup`. The split
// mirrors the storage rows — identity is ingest-written, rollup is
// deriver-written — so the wire can't blur which layer owns a field.
type SessionItem struct {
	// Identity — capture-side facts, ingest-written.
	ID               string         `json:"id"`
	HarnessID        string         `json:"harness_id"`
	HarnessSessionID string         `json:"harness_session_id"`
	Cwd              string         `json:"cwd,omitempty"`
	HarnessVersion   string         `json:"harness_version,omitempty"`
	ParentSessionID  string         `json:"parent_session_id,omitempty"`
	StartedAt        time.Time      `json:"started_at"`
	LastSeenAt       time.Time      `json:"last_seen_at"`
	EndedAt          *time.Time     `json:"ended_at,omitempty"`
	HarnessMetadata  map[string]any `json:"harness_metadata,omitempty"`
	// AuthSubject is the gateway-stamped JWT subject (WorkOS user id)
	// captured at ingest; empty for rows captured before the edge began
	// stamping it.
	AuthSubject string `json:"auth_subject,omitempty"`
	// Name is the session's display label: the value on the identity row —
	// a user rename or the harness-supplied session name — when set,
	// otherwise the folded title (rollup.title) as a fallback. Empty only
	// when the session has neither. It therefore equals rollup.title
	// exactly when no identity-row name exists.
	Name string `json:"name,omitempty"`
	// Live is a runtime presence signal, not a projection fact: true when
	// the session was seen within the liveness window AND the deriver has
	// not marked it terminal. Computed at response time from last_seen_at,
	// so the console renders it directly instead of inferring "running"
	// from recency itself (keeps the console dumb; RFD 00007 §C).
	Live bool `json:"live"`
	// Rollup is the deriver-owned projection over the session's spans.
	Rollup SessionRollup `json:"rollup"`
}

// sessionLiveWindow bounds how recently a session must have been seen to
// read as live. Server config now (mirrors the console's old 5-minute
// client-side window) so liveness is decided in one place.
const sessionLiveWindow = 5 * time.Minute

// terminalStatus reports whether a derived status is a settled outcome, in
// which case the session is done regardless of recency.
func terminalStatus(s string) bool {
	switch s {
	case "completed", "failed", "abandoned":
		return true
	}
	return false
}

// SessionRollup is the deriver-owned session projection — status, title,
// counts, and spend, all folded from the span layer at derive time.
// Every field is 'unknown'/zero/empty until the session first derives.
type SessionRollup struct {
	Status string `json:"status"`
	// Title is the deriver's folded session title (derived_title),
	// generated from the conversation. Empty until title generation
	// produces one. It never falls back to the identity-row name, so it is
	// the stable descriptive title clients prefer for display; the
	// identity-row label (harness name or rename) is SessionItem.Name.
	Title     string `json:"title,omitempty"`
	Preview   string `json:"preview,omitempty"`
	TurnCount int    `json:"turn_count"`
	// Model is the dominant conversation-spine model; ModelUsage is the
	// per-model spend breakdown across every thread (subagent models
	// included), cost-ordered so the UI can show "dominant model + share"
	// without a cheap-subagent fan-out skewing it.
	Model      string       `json:"model,omitempty"`
	ModelUsage []ModelUsage `json:"model_usage,omitempty"`
	// KindCounts (spans per call_kind) and Tasks (TaskCreate/TaskUpdate
	// folds) are pinned so the rollup shape is uniform across sessions.
	KindCounts map[string]int `json:"kind_counts"`
	Tasks      []TreeTask     `json:"tasks"`
	Usage      SessionUsage   `json:"usage"`
}

// SessionUsage is the session's total token/cost spend, folded from the
// span layer. Pinned (no omitempty) for a uniform object shape.
type SessionUsage struct {
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUSD      float64 `json:"cost_usd"`
}

// ModelUsage is one model's contribution to a session in the API: how
// many llm calls ran on it and what they spent. Cost-weighted (priced
// at derive time) so a per-model share reflects spend, not call count.
type ModelUsage struct {
	Model        string  `json:"model"`
	Calls        int64   `json:"calls"`
	InputTokens  int64   `json:"input_tokens"`
	OutputTokens int64   `json:"output_tokens"`
	CostUsd      float64 `json:"cost_usd"`
}

// SessionListResponse is the response envelope for GET /v1/sessions.
type SessionListResponse struct {
	Items      []SessionItem `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// SessionDetailResponse is the response for GET /v1/sessions/:id: the
// session record alone. The conversation content lives on the span model
// (GET /v1/sessions/:id/traces).
type SessionDetailResponse struct {
	Session SessionItem `json:"session"`
}

func sessionItemFromStorage(s storage.SessionRecord, now time.Time) SessionItem {
	item := SessionItem{
		ID:               s.ID,
		HarnessID:        s.HarnessID,
		HarnessSessionID: s.HarnessSessionID,
		Cwd:              s.Cwd,
		HarnessVersion:   s.HarnessVersion,
		ParentSessionID:  s.ParentSessionID,
		StartedAt:        s.StartedAt,
		LastSeenAt:       s.LastSeenAt,
		EndedAt:          s.EndedAt,
		HarnessMetadata:  s.HarnessMetadata,
		AuthSubject:      s.AuthSubject,
		Name:             s.Name,
		Live:             now.Sub(s.LastSeenAt) < sessionLiveWindow && !terminalStatus(s.DerivedStatus),
		Rollup: SessionRollup{
			Status:     s.DerivedStatus,
			Title:      s.DerivedTitle,
			Preview:    s.Preview,
			TurnCount:  s.TurnCount,
			Model:      s.Model,
			ModelUsage: modelUsageFromStorage(s.ModelUsage),
			KindCounts: map[string]int{},
			Tasks:      []TreeTask{},
			Usage: SessionUsage{
				InputTokens:  s.TotalInputTokens,
				OutputTokens: s.TotalOutputTokens,
				CostUSD:      s.TotalCostUsd,
			},
		},
	}
	// Tasks/kind_counts are stored as raw deriver JSON; decode them into
	// the rollup, leaving the pinned []/{} on absent or malformed values.
	if len(s.Tasks) > 0 {
		_ = json.Unmarshal(s.Tasks, &item.Rollup.Tasks)
	}
	if len(s.KindCounts) > 0 {
		_ = json.Unmarshal(s.KindCounts, &item.Rollup.KindCounts)
	}
	return item
}

// modelUsageFromStorage maps the storage-layer per-model breakdown to
// the API shape. Nil in stays nil out (omitted from the response).
func modelUsageFromStorage(in []storage.ModelUsage) []ModelUsage {
	if len(in) == 0 {
		return nil
	}
	out := make([]ModelUsage, len(in))
	for i, mu := range in {
		out[i] = ModelUsage{
			Model:        mu.Model,
			Calls:        mu.Calls,
			InputTokens:  mu.InputTokens,
			OutputTokens: mu.OutputTokens,
			CostUsd:      mu.CostUSD,
		}
	}
	return out
}

// sessionsCursor is the decoded pagination cursor. It carries the sort context
// so the keyset boundary is unambiguous and a replay under a different sort is
// detectable.
type sessionsCursor struct {
	Sort string `json:"sort,omitempty"`
	Dir  string `json:"dir,omitempty"`
	Val  string `json:"val,omitempty"`
	ID   string `json:"id"`
}

func encodeSessionsCursor(c sessionsCursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		// json.Marshal cannot fail for this struct shape.
		panic(fmt.Sprintf("encoding sessions cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeSessionsCursor(token string) (sessionsCursor, error) {
	if token == "" {
		return sessionsCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return sessionsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c sessionsCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return sessionsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	// Every cursor we mint carries its full sort context plus a keyset boundary
	// {id,val}. The console resets the cursor on any sort change, so a token
	// missing the sort context is malformed or hand-crafted, not a legacy
	// client — reject it here rather than defaulting it into a boundary the
	// caller never asked for. (Val may legitimately be empty for a text sort
	// column whose boundary row holds an empty string; the numeric-column guard
	// in handleListSessions handles the case where an empty Val would 500.)
	if c.ID == "" || c.Sort == "" || c.Dir == "" {
		return sessionsCursor{}, errors.New("invalid cursor: missing sort context")
	}
	return c, nil
}

// handleListSessions handles GET /v1/sessions.
//
//	@Summary		List sessions
//	@ID			listSessions
//	@Description	Returns one row per harness session from the sessions table, cursor-paginated. Default order is last_active (last_seen_at) desc; override with the sort and direction query params.
//	@Tags			sessions
//	@Produce		json
//	@Param			limit				query		int		false	"Maximum number of sessions to return (default 50, max 200)"	minimum(1)
//	@Param			cursor				query		string	false	"Opaque pagination cursor returned by a previous response"
//	@Param			sort				query		string	false	"Sort column: last_active|started_at|turn_count|total_cost_usd|total_tokens|duration_ns|derived_status|auth_subject (default last_active)"
//	@Param			direction			query		string	false	"Sort direction: asc|desc (default desc)"
//	@Param			since				query		string	false	"Only include sessions with a turn started at or after this RFC3339 timestamp (activity window, matches /v1/stats)"	format(date-time)
//	@Param			until				query		string	false	"Only include sessions with a turn started before this RFC3339 timestamp (activity window, matches /v1/stats)"			format(date-time)
//	@Param			harness_id			query		string	false	"Filter to the single session with this harness id (exact match; requires harness_session_id, incompatible with cursor; limit is ignored when the filter is active)"
//	@Param			harness_session_id	query		string	false	"Filter to the single session with this harness session id (exact match; requires harness_id, incompatible with cursor; limit is ignored when the filter is active)"
//	@Param			auth_subject		query		string	false	"Filter the paged list to sessions captured for this gateway-stamped JWT subject (exact match; ignored on the harness filter path)"
//	@Success		200					{object}	SessionListResponse
//	@Failure		400					{object}	llm.ErrorResponse	"Invalid query parameters, a lone harness filter param, or cursor combined with the harness filter"
//	@Failure		500					{object}	llm.ErrorResponse	"Failed to list sessions"
//	@Failure		501					{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions [get]
func (s *Server) handleListSessions(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	// The harness natural-key filter is a point lookup that bypasses the
	// paged-list path entirely. Route to it whenever either param is
	// non-empty — an empty value is treated as absent, since ingest
	// guarantees no stored row carries an empty harness id, so an empty
	// value could never address a row anyway. Both-or-neither validation
	// (and cursor incompatibility) happens in the filter handler; requests
	// without the params take the existing path untouched.
	if c.Query("harness_id") != "" || c.Query("harness_session_id") != "" {
		return s.listSessionsByHarness(c, reader)
	}

	limit := defaultSessionsLimit
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "limit must be a positive integer"})
		}
		if parsed > maxSessionsLimit {
			parsed = maxSessionsLimit
		}
		limit = parsed
	}

	sortField, ok := storage.ParseSessionSortField(c.Query("sort"))
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid sort field"})
	}
	dir, ok := storage.ParseSortDirection(c.Query("direction"))
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid direction"})
	}
	opts := storage.SessionListOpts{Sort: sortField, Dir: dir}

	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeSessionsCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		// A cursor is only valid within the sort it was minted under; the UI
		// resets the cursor on any sort change, so a mismatch is a malformed
		// request, not a normal transition.
		if cur.Sort != string(sortField) || cur.Dir != string(dir) {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "cursor does not match sort/direction"})
		}
		// An empty boundary value only round-trips through the keyset for a text
		// column (''::text is valid); for numeric/timestamptz columns it would
		// cast as ''::bigint and 500 mid-scan. Reject it as the malformed client
		// input it is rather than letting it reach storage. (col resolves here
		// because sortField already passed ParseSessionSortField above.)
		if col, _ := storage.SessionSortColumn(sortField); col.Cast() != "text" && cur.Val == "" {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid cursor: empty boundary value"})
		}
		opts.CursorVal = &cur.Val
		opts.CursorID = &cur.ID
	}
	if raw := c.Query("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "since must be an RFC3339 timestamp"})
		}
		opts.Since = &t
	}
	if raw := c.Query("until"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "until must be an RFC3339 timestamp"})
		}
		opts.Until = &t
	}

	orgID := orgIDFromCtx(c)
	// Fetch one extra item to detect whether a next page exists.
	// Trusted to the same degree as X-Tapes-Org-Id: client-asserted
	// today, with the cloud edge able to stamp it server-side from the
	// validated JWT once claim mapping is enabled on the read route.
	opts.AuthSubject = c.Query("auth_subject")
	opts.Limit = limit + 1
	sessions, err := reader.ListSessionRecords(c.Context(), orgID, opts)
	if err != nil {
		s.logger.Error("list sessions", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
	}

	var nextCursor string
	if len(sessions) > limit {
		sessions = sessions[:limit]
		last := sessions[len(sessions)-1]
		nextCursor = encodeSessionsCursor(sessionsCursor{
			Sort: string(sortField),
			Dir:  string(dir),
			Val:  last.SortVal,
			ID:   last.ID,
		})
	}

	items := make([]SessionItem, len(sessions))
	for i, sess := range sessions {
		items[i] = sessionItemFromStorage(sess, time.Now())
	}

	return c.JSON(SessionListResponse{
		Items:      items,
		NextCursor: nextCursor,
	})
}

// listSessionsByHarness handles GET /v1/sessions when the harness
// natural-key filter params are present. It validates that harness_id and
// harness_session_id are supplied both-or-neither (400 on a lone param),
// rejects cursor combined with the filter (400), then performs a single
// org-scoped exact-match lookup via GetSessionRecordByHarness and returns
// the standard SessionListResponse envelope with 0 or 1 items and no
// next_cursor.
func (s *Server) listSessionsByHarness(c *fiber.Ctx, reader sessionsReader) error {
	harnessID := c.Query("harness_id")
	harnessSessionID := c.Query("harness_session_id")
	if harnessID == "" || harnessSessionID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "harness_id and harness_session_id must be supplied together",
		})
	}
	if c.Query("cursor") != "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "cursor cannot be combined with the harness filter",
		})
	}

	orgID := orgIDFromCtx(c)
	sess, err := reader.GetSessionRecordByHarness(c.Context(), orgID, harnessID, harnessSessionID)
	if err != nil {
		s.logger.Error("get session by harness", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
	}

	// A nil record is a normal no-match: the list envelope's empty items
	// form expresses it (never 404 — that's the :id endpoint's vocabulary).
	items := []SessionItem{}
	if sess != nil {
		items = append(items, sessionItemFromStorage(*sess, time.Now()))
	}
	return c.JSON(SessionListResponse{Items: items})
}

// handleGetSession handles GET /v1/sessions/:id.
//
//	@Summary		Get a session
//	@ID			getSession
//	@Description	Returns a single session record. The conversation content lives on the span model: GET /v1/sessions/{id}/traces.
//	@Tags			sessions
//	@Produce		json
//	@Param			id	path		string	true	"Session id (UUID)"
//	@Success		200	{object}	SessionDetailResponse
//	@Failure		400	{object}	llm.ErrorResponse	"Missing or malformed id"
//	@Failure		404	{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to load session"
//	@Failure		501	{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions/{id} [get]
func (s *Server) handleGetSession(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}
	if _, err := uuid.Parse(id); err != nil {
		// A session id is a UUID; a malformed one is a client error, not a
		// storage failure. (The swagger annotation documents 400 here.)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
	}

	orgID := orgIDFromCtx(c)
	sess, err := reader.GetSessionRecord(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get session", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	return c.JSON(SessionDetailResponse{
		Session: sessionItemFromStorage(*sess, time.Now()),
	})
}

// exportDetail selects how much of the span projection an export line
// carries: full spans (the default) or trace headers only.
type exportDetail string

const (
	exportDetailSpans  exportDetail = "spans"
	exportDetailTraces exportDetail = "traces"
)

// exportDetailFromQuery maps the ?detail= query param to a grain; empty
// defaults to the full span grain. The second return is false for an
// unrecognized value so handlers can 400 instead of silently exporting
// a different grain than the caller asked for.
func exportDetailFromQuery(v string) (exportDetail, bool) {
	switch v {
	case "", string(exportDetailSpans):
		return exportDetailSpans, true
	case string(exportDetailTraces):
		return exportDetailTraces, true
	}
	return "", false
}

// exportTraceHeader is one trace in a detail=traces export line: the
// turn header alone, with no spans/links keys at all — omitting them
// distinguishes "not exported at this grain" from "zero spans".
type exportTraceHeader struct {
	Trace TraceItem `json:"trace"`
}

// exportSessionTraceHeaders is a detail=traces export line: the session
// with its turn headers. tasks/kind_counts are span-derived, so they are
// omitted along with the spans. `schema` stamps the projection generation
// so a traces-grain export line is self-describing like the composite.
type exportSessionTraceHeaders struct {
	Schema  string              `json:"schema"`
	Session SessionItem         `json:"session"`
	Traces  []exportTraceHeader `json:"traces"`
}

// exportSessionLine renders one session's export record as a single JSON
// line. At the spans grain (default) it is the same nested session →
// traces → spans projection GET /v1/sessions/{id}/traces serves (full
// payloads); at the traces grain it is the session with turn headers
// only, read via ListTraceSummaries so span payloads are never loaded.
// Both export endpoints emit these grains, so a bulk export is exactly a
// concatenation of per-session exports.
func exportSessionLine(ctx context.Context, reader spanModelReader, orgID string, sess storage.SessionRecord, detail exportDetail, w io.Writer) error {
	if detail == exportDetailTraces {
		rows, err := reader.ListTraceSummaries(ctx, sess.ID)
		if err != nil {
			return fmt.Errorf("list trace summaries for session %s: %w", sess.ID, err)
		}
		item := sessionItemFromStorage(sess, time.Now())
		line := exportSessionTraceHeaders{
			Schema:  ProjectionSchema,
			Session: item,
			Traces:  make([]exportTraceHeader, 0, len(rows)),
		}
		for _, row := range rows {
			ti := traceItemFromTurn(row.SpanTurnRecord, row.SpanCount)
			line.Traces = append(line.Traces, exportTraceHeader{Trace: ti})
		}
		if err := json.NewEncoder(w).Encode(line); err != nil {
			return fmt.Errorf("encoding session %s: %w", sess.ID, err)
		}
		return nil
	}

	return streamSessionSpanExport(ctx, reader, orgID, sess, w)
}

// streamSessionSpanExport renders the spans-grain export line for one
// session — the same nested session → traces → spans projection
// BuildSessionTraces produces at PayloadFull — but bounds peak memory to
// one trace's spans instead of the whole session's. The light,
// payload-free rows (turn headers, which carry the trace order and per-
// trace span counts, plus the session-scoped links) are loaded whole;
// each trace's heavy spans are read, encoded, and released one trace at a
// time. This is the fix for the bulk export OOM: a single heavy session no
// longer has to materialize every span payload at once.
//
// The output is byte-for-byte identical to
//
//	json.NewEncoder(w).Encode(BuildSessionTraces(session, turns, spans, links, PayloadFull))
//
// for the same rows — the array/object framing is written by hand and each
// element goes through the same json marshaller the composite response
// uses, so the reused serializers (spanItemFromRecord, traceItemFromTurn,
// spanLinkItem) fix the wire shape by construction. The golden test in
// sessions_export_span_stream_test.go pins that equivalence.
func streamSessionSpanExport(ctx context.Context, reader spanModelReader, orgID string, sess storage.SessionRecord, w io.Writer) error {
	// Light, payload-free rows loaded whole: turn headers are the trace
	// ordering authority (BuildSessionTraces emits traces in this order),
	// links are the flat session-scoped array.
	turns, err := reader.ListTraceSummaries(ctx, sess.ID)
	if err != nil {
		return fmt.Errorf("list trace summaries for session %s: %w", sess.ID, err)
	}
	links, err := reader.ListSessionLinks(ctx, sess.ID)
	if err != nil {
		return fmt.Errorf("list session links for session %s: %w", sess.ID, err)
	}

	// {"schema":...,"session":...,"traces":[ — the SessionTracesResponse
	// field order, written by hand so the framing matches json.Marshal of
	// the whole struct.
	if err := writeJSONRaw(w, `{"schema":`); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	if err := writeJSONValue(w, ProjectionSchema); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	if err := writeJSONRaw(w, `,"session":`); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	if err := writeJSONValue(w, sessionItemFromStorage(sess, time.Now())); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	if err := writeJSONRaw(w, `,"traces":[`); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}

	for i, turn := range turns {
		if i > 0 {
			if err := writeJSONRaw(w, ","); err != nil {
				return fmt.Errorf("encoding session %s: %w", sess.ID, err)
			}
		}
		if err := streamTraceDetail(ctx, reader, orgID, turn.SpanTurnRecord, w); err != nil {
			return fmt.Errorf("encoding session %s: %w", sess.ID, err)
		}
	}

	// ],"links":[ ...session links... ]}
	if err := writeJSONRaw(w, `],"links":[`); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	for i, l := range links {
		if i > 0 {
			if err := writeJSONRaw(w, ","); err != nil {
				return fmt.Errorf("encoding session %s: %w", sess.ID, err)
			}
		}
		if err := writeJSONValue(w, spanLinkItem(l)); err != nil {
			return fmt.Errorf("encoding session %s: %w", sess.ID, err)
		}
	}
	// The trailing newline matches json.Encoder.Encode, so a streamed line
	// terminates exactly like the materialized one.
	if err := writeJSONRaw(w, "]}\n"); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	return nil
}

// streamTraceDetail writes one trace's {"trace":...,"spans":[...]} object,
// loading that trace's spans on their own so only one trace's payloads are
// resident at a time. This mirrors the embedded TraceDetail json.Marshal
// produces inside the composite response (Schema and Links are zero there,
// so both omitempty fields drop out — only trace and spans remain), and
// the span count on the header is len(spans), exactly as
// BuildSessionTraces computes it.
func streamTraceDetail(ctx context.Context, reader spanModelReader, orgID string, turn storage.SpanTurnRecord, w io.Writer) error {
	spans, err := reader.ListTraceSpans(ctx, orgID, turn.TraceID)
	if err != nil {
		return fmt.Errorf("list spans for trace %s: %w", turn.TraceID, err)
	}
	if err := writeJSONRaw(w, `{"trace":`); err != nil {
		return err
	}
	if err := writeJSONValue(w, traceItemFromTurn(turn, len(spans))); err != nil {
		return err
	}
	if err := writeJSONRaw(w, `,"spans":[`); err != nil {
		return err
	}
	for i, sp := range spans {
		if i > 0 {
			if err := writeJSONRaw(w, ","); err != nil {
				return err
			}
		}
		// Encode one span at a time so the marshalling buffer stays O(one
		// span), never O(one trace) — the resident span slice is already
		// bounded to this trace.
		if err := writeJSONValue(w, spanItemFromRecord(sp, PayloadFull)); err != nil {
			return err
		}
	}
	return writeJSONRaw(w, "]}")
}

// writeJSONValue marshals v with the same HTML-escaping json.Marshal (and
// thus json.Encoder.Encode) applies, and writes it out. Marshalling each
// element separately and gluing the array/object framing by hand yields
// bytes identical to marshalling the whole composite, since JSON encoding
// of these payload types is context-free.
func writeJSONValue(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// writeJSONRaw writes literal structural bytes (brackets, commas, keys).
func writeJSONRaw(w io.Writer, s string) error {
	_, err := io.WriteString(w, s)
	return err
}

// exportFilename appends the non-default grain to an export filename so
// a traces-grain download is distinguishable from a full one on disk.
func exportFilename(base string, detail exportDetail) string {
	if detail == exportDetailTraces {
		return strings.TrimSuffix(base, ".jsonl") + "-traces.jsonl"
	}
	return base
}

// handleExportSession handles GET /v1/sessions/:id/export. It renders the
// session's full trace/span projection as one JSONL line, for the
// console's per-session download.
//
//	@Summary		Export a session as JSONL
//	@ID			exportSession
//	@Description	Returns the session as a single JSON line (downloadable attachment): the session object with its traces, each trace carrying its full spans — the same shape as GET /v1/sessions/{id}/traces with payload=full. detail=traces exports turn headers only (no spans or links).
//	@Tags			sessions
//	@Produce		application/x-ndjson
//	@Param			id		path	string	true	"Session id (UUID)"
//	@Param			detail	query	string	false	"Export granularity: spans (default, traces with full spans) or traces (turn headers only)"	Enums(spans, traces)
//	@Success		200		{string}	string	"JSONL body, one session object with nested traces (and spans at detail=spans)"
//	@Failure		400		{object}	llm.ErrorResponse	"Missing or malformed id, or unrecognized detail"
//	@Failure		404		{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to load or render the session"
//	@Failure		501		{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions/{id}/export [get]
func (s *Server) handleExportSession(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}
	// The export needs both sessionsReader and spanModelReader; a driver
	// can implement the former without the latter, so this is a second,
	// independent 501 gate checked before any header is set.
	spanReader, ok := s.driver.(spanModelReader)
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
	detail, ok := exportDetailFromQuery(c.Query("detail"))
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "detail must be spans or traces"})
	}

	orgID := orgIDFromCtx(c)
	// Resolve existence under the org BEFORE anything is streamed, so a
	// cross-org request gets a clean 404 with no headers or partial body
	// committed — the same tenancy gate handleGetSession applies.
	sess, err := reader.GetSessionRecord(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get session for export", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	// Render into a buffer first so the NDJSON/attachment headers are only
	// committed once the export succeeds. A single session is bounded, so
	// buffering is cheap — and it means a mid-render failure returns a clean
	// JSON error (application/json) instead of a 500 body wearing a
	// Content-Disposition: attachment header from a download that never
	// happened. (The streaming bulk endpoint can't do this; a single session
	// can.)
	var buf bytes.Buffer
	if err := exportSessionLine(c.Context(), spanReader, orgID, *sess, detail, &buf); err != nil {
		s.logger.Error("export session", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to render session export"})
	}

	filename := exportFilename(fmt.Sprintf("session-%s-%s.jsonl", id, time.Now().UTC().Format("2006-01-02")), detail)
	c.Set("Content-Type", "application/x-ndjson")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	return c.Send(buf.Bytes())
}

// exportSessionsPageLimit is the internal page size handleExportSessions
// uses when walking ListSessionRecords. It intentionally matches
// maxSessionsLimit (the UI list cap): the point of this endpoint is to
// keep paging past that cap rather than being bounded by it, one page at a
// time, so memory stays flat regardless of how many sessions fall in the
// window.
const exportSessionsPageLimit = maxSessionsLimit

// handleExportSessions handles GET /v1/sessions/export?since=&until=. It
// streams every session in the requested window (default: trailing 30
// days) as one nested JSON line each — session → traces → spans — paging
// internally past the 200-row UI cap via the same keyset cursor
// handleListSessions uses.
//
//	@Summary		Export sessions in a time window as JSONL
//	@ID			exportSessions
//	@Description	Streams one JSON line per session in the given window, newest-first, as a downloadable attachment. Each line is the session object with its traces, each trace carrying its full spans — the same shape as GET /v1/sessions/{id}/traces with payload=full. detail=traces exports turn headers only (no spans or links). Defaults to the trailing 30 days. Not bounded by the /v1/sessions list cap — pages internally.
//	@Tags			sessions
//	@Produce		application/x-ndjson
//	@Param			since	query	string	false	"Only include sessions with a turn started at or after this RFC3339 timestamp (activity window; default: now - 30 days)"	format(date-time)
//	@Param			until	query	string	false	"Only include sessions with a turn started before this RFC3339 timestamp (activity window)"								format(date-time)
//	@Param			detail	query	string	false	"Export granularity: spans (default, traces with full spans) or traces (turn headers only)"				Enums(spans, traces)
//	@Success		200		{string}	string	"JSONL body, one JSON object per session with nested traces (and spans at detail=spans)"
//	@Failure		400		{object}	llm.ErrorResponse	"Malformed since/until, or unrecognized detail"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to list or render sessions"
//	@Failure		501		{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions/export [get]
func (s *Server) handleExportSessions(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}
	spanReader, ok := s.driver.(spanModelReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "span traces not supported by this backend"})
	}
	detail, ok := exportDetailFromQuery(c.Query("detail"))
	if !ok {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "detail must be spans or traces"})
	}

	// The 30-day window is the maximum span for v1 (R-20), not just the
	// default: floor is enforced unconditionally, even when the caller
	// supplies an explicit since older than 30 days, so the endpoint can
	// never be used to stream an org's entire history
	// (?since=1970-01-01T00:00:00Z). In-window since/until overrides
	// (R-9) still work as before — only the lower bound is clamped.
	floor := time.Now().UTC().AddDate(0, 0, -30)
	since := floor
	if raw := c.Query("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "since must be an RFC3339 timestamp"})
		}
		since = t
	}
	if since.Before(floor) {
		since = floor
	}
	var until *time.Time
	if raw := c.Query("until"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "until must be an RFC3339 timestamp"})
		}
		until = &t
	}
	if until != nil && !until.After(since) {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "until must be after since"})
	}

	orgID := orgIDFromCtx(c)
	ctx := c.Context()

	// Name the file after the window that actually produced it. The default
	// (no since/until) is the trailing 30 days; an explicit since/until gets a
	// dated range so the filename never claims "last-30-days" for a narrower
	// window. `since` here is the effective (clamped) lower bound.
	now := time.Now().UTC()
	filename := fmt.Sprintf("sessions-last-30-days-%s.jsonl", now.Format("2006-01-02"))
	if c.Query("since") != "" || c.Query("until") != "" {
		end := "now"
		if until != nil {
			end = until.UTC().Format("2006-01-02")
		}
		filename = fmt.Sprintf("sessions-%s-to-%s.jsonl", since.UTC().Format("2006-01-02"), end)
	}
	filename = exportFilename(filename, detail)
	c.Set("Content-Type", "application/x-ndjson")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	// SetBodyStreamWriter's callback has no error return: a failure
	// mid-stream can only be logged and the loop stopped, since the HTTP
	// status is already committed by the time bytes are flowing (documented
	// as an accepted v1 tradeoff in the design). ctx.Bind() is unavailable
	// here (this is the raw fasthttp callback, not a fiber.Ctx), so orgID,
	// since, until, reader, and spanReader are captured directly.
	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer w.Flush()

		opts := storage.SessionListOpts{
			Since: &since,
			Until: until,
			Limit: exportSessionsPageLimit,
		}
		for {
			sessions, err := reader.ListSessionRecords(streamCtx, orgID, opts)
			if err != nil {
				s.logger.Error("list sessions for export", "error", err)
				return
			}
			for _, sess := range sessions {
				if err := exportSessionLine(streamCtx, spanReader, orgID, sess, detail, w); err != nil {
					s.logger.Error("export session", "id", sess.ID, "error", err)
					return
				}
				// Flush after each session so bytes reach the client
				// incrementally instead of only at the end of the whole
				// window — the point of streaming.
				if err := w.Flush(); err != nil {
					// Client went away or the connection failed; nothing
					// left to do but stop producing.
					return
				}
			}

			if len(sessions) < exportSessionsPageLimit {
				return
			}
			last := sessions[len(sessions)-1]
			opts.CursorVal = &last.SortVal
			opts.CursorID = &last.ID
		}
	})

	return nil
}

// handleDeleteSession handles DELETE /v1/sessions/:id.
//
//	@Summary		Delete a session
//	@ID			deleteSession
//	@Description	Permanently deletes a session and its subtree: subagent child sessions and their derived traces/spans cascade with it. Org-scoped — any caller in the org may delete any of its sessions. The immutable raw_turns capture log is left intact.
//	@Tags			sessions
//	@Param			id	path	string	true	"Session id (UUID)"
//	@Success		204	"Session deleted"
//	@Failure		400	{object}	llm.ErrorResponse	"Missing or malformed id"
//	@Failure		404	{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to delete session"
//	@Failure		501	{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions/{id} [delete]
func (s *Server) handleDeleteSession(c *fiber.Ctx) error {
	writer, ok := s.driver.(sessionsWriter)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}
	if _, err := uuid.Parse(id); err != nil {
		// A session id is a UUID; a malformed one is a client error, not a
		// storage miss — mirrors handleGetSession's 400.
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
	}

	orgID := orgIDFromCtx(c)
	deleted, err := writer.DeleteSession(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("delete session", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to delete session"})
	}
	if !deleted {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// sessionUpdateRequest is the PATCH /v1/sessions/:id body. Name is a pointer
// so an absent field (nil) is distinguishable from an explicit null or an
// empty string, both of which mean "clear back to the auto-derived title"
// (CC-3); an absent field is a 400 (nothing to update).
type sessionUpdateRequest struct {
	Name *string `json:"name"`
}

// Referenced only by the swagger @Param annotation on handleUpdateSession (the
// handler decodes into a raw map to tell an absent field from an explicit
// null), so keep it alive for the unused linter — same pattern as the swagger
// request types in swagger.go.
var _ = sessionUpdateRequest{}

// handleUpdateSession handles PATCH /v1/sessions/:id.
//
// It updates the user-editable session title only (CC-1, CC-4): the server
// trims and bounds the name (CC-3), calls UpdateSessionName with the
// org-scoped predicate carried in storage (CC-2), and on success re-reads
// GetSessionRecord to return the updated session summary so the client can
// write its cache through (CC-6/CC-7 on the frontend side).
//
//	@Summary		Update a session's title
//	@Description	Updates the user-editable session name. An absent field is a 400; null or empty (after trim) clears back to the auto-derived title. Length is bounded to 200 characters.
//	@Tags			sessions
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string					true	"Session id (UUID)"
//	@Param			request	body		sessionUpdateRequest	true	"Update request"
//	@Success		200		{object}	SessionDetailResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Missing/malformed id, missing name field, or name exceeds 200 characters"
//	@Failure		404		{object}	llm.ErrorResponse	"Session not found or not in caller's org"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to update session"
//	@Failure		501		{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions/{id} [patch]
func (s *Server) handleUpdateSession(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}
	if _, err := uuid.Parse(id); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
	}

	// Decode into raw messages first so an absent "name" key (nothing to
	// update, 400) is distinguishable from an explicit null or empty string
	// (both valid "clear the title" requests). A *string alone can't make
	// that distinction — both cases unmarshal to nil.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(c.Body(), &raw); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid request body"})
	}
	nameRaw, present := raw["name"]
	if !present {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "name is required"})
	}

	var name *string
	if err := json.Unmarshal(nameRaw, &name); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "name must be a string or null"})
	}

	// Normalize server-side (CC-3): trim; empty-after-trim (including an
	// explicit null) clears back to the auto-derived title (nil); otherwise
	// bound the length and store the trimmed value.
	var normalized *string
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed != "" {
			if utf8.RuneCountInString(trimmed) > maxSessionNameLength {
				return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "name must be at most 200 characters"})
			}
			normalized = &trimmed
		}
	}

	orgID := orgIDFromCtx(c)
	rowsAffected, err := reader.UpdateSessionName(c.Context(), orgID, id, normalized)
	if err != nil {
		s.logger.Error("update session name", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to update session"})
	}
	if rowsAffected == 0 {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	sess, err := reader.GetSessionRecord(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get session", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to update session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	return c.JSON(SessionDetailResponse{
		Session: sessionItemFromStorage(*sess, time.Now()),
	})
}

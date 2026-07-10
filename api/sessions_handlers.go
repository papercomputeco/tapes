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
	"time"

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
)

// SessionItem is the per-row shape returned by GET /v1/sessions. It mirrors
// the sessions table directly — no ancestry walk, no stem aggregation.
type SessionItem struct {
	ID                string     `json:"id"`
	HarnessID         string     `json:"harness_id"`
	HarnessSessionID  string     `json:"harness_session_id"`
	Name              string     `json:"name,omitempty"`
	Cwd               string     `json:"cwd,omitempty"`
	HarnessVersion    string     `json:"harness_version,omitempty"`
	ParentSessionID   string     `json:"parent_session_id,omitempty"`
	StartedAt         time.Time  `json:"started_at"`
	LastSeenAt        time.Time  `json:"last_seen_at"`
	EndedAt           *time.Time `json:"ended_at,omitempty"`
	TurnCount         int        `json:"turn_count"`
	TotalInputTokens  int64      `json:"total_input_tokens"`
	TotalOutputTokens int64      `json:"total_output_tokens"`
	TotalCostUsd      float64    `json:"total_cost_usd"`
	DerivedStatus     string     `json:"derived_status"`
	// Model is the dominant conversation-spine model, folded at derive
	// time; empty until the session first derives.
	Model string `json:"model,omitempty"`
	// ModelUsage is the per-model spend breakdown folded at derive time
	// across every thread (subagent models included), ordered
	// dominant-model-first by cost. The share basis is cost, not call
	// count, so the UI can show "dominant model + per-model %" without a
	// cheap-subagent fan-out skewing it. Populated on the session detail;
	// nil until the session first derives.
	ModelUsage      []ModelUsage   `json:"model_usage,omitempty"`
	HarnessMetadata map[string]any `json:"harness_metadata,omitempty"`
	Preview         string         `json:"preview,omitempty"`
	// AuthSubject is the gateway-stamped JWT subject (WorkOS user id)
	// captured at ingest; empty for rows captured before the edge began
	// stamping it.
	AuthSubject string `json:"auth_subject,omitempty"`
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

func sessionItemFromStorage(s storage.SessionRecord) SessionItem {
	return SessionItem{
		ID:                s.ID,
		HarnessID:         s.HarnessID,
		HarnessSessionID:  s.HarnessSessionID,
		Name:              s.Name,
		Cwd:               s.Cwd,
		HarnessVersion:    s.HarnessVersion,
		ParentSessionID:   s.ParentSessionID,
		StartedAt:         s.StartedAt,
		LastSeenAt:        s.LastSeenAt,
		EndedAt:           s.EndedAt,
		TurnCount:         s.TurnCount,
		TotalInputTokens:  s.TotalInputTokens,
		TotalOutputTokens: s.TotalOutputTokens,
		TotalCostUsd:      s.TotalCostUsd,
		DerivedStatus:     s.DerivedStatus,
		Model:             s.Model,
		ModelUsage:        modelUsageFromStorage(s.ModelUsage),
		HarnessMetadata:   s.HarnessMetadata,
		Preview:           s.Preview,
		AuthSubject:       s.AuthSubject,
	}
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
//	@Description	Returns one row per harness session from the sessions table, cursor-paginated. Default order is last_active (last_seen_at) desc; override with the sort and direction query params.
//	@Tags			sessions
//	@Produce		json
//	@Param			limit				query		int		false	"Maximum number of sessions to return (default 50, max 200)"	minimum(1)
//	@Param			cursor				query		string	false	"Opaque pagination cursor returned by a previous response"
//	@Param			sort				query		string	false	"Sort column: last_active|started_at|turn_count|total_cost_usd|total_tokens|duration_ns|derived_status|auth_subject (default last_active)"
//	@Param			direction			query		string	false	"Sort direction: asc|desc (default desc)"
//	@Param			since				query		string	false	"Only include sessions active (last_seen_at) at or after this RFC3339 timestamp"	format(date-time)
//	@Param			until				query		string	false	"Only include sessions active (last_seen_at) before this RFC3339 timestamp"			format(date-time)
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
		items[i] = sessionItemFromStorage(sess)
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
		items = append(items, sessionItemFromStorage(*sess))
	}
	return c.JSON(SessionListResponse{Items: items})
}

// handleGetSession handles GET /v1/sessions/:id.
//
//	@Summary		Get a session
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
		Session: sessionItemFromStorage(*sess),
	})
}

// exportSessionLine renders one session's export record: the same
// nested session → traces → spans projection GET /v1/sessions/{id}/traces
// serves (full payloads), encoded as a single JSON line. Both export
// endpoints emit this grain, so a bulk export is exactly a concatenation
// of per-session exports.
func exportSessionLine(ctx context.Context, reader spanModelReader, sess storage.SessionRecord, w io.Writer) error {
	turns, spans, links, err := reader.ListSessionSpanModel(ctx, sess.ID)
	if err != nil {
		return fmt.Errorf("list span model for session %s: %w", sess.ID, err)
	}
	resp := BuildSessionTraces(sessionItemFromStorage(sess), turns, spans, links, PayloadFull)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return fmt.Errorf("encoding session %s: %w", sess.ID, err)
	}
	return nil
}

// handleExportSession handles GET /v1/sessions/:id/export. It renders the
// session's full trace/span projection as one JSONL line, for the
// console's per-session download.
//
//	@Summary		Export a session as JSONL
//	@Description	Returns the session as a single JSON line (downloadable attachment): the session object with its traces, each trace carrying its full spans — the same shape as GET /v1/sessions/{id}/traces with payload=full.
//	@Tags			sessions
//	@Produce		application/x-ndjson
//	@Param			id	path	string	true	"Session id (UUID)"
//	@Success		200	{string}	string	"JSONL body, one session object with nested traces and spans"
//	@Failure		400	{object}	llm.ErrorResponse	"Missing or malformed id"
//	@Failure		404	{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to load or render the session"
//	@Failure		501	{object}	llm.ErrorResponse	"Sessions not supported by this backend"
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
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}
	if _, err := uuid.Parse(id); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
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
	if err := exportSessionLine(c.Context(), spanReader, *sess, &buf); err != nil {
		s.logger.Error("export session", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to render session export"})
	}

	filename := fmt.Sprintf("session-%s-%s.jsonl", id, time.Now().UTC().Format("2006-01-02"))
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
//	@Description	Streams one JSON line per session in the given window, newest-first, as a downloadable attachment. Each line is the session object with its traces, each trace carrying its full spans — the same shape as GET /v1/sessions/{id}/traces with payload=full. Defaults to the trailing 30 days. Not bounded by the /v1/sessions list cap — pages internally.
//	@Tags			sessions
//	@Produce		application/x-ndjson
//	@Param			since	query	string	false	"Only include sessions active (last_seen_at) at or after this RFC3339 timestamp (default: now - 30 days)"	format(date-time)
//	@Param			until	query	string	false	"Only include sessions active (last_seen_at) before this RFC3339 timestamp"								format(date-time)
//	@Success		200		{string}	string	"JSONL body, one JSON object per session with nested traces and spans"
//	@Failure		400		{object}	llm.ErrorResponse	"Malformed since/until"
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
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
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
				if err := exportSessionLine(streamCtx, spanReader, sess, w); err != nil {
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

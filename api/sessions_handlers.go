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
	ModelUsage []ModelUsage `json:"model_usage,omitempty"`
	// Outcomes is the fold of artifacts the session produced (pull
	// requests / repos / issues), detected from tool spans at derive
	// time and deduped by URL. Each carries trace/span provenance back
	// to the detecting tool call. Nil until the session derives or when
	// it produced nothing — "no outcome" is a signal the UI renders
	// explicitly (PCC-837/PCC-840).
	Outcomes        []Outcome      `json:"outcomes,omitempty"`
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

// Outcome is one artifact a session produced, in the API: kind is an
// open set (pull_request / repo / issue / linear_issue today), url is
// the artifact's identity, and trace_id/span_id point back at the
// detecting tool span so consumers can deep-link the exact turn.
type Outcome struct {
	Kind       string    `json:"kind"`
	URL        string    `json:"url"`
	Title      string    `json:"title,omitempty"`
	Repo       string    `json:"repo,omitempty"`
	TraceID    string    `json:"trace_id,omitempty"`
	SpanID     string    `json:"span_id,omitempty"`
	DetectedBy string    `json:"detected_by,omitempty"`
	DetectedAt time.Time `json:"detected_at,omitzero"`
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
		Outcomes:          outcomesFromStorage(s.Outcomes),
		HarnessMetadata:   s.HarnessMetadata,
		Preview:           s.Preview,
		AuthSubject:       s.AuthSubject,
	}
}

// outcomesFromStorage maps the storage-layer outcome fold to the API
// shape. Nil in stays nil out (omitted from the response).
func outcomesFromStorage(in []storage.Outcome) []Outcome {
	if len(in) == 0 {
		return nil
	}
	out := make([]Outcome, len(in))
	for i, o := range in {
		out[i] = Outcome{
			Kind:       o.Kind,
			URL:        o.URL,
			Title:      o.Title,
			Repo:       o.Repo,
			TraceID:    o.TraceID,
			SpanID:     o.SpanID,
			DetectedBy: o.DetectedBy,
			DetectedAt: o.DetectedAt,
		}
	}
	return out
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

// sessionsCursor is the decoded pagination cursor for the sessions list,
// keyed on (last_seen_at DESC, id DESC).
type sessionsCursor struct {
	LastSeenAt time.Time `json:"ts"`
	ID         string    `json:"id"`
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
	if c.ID == "" {
		return sessionsCursor{}, errors.New("invalid cursor: missing id")
	}
	return c, nil
}

// handleListSessions handles GET /v1/sessions.
//
//	@Summary		List sessions
//	@Description	Returns one row per harness session from the sessions table, newest first (last_seen_at desc), cursor-paginated.
//	@Tags			sessions
//	@Produce		json
//	@Param			limit				query		int		false	"Maximum number of sessions to return (default 50, max 200)"	minimum(1)
//	@Param			cursor				query		string	false	"Opaque pagination cursor returned by a previous response"
//	@Param			since				query		string	false	"Only include sessions active (last_seen_at) at or after this RFC3339 timestamp"	format(date-time)
//	@Param			until				query		string	false	"Only include sessions active (last_seen_at) before this RFC3339 timestamp"		format(date-time)
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

	opts := storage.SessionListOpts{}
	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeSessionsCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		opts.CursorTs = &cur.LastSeenAt
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
			LastSeenAt: last.LastSeenAt,
			ID:         last.ID,
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

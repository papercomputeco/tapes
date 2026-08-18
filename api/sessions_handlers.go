package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	// ListSessionRecordsByHarnessSessionID serves the lone-harness_session_id
	// form of the /v1/sessions filter: an exact match on harness_session_id
	// across all harnesses in the org. The id is unique within a harness, so
	// the result carries at most one row per harness — in practice zero or
	// one. No match is an empty slice, not an error.
	ListSessionRecordsByHarnessSessionID(ctx context.Context, orgID string, harnessSessionID string) ([]storage.SessionRecord, error)
	// UpdateSessionDisplayName sets (or, when name is nil or trims empty,
	// clears) the user-editable session title. It writes the dedicated
	// display_name column, not name, so a rename survives ingest re-sending
	// the harness slug (PCC-970). The org_id predicate lives in the
	// implementation's storage query (CC-2): a cross-org id must affect
	// zero rows. Returns the number of rows affected so the handler can
	// distinguish "updated" from "not in this org / unknown id" (404).
	UpdateSessionDisplayName(ctx context.Context, orgID, id string, name *string) (int64, error)
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
	// Name is the harness identity-row label — the harness-supplied session
	// name (a plan slug), or the folded title (rollup.title) as a fallback
	// when no name was captured. This is capture/deriver provenance, NOT a
	// user title: ingest re-sends it every turn. Clients should render
	// DisplayTitle, not Name (PCC-970).
	Name string `json:"name,omitempty"`
	// DisplayName is the user's Console rename (sessions.display_name),
	// empty unless a user set one. Written only by PATCH /v1/sessions/:id,
	// never by ingest, so it survives a live session. It is the top of the
	// DisplayTitle resolution; exposed raw so the edit affordance can seed
	// its input from the user's own title (not the resolved fallback).
	DisplayName string `json:"display_name,omitempty"`
	// DisplayTitle is the server-resolved label clients should render:
	// DisplayName -> rollup.title (generated) -> preview -> Name -> id
	// slice. Resolving once on the server keeps every client (Console,
	// paper CLI) from re-deriving — and diverging on — the precedence
	// (PCC-970). Never empty: it falls back to a short harness id slice, then
	// the session id (the primary key, always set for a stored row).
	DisplayTitle string `json:"display_title"`
	// Live is a runtime presence signal, not a projection fact: true when
	// the session has no recorded end and was seen within the liveness
	// window. Keyed on ended_at + last_seen_at recency (both ingest-fresh),
	// never on the derived status: an interactive session folds to a
	// terminal status (an end_turn assistant reply reads as "completed")
	// after every turn while still open, so status cannot gate liveness.
	// Computed at response time so the console renders it directly instead
	// of inferring "running" itself.
	Live bool `json:"live"`
	// Rollup is the deriver-owned projection over the session's spans.
	Rollup SessionRollup `json:"rollup"`
}

// sessionLiveWindow bounds how recently a session must have been seen to
// read as live. Server config now (mirrors the console's old 5-minute
// client-side window) so liveness is decided in one place.
const sessionLiveWindow = 5 * time.Minute

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

// resolveSessionDisplayTitle picks the label clients render, resolving the
// provenance tiers in ONE place so every client (Console, paper CLI) agrees
// instead of re-deriving — and diverging on — the precedence (PCC-970):
//
//	DisplayName (user Console rename) — explicit, always wins
//	-> DerivedTitle (generated title-gen output)
//	-> Preview (first user prompt)    — skipped if it's a JSON tool-result blob
//	-> Name (harness slug / coalesced) — last human-ish label
//	-> a short harness_session_id slice, then the session id — never empty
//
// Preview is already scaffolding-stripped by the deriver. PreviewIsJSON is
// classified from the full prompt before storage truncates the preview; the
// local validity check also covers untruncated records constructed by other
// storage implementations and tests.
func resolveSessionDisplayTitle(s storage.SessionRecord) string {
	if t := strings.TrimSpace(s.DisplayName); t != "" {
		return t
	}
	if t := strings.TrimSpace(s.DerivedTitle); t != "" {
		return t
	}
	if t := strings.TrimSpace(s.Preview); t != "" && !s.PreviewIsJSON && !looksLikeJSONPreview(t) {
		return t
	}
	if t := strings.TrimSpace(s.Name); t != "" {
		return t
	}
	// Ultimate fallback: the harness id slice, or — if a row somehow carries
	// no harness_session_id — the session's own primary key, which is always
	// set for a stored row. Guarantees the never-empty contract (DisplayTitle
	// has no omitempty) even for a degenerate record.
	if slug := shortHarnessSessionID(s.HarnessSessionID); slug != "" {
		return slug
	}
	return s.ID
}

// looksLikeJSONPreview guards against previews that are really tool-result
// payloads rather than user prose. The opening delimiter is only a fast path:
// Markdown-style plugin invocations also begin with "[" (for example,
// [@visualize](plugin://...)), so require the whole preview to be valid JSON
// before suppressing it as a display-title candidate.
func looksLikeJSONPreview(s string) bool {
	t := strings.TrimLeft(s, " \t\r\n")
	if !strings.HasPrefix(t, "{") && !strings.HasPrefix(t, "[") {
		return false
	}
	return json.Valid([]byte(t))
}

// shortHarnessSessionID is the last-resort label: a 12-char slice of the
// harness session id, matching the Console's historical id fallback.
func shortHarnessSessionID(id string) string {
	const n = 12
	if len(id) <= n {
		return id
	}
	return id[:n]
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
		DisplayName:      s.DisplayName,
		DisplayTitle:     resolveSessionDisplayTitle(s),
		Live:             s.EndedAt == nil && now.Sub(s.LastSeenAt) < sessionLiveWindow,
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
func (s *Server) handleListSessions(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	// The harness natural-key filter is an exact-match lookup that bypasses
	// the paged-list path entirely. Route to it whenever either param is
	// non-empty — an empty value is treated as absent, since ingest
	// guarantees no stored row carries an empty harness id, so an empty
	// value could never address a row anyway. Param validation (a lone
	// harness_id, cursor incompatibility) happens in the filter handler;
	// requests without the params take the existing path untouched.
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

	orgID := singleTenantOrgID
	// auth_subject is a caller-supplied filter, not an identity claim: it
	// narrows results within this tenant and grants nothing. The verified
	// subject is stamped at ingest from the JWT (x-paper-auth-subject) and
	// is what gets stored; this only chooses which of those rows to show.
	opts.AuthSubject = c.Query("auth_subject")
	// Fetch one extra item to detect whether a next page exists.
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
// natural-key filter params are present. Both params are exact-match
// filters: the pair is an org-scoped point lookup on the (harness_id,
// harness_session_id) unique index, and harness_session_id alone matches
// across all harnesses — the id is unique per harness, so even the lone
// form returns at most one row per harness (in practice zero or one). A
// lone harness_id is rejected (400): it names a harness, not a session,
// and would be an unbounded, unpaginated list. Cursor, sort, direction,
// since, and until combined with the filter are rejected (400): they
// belong to the paged-list path, and the lookup has no ordering or window
// to apply them to. Returns the standard SessionListResponse envelope
// with the matching items and no next_cursor.
func (s *Server) listSessionsByHarness(c *fiber.Ctx, reader sessionsReader) error {
	harnessID := c.Query("harness_id")
	harnessSessionID := c.Query("harness_session_id")
	if harnessSessionID == "" {
		// harnessID is necessarily non-empty here: the router only enters
		// this path when at least one of the two params is non-empty.
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "harness_id requires harness_session_id",
		})
	}
	if c.Query("cursor") != "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "cursor cannot be combined with the harness filter",
		})
	}
	// The paged-list options have nothing to act on here: the lookup's
	// order is fixed and its window is the whole table. Refusing them
	// loudly — naming each offender — beats silently discarding options
	// the caller believes are in effect. This applies to the paired and
	// lone forms alike.
	var unsupported []string
	for _, name := range []string{"sort", "direction", "since", "until"} {
		if c.Query(name) != "" {
			unsupported = append(unsupported, name)
		}
	}
	if len(unsupported) > 0 {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: strings.Join(unsupported, ", ") + " cannot be combined with the harness filter",
		})
	}

	orgID := singleTenantOrgID

	// No match is a normal outcome on either branch: the list envelope's
	// empty items form expresses it (never 404 — that's the :id endpoint's
	// vocabulary).
	items := []SessionItem{}
	if harnessID == "" {
		recs, err := reader.ListSessionRecordsByHarnessSessionID(c.Context(), orgID, harnessSessionID)
		if err != nil {
			s.logger.Error("list sessions by harness session id", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
		}
		for _, rec := range recs {
			items = append(items, sessionItemFromStorage(rec, time.Now()))
		}
		return c.JSON(SessionListResponse{Items: items})
	}

	sess, err := reader.GetSessionRecordByHarness(c.Context(), orgID, harnessID, harnessSessionID)
	if err != nil {
		s.logger.Error("get session by harness", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
	}
	if sess != nil {
		items = append(items, sessionItemFromStorage(*sess, time.Now()))
	}
	return c.JSON(SessionListResponse{Items: items})
}

// handleGetSession handles GET /v1/sessions/:id.
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

	orgID := singleTenantOrgID
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

// handleDeleteSession handles DELETE /v1/sessions/:id.
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

	orgID := singleTenantOrgID
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

// sessionUpdateRequest is the PATCH /v1/sessions/:id body. DisplayName is a
// pointer so an absent field (nil) is distinguishable from an explicit null
// or an empty string, both of which mean "clear back to the auto-derived
// title" (CC-3); an absent field is a 400 (nothing to update). The field is
// display_name (not name) so the request matches the display_name it sets on
// the response — and never the harness identity `name` (PCC-970).
type sessionUpdateRequest struct {
	DisplayName *string `json:"display_name"`
}

// Referenced only by the swagger @Param annotation on handleUpdateSession (the
// handler decodes into a raw map to tell an absent field from an explicit
// null), so keep it alive for the unused linter — same pattern as the swagger
// request types in swagger.go.
var _ = sessionUpdateRequest{}

// handleUpdateSession handles PATCH /v1/sessions/:id.
//
// It updates the user-editable display title only (CC-1, CC-4): the server
// trims and bounds the value (CC-3), calls UpdateSessionDisplayName with the
// org-scoped predicate carried in storage (CC-2), and on success re-reads
// GetSessionRecord to return the updated session summary so the client can
// write its cache through (CC-6/CC-7 on the frontend side).
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

	// Decode into raw messages first so an absent "display_name" key (nothing
	// to update, 400) is distinguishable from an explicit null or empty string
	// (both valid "clear the title" requests). A *string alone can't make
	// that distinction — both cases unmarshal to nil.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(c.Body(), &raw); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid request body"})
	}
	nameRaw, present := raw["display_name"]
	if !present {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "display_name is required"})
	}

	var name *string
	if err := json.Unmarshal(nameRaw, &name); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "display_name must be a string or null"})
	}

	// Normalize server-side (CC-3): trim; empty-after-trim (including an
	// explicit null) clears back to the auto-derived title (nil); otherwise
	// bound the length and store the trimmed value.
	var normalized *string
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed != "" {
			if utf8.RuneCountInString(trimmed) > maxSessionNameLength {
				return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "display_name must be at most 200 characters"})
			}
			normalized = &trimmed
		}
	}

	orgID := singleTenantOrgID
	rowsAffected, err := reader.UpdateSessionDisplayName(c.Context(), orgID, id, normalized)
	if err != nil {
		s.logger.Error("update session display name", "id", id, "error", err)
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

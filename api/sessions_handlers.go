package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// sessionsReader is the capability interface for the product sessions API
// (the sessions-table-backed surface at /v1/sessions). The Postgres driver
// implements it; the handler returns 501 for drivers that don't.
type sessionsReader interface {
	ListSessionRecords(ctx context.Context, orgID string, opts storage.SessionListOpts) ([]storage.SessionRecord, error)
	GetSessionRecord(ctx context.Context, orgID, id string) (*storage.SessionRecord, error)
	ListNodesBySession(ctx context.Context, sessionID string) ([]*merkle.Node, error)
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
	Model           string         `json:"model,omitempty"`
	HarnessMetadata map[string]any `json:"harness_metadata,omitempty"`
	Preview         string         `json:"preview,omitempty"`
}

// SessionListResponse is the response envelope for GET /v1/sessions.
type SessionListResponse struct {
	Items      []SessionItem `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// StemSummary describes one root-to-deepest-leaf path (a "stem") within a
// session. The Stems array in SessionDetailResponse lists every root so the
// caller can switch between stems without a separate API call.
type StemSummary struct {
	RootHash string `json:"root_hash"`
	Length   int    `json:"length"`
	Preview  string `json:"preview,omitempty"`
	Model    string `json:"model,omitempty"`
}

// SessionDetailResponse is the response for GET /v1/sessions/:id. Turns
// contains the selected stem (controlled by ?stem= and ?root=). Stems lists
// every root in the session sorted by length descending so callers can offer
// a picker.
type SessionDetailResponse struct {
	Session SessionItem   `json:"session"`
	Turns   []Turn        `json:"turns"`
	Stems   []StemSummary `json:"stems"`
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
		HarnessMetadata:   s.HarnessMetadata,
		Preview:           s.Preview,
	}
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
//	@Description	Returns one row per harness session from the sessions table, newest first (last_seen_at desc), cursor-paginated. This is the product session view; the Merkle leaf-chain view lives at /v1/stems.
//	@Tags			sessions
//	@Produce		json
//	@Param			limit	query		int		false	"Maximum number of sessions to return (default 50, max 200)"	minimum(1)
//	@Param			cursor	query		string	false	"Opaque pagination cursor returned by a previous response"
//	@Param			since	query		string	false	"Only include sessions active (last_seen_at) at or after this RFC3339 timestamp"	format(date-time)
//	@Param			until	query		string	false	"Only include sessions active (last_seen_at) before this RFC3339 timestamp"		format(date-time)
//	@Success		200		{object}	SessionListResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Invalid query parameters"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to list sessions"
//	@Failure		501		{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions [get]
func (s *Server) handleListSessions(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
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

// sessionStems returns one StemSummary per root in nodes, sorted by length
// descending (longest first).
func sessionStems(nodes []*merkle.Node) []StemSummary {
	inSet := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		inSet[n.Hash] = true
	}
	children := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		if n.ParentHash != nil && inSet[*n.ParentHash] {
			children[*n.ParentHash] = append(children[*n.ParentHash], n.Hash)
		}
	}

	var depth func(hash string) int
	depth = func(hash string) int {
		best := 0
		for _, kid := range children[hash] {
			if d := depth(kid); d > best {
				best = d
			}
		}
		return 1 + best
	}

	var stems []StemSummary
	for _, n := range nodes {
		if n.ParentHash == nil || !inSet[*n.ParentHash] {
			stems = append(stems, StemSummary{
				RootHash: n.Hash,
				Length:   depth(n.Hash),
				Preview:  makePreview(n),
				Model:    n.Bucket.Model,
			})
		}
	}
	sort.Slice(stems, func(i, j int) bool {
		return stems[i].Length > stems[j].Length
	})
	return stems
}

// subtreeFromRoot returns all nodes reachable from rootHash by following
// parent→child edges, in the order they appear in nodes.
func subtreeFromRoot(rootHash string, nodes []*merkle.Node) []*merkle.Node {
	byHash := make(map[string]*merkle.Node, len(nodes))
	inSet := make(map[string]bool, len(nodes))
	children := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		byHash[n.Hash] = n
		inSet[n.Hash] = true
	}
	for _, n := range nodes {
		if n.ParentHash != nil && inSet[*n.ParentHash] {
			children[*n.ParentHash] = append(children[*n.ParentHash], n.Hash)
		}
	}

	var result []*merkle.Node
	var visit func(hash string)
	visit = func(hash string) {
		if n, ok := byHash[hash]; ok {
			result = append(result, n)
			for _, kid := range children[hash] {
				visit(kid)
			}
		}
	}
	visit(rootHash)
	return result
}

// longestStemFromNodes returns the nodes on the longest root-to-leaf path
// (stem) in the session's DAG, in chronological order (root first). Roots are
// nodes whose parent_hash is absent or points outside the supplied set.
func longestStemFromNodes(nodes []*merkle.Node) []*merkle.Node {
	if len(nodes) == 0 {
		return nil
	}

	byHash := make(map[string]*merkle.Node, len(nodes))
	inSet := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		byHash[n.Hash] = n
		inSet[n.Hash] = true
	}

	children := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		if n.ParentHash != nil && inSet[*n.ParentHash] {
			children[*n.ParentHash] = append(children[*n.ParentHash], n.Hash)
		}
	}

	var deepest func(hash string) []string
	deepest = func(hash string) []string {
		kids := children[hash]
		if len(kids) == 0 {
			return []string{hash}
		}
		var best []string
		for _, kid := range kids {
			if path := deepest(kid); len(path) > len(best) {
				best = path
			}
		}
		return append([]string{hash}, best...)
	}

	var bestPath []string
	for _, n := range nodes {
		if n.ParentHash == nil || !inSet[*n.ParentHash] {
			if path := deepest(n.Hash); len(path) > len(bestPath) {
				bestPath = path
			}
		}
	}

	result := make([]*merkle.Node, len(bestPath))
	for i, h := range bestPath {
		result[i] = byHash[h]
	}
	return result
}

// handleGetSession handles GET /v1/sessions/:id.
//
//	@Summary		Get a session
//	@Description	Returns a single session, the selected stem of turns, and a summary of every stem (root-to-leaf path) in the session. ?stem=longest (default) returns the longest stem; ?stem=all returns every node ordered by created_at. ?root=<hash> restricts turns to the subtree rooted at that node.
//	@Tags			sessions
//	@Produce		json
//	@Param			id		path		string	true	"Session id (UUID)"
//	@Param			stem	query		string	false	"Which turns to return: longest (default) or all"	Enums(longest, all)
//	@Param			root	query		string	false	"Restrict turns to the subtree rooted at this node hash"
//	@Success		200		{object}	SessionDetailResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Missing id or unknown root"
//	@Failure		404		{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to load session"
//	@Failure		501		{object}	llm.ErrorResponse	"Sessions not supported by this backend"
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

	nodes, err := reader.ListNodesBySession(c.Context(), id)
	if err != nil {
		s.logger.Error("list nodes by session", "session_id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session turns"})
	}

	// Always compute the stems summary so the caller can render a picker.
	stems := sessionStems(nodes)

	// ?root=<hash> restricts turns to the subtree rooted at that node.
	// Validated against the computed stems so callers get a clear 400 rather
	// than a silent empty response for an unknown hash.
	working := nodes
	if rootHash := c.Query("root"); rootHash != "" {
		found := false
		for _, st := range stems {
			if st.RootHash == rootHash {
				found = true
				break
			}
		}
		if !found {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "root not found in this session"})
		}
		working = subtreeFromRoot(rootHash, nodes)
	}

	var selected []*merkle.Node
	switch c.Query("stem", "longest") {
	case "all":
		selected = working
	default:
		selected = longestStemFromNodes(working)
	}

	turns := make([]Turn, len(selected))
	for i, n := range selected {
		turns[i] = turnFromNode(n)
	}

	return c.JSON(SessionDetailResponse{
		Session: sessionItemFromStorage(*sess),
		Turns:   turns,
		Stems:   stems,
	})
}

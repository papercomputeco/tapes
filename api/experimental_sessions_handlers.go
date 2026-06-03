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
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// experimentalSessionsReader is the capability interface for the experimental
// sessions API. The Postgres driver implements it; the handler falls back to
// 501 Not Implemented for drivers that don't.
type experimentalSessionsReader interface {
	ListExperimentalSessions(ctx context.Context, orgID string, limit int, cursorTs *time.Time, cursorID *string) ([]storage.ExperimentalSession, error)
	GetExperimentalSessionByID(ctx context.Context, orgID, id string) (*storage.ExperimentalSession, error)
	ListNodesBySession(ctx context.Context, sessionID string) ([]*merkle.Node, error)
}

const (
	defaultExperimentalLimit = 50
	maxExperimentalLimit     = 200
)

// ExperimentalSessionItem is the per-row shape returned by
// GET /v1/experimental/sessions. It mirrors the sessions table directly —
// no ancestry walk, no chain aggregation.
type ExperimentalSessionItem struct {
	ID               string         `json:"id"`
	HarnessID        string         `json:"harness_id"`
	HarnessSessionID string         `json:"harness_session_id"`
	Name             string         `json:"name,omitempty"`
	Cwd              string         `json:"cwd,omitempty"`
	HarnessVersion   string         `json:"harness_version,omitempty"`
	ParentSessionID  string         `json:"parent_session_id,omitempty"`
	StartedAt        time.Time      `json:"started_at"`
	LastSeenAt       time.Time      `json:"last_seen_at"`
	EndedAt          *time.Time     `json:"ended_at,omitempty"`
	TurnCount        int            `json:"turn_count"`
	TotalInputTokens  int64         `json:"total_input_tokens"`
	TotalOutputTokens int64         `json:"total_output_tokens"`
	TotalCostUsd      float64       `json:"total_cost_usd"`
	HarnessMetadata   map[string]any `json:"harness_metadata,omitempty"`
}

// ExperimentalSessionListResponse is the response envelope for
// GET /v1/experimental/sessions.
type ExperimentalSessionListResponse struct {
	Items      []ExperimentalSessionItem `json:"items"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

// ExperimentalSessionDetailResponse is the response for
// GET /v1/experimental/sessions/:id. It carries the session row plus all
// nodes attributed to the session in chronological order.
type ExperimentalSessionDetailResponse struct {
	Session ExperimentalSessionItem `json:"session"`
	Turns   []Turn                  `json:"turns"`
}

func experimentalSessionItemFromStorage(s storage.ExperimentalSession) ExperimentalSessionItem {
	return ExperimentalSessionItem{
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
		HarnessMetadata:   s.HarnessMetadata,
	}
}

// experimentalCursor is the decoded pagination cursor for the experimental
// sessions list, keyed on (last_seen_at DESC, id DESC).
type experimentalCursor struct {
	LastSeenAt time.Time `json:"ts"`
	ID         string    `json:"id"`
}

func encodeExperimentalCursor(c experimentalCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeExperimentalCursor(token string) (experimentalCursor, error) {
	if token == "" {
		return experimentalCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return experimentalCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c experimentalCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return experimentalCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if c.ID == "" {
		return experimentalCursor{}, errors.New("invalid cursor: missing id")
	}
	return c, nil
}

// handleListExperimentalSessions handles GET /v1/experimental/sessions.
func (s *Server) handleListExperimentalSessions(c *fiber.Ctx) error {
	reader, ok := s.driver.(experimentalSessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "experimental sessions not supported by this backend"})
	}

	limit := defaultExperimentalLimit
	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "limit must be a positive integer"})
		}
		if parsed > maxExperimentalLimit {
			parsed = maxExperimentalLimit
		}
		limit = parsed
	}

	var cursorTs *time.Time
	var cursorID *string
	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeExperimentalCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		cursorTs = &cur.LastSeenAt
		cursorID = &cur.ID
	}

	orgID := orgIDFromCtx(c)
	// Fetch one extra item to detect whether a next page exists.
	sessions, err := reader.ListExperimentalSessions(c.Context(), orgID, limit+1, cursorTs, cursorID)
	if err != nil {
		s.logger.Error("list experimental sessions", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
	}

	var nextCursor string
	if len(sessions) > limit {
		sessions = sessions[:limit]
		last := sessions[len(sessions)-1]
		nextCursor = encodeExperimentalCursor(experimentalCursor{
			LastSeenAt: last.LastSeenAt,
			ID:         last.ID,
		})
	}

	items := make([]ExperimentalSessionItem, len(sessions))
	for i, sess := range sessions {
		items[i] = experimentalSessionItemFromStorage(sess)
	}

	return c.JSON(ExperimentalSessionListResponse{
		Items:      items,
		NextCursor: nextCursor,
	})
}

// longestChainFromNodes returns the nodes on the longest root-to-leaf path in
// the session's DAG, in chronological order (root first). Roots are nodes
// whose parent_hash is absent or points outside the supplied set.
func longestChainFromNodes(nodes []*merkle.Node) []*merkle.Node {
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

// handleGetExperimentalSession handles GET /v1/experimental/sessions/:id.
func (s *Server) handleGetExperimentalSession(c *fiber.Ctx) error {
	reader, ok := s.driver.(experimentalSessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "experimental sessions not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}

	orgID := orgIDFromCtx(c)
	sess, err := reader.GetExperimentalSessionByID(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get experimental session", "id", id, "error", err)
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

	chain := longestChainFromNodes(nodes)
	turns := make([]Turn, len(chain))
	for i, n := range chain {
		turns[i] = turnFromNode(n)
	}

	return c.JSON(ExperimentalSessionDetailResponse{
		Session: experimentalSessionItemFromStorage(*sess),
		Turns:   turns,
	})
}

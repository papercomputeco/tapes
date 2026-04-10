package api

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// previewMaxChars is the maximum number of characters included in the
// `preview` field of a session list item.
const previewMaxChars = 200

// SessionListItem is the per-item shape returned by GET /v1/sessions.
//
// It deliberately omits fields that would require an ancestry walk per item
// (e.g. started_at, depth, per-session aggregates). Callers that need those
// should fetch /v1/sessions/:hash for the specific session.
type SessionListItem struct {
	Hash      string    `json:"hash"`
	HeadRole  string    `json:"head_role,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitzero"`
	Project   string    `json:"project,omitempty"`
	AgentName string    `json:"agent_name,omitempty"`
	Model     string    `json:"model,omitempty"`
	Provider  string    `json:"provider,omitempty"`
	Preview   string    `json:"preview,omitempty"`
}

// SessionListResponse is the response envelope for GET /v1/sessions.
type SessionListResponse struct {
	Items      []SessionListItem `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

// SessionSummaryListResponse is the response envelope for
// GET /v1/sessions/summary. Items carry the rich per-session aggregates
// computed by pkg/sessions.BuildSummary.
type SessionSummaryListResponse struct {
	Items      []sessions.SessionSummary `json:"items"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

// SessionResponse is the response for GET /v1/sessions/:hash.
type SessionResponse struct {
	// Hash is the head of the returned chain (== the requested hash).
	Hash string `json:"hash"`

	// Depth is the total number of turns in the full ancestry of Hash.
	// When the client passes ?depth=N, the Turns array may contain fewer
	// than Depth items.
	Depth int `json:"depth"`

	// Turns contains the chain in chronological order (root-first).
	// When ?depth=N is supplied, only the last N turns (head + N-1 ancestors)
	// are returned, still in chronological order.
	Turns []Turn `json:"turns"`

	// Truncated is true when the ancestry walk stopped at a parent_hash
	// that could not be resolved in the current store. MissingParent
	// names that hash. This is an expected edge case on stores that
	// trim older data, merge foreign content, or offload history to
	// another source — not an error.
	Truncated     bool   `json:"truncated,omitempty"`
	MissingParent string `json:"missing_parent,omitempty"`
}

// Turn is a single message in a session's chain.
type Turn struct {
	Hash       string             `json:"hash"`
	ParentHash *string            `json:"parent_hash,omitempty"`
	Role       string             `json:"role"`
	Content    []llm.ContentBlock `json:"content"`
	Model      string             `json:"model,omitempty"`
	Provider   string             `json:"provider,omitempty"`
	AgentName  string             `json:"agent_name,omitempty"`
	StopReason string             `json:"stop_reason,omitempty"`
	Usage      *llm.Usage         `json:"usage,omitempty"`
	CreatedAt  time.Time          `json:"created_at,omitzero"`
}

// StatsResponse is the response for GET /v1/stats.
type StatsResponse struct {
	SessionCount int `json:"session_count"`
	TurnCount    int `json:"turn_count"`
	RootCount    int `json:"root_count"`
}

// handleListSessions handles GET /v1/sessions.
func (s *Server) handleListSessions(c *fiber.Ctx) error {
	opts, err := parseListOpts(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	page, err := s.driver.ListSessions(c.Context(), opts)
	if err != nil {
		s.logger.Error("list sessions", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
	}

	items := make([]SessionListItem, len(page.Items))
	for i, n := range page.Items {
		items[i] = sessionListItemFromNode(n)
	}

	return c.JSON(SessionListResponse{
		Items:      items,
		NextCursor: page.NextCursor,
	})
}

// handleGetSession handles GET /v1/sessions/:hash.
func (s *Server) handleGetSession(c *fiber.Ctx) error {
	hash := c.Params("hash")
	if hash == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "hash parameter required"})
	}

	depth := 0
	if raw := c.Query("depth"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "depth must be a positive integer"})
		}
		depth = parsed
	}

	chain, err := s.driver.AncestryChain(c.Context(), hash)
	if err != nil {
		var notFound storage.NotFoundError
		if errors.As(err, &notFound) {
			return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
		}
		s.logger.Error("load ancestry", "hash", hash, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}

	// AncestryChain returns node-first (leaf) then back toward root. Slice
	// to the requested depth before reversing into chronological order.
	ancestry := chain.Nodes
	total := len(ancestry)
	slice := ancestry
	if depth > 0 && depth < total {
		slice = ancestry[:depth]
	}

	turns := make([]Turn, len(slice))
	for i, n := range slice {
		// Reverse: last in slice becomes first in turns so output is root-first.
		turns[len(slice)-1-i] = turnFromNode(n)
	}

	return c.JSON(SessionResponse{
		Hash:          hash,
		Depth:         total,
		Turns:         turns,
		Truncated:     chain.Incomplete,
		MissingParent: chain.MissingParent,
	})
}

// handleStats handles GET /v1/stats.
func (s *Server) handleStats(c *fiber.Ctx) error {
	opts, err := parseListOpts(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	// Pagination fields are meaningless for stats.
	opts.Limit = 0
	opts.Cursor = ""

	stats, err := s.driver.CountSessions(c.Context(), opts)
	if err != nil {
		s.logger.Error("count sessions", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to compute stats"})
	}

	return c.JSON(StatsResponse{
		SessionCount: stats.SessionCount,
		TurnCount:    stats.TurnCount,
		RootCount:    stats.RootCount,
	})
}

// parseListOpts reads ListOpts fields from query params. Filter fields are
// shared by /v1/sessions and /v1/stats. Pagination fields (limit, cursor) are
// parsed here too; callers that don't need them overwrite afterwards.
//
// All validation errors are returned as plain Go errors so the calling
// handler can map them to a 400 Bad Request response, instead of letting
// them surface from the storage driver as a 500.
func parseListOpts(c *fiber.Ctx) (storage.ListOpts, error) {
	opts := storage.ListOpts{
		Project:  c.Query("project"),
		Agent:    c.Query("agent_name"),
		Model:    c.Query("model"),
		Provider: c.Query("provider"),
	}

	if raw := c.Query("cursor"); raw != "" {
		// Decode the cursor up front so a malformed token produces a
		// 400 from the handler, not a 500 from the driver. The driver
		// will decode it again later, which is harmless.
		if _, err := storage.DecodeCursor(raw); err != nil {
			return storage.ListOpts{}, err
		}
		opts.Cursor = raw
	}

	if raw := c.Query("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return storage.ListOpts{}, errors.New("limit must be a positive integer")
		}
		opts.Limit = parsed
	}

	if raw := c.Query("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return storage.ListOpts{}, errors.New("since must be an RFC3339 timestamp")
		}
		opts.Since = &t
	}

	if raw := c.Query("until"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return storage.ListOpts{}, errors.New("until must be an RFC3339 timestamp")
		}
		opts.Until = &t
	}

	return opts, nil
}

// sessionListItemFromNode builds a list item from a leaf node. It does not
// walk the ancestry; all fields come off the leaf itself.
func sessionListItemFromNode(n *merkle.Node) SessionListItem {
	return SessionListItem{
		Hash:      n.Hash,
		HeadRole:  n.Bucket.Role,
		UpdatedAt: n.CreatedAt,
		Project:   n.Project,
		AgentName: n.Bucket.AgentName,
		Model:     n.Bucket.Model,
		Provider:  n.Bucket.Provider,
		Preview:   makePreview(n),
	}
}

func turnFromNode(n *merkle.Node) Turn {
	return Turn{
		Hash:       n.Hash,
		ParentHash: n.ParentHash,
		Role:       n.Bucket.Role,
		Content:    n.Bucket.Content,
		Model:      n.Bucket.Model,
		Provider:   n.Bucket.Provider,
		AgentName:  n.Bucket.AgentName,
		StopReason: n.StopReason,
		Usage:      n.Usage,
		CreatedAt:  n.CreatedAt,
	}
}

// makePreview returns the first previewMaxChars runes of the node's
// concatenated text content, with any surrounding whitespace trimmed.
// Truncates on rune boundaries so multi-byte characters are never split.
func makePreview(n *merkle.Node) string {
	text := strings.TrimSpace(n.Bucket.ExtractText())
	runes := []rune(text)
	if len(runes) <= previewMaxChars {
		return text
	}
	return string(runes[:previewMaxChars])
}

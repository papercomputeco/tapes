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
//
// All fields are computed by a single storage-driver aggregate over the
// matching node set — no per-session chain walk. This means:
//
//   - InputTokens / OutputTokens / ToolCalls are SUMs over every node
//     matching the filter, NOT per-chain folds. Each piece of work (each
//     token billed, each tool_use invoked) is counted exactly once
//     regardless of how many leaves share its ancestor. This deliberately
//     diverges from /v1/sessions/summary's per-chain numbers, which
//     multi-count shared ancestors when leaves descend from a common
//     branch.
//   - TotalCost is folded in the handler from the per-model token rollup
//     returned by the driver, using the configured pricing table.
//   - TotalDurationNs is the wall-clock span MAX(created_at) − MIN(created_at)
//     across matching nodes, in nanoseconds. It is NOT a sum of per-call
//     durations. The underlying nodes.total_duration_ns column is now
//     populated by the proxy with per-call wall-clock duration (PCC-514);
//     switching this endpoint to SUM(total_duration_ns) is a separate
//     decision since it changes the visible semantic.
//   - CompletedCount uses leaf-status-only classification: an assistant leaf
//     with a terminal stop_reason ("stop", "end_turn", "end-turn", "eos").
//     This is an approximation of pkg/sessions.DetermineStatus, which also
//     considers tool errors and git activity from the full chain. Sessions
//     where the agent shipped work (e.g. `git commit`) but the leaf is not
//     itself terminal will undercount here. PCC-515 tracks the durable
//     fix (denormalize derived_status on Put + backfill).
type StatsResponse struct {
	SessionCount    int     `json:"session_count"`
	TurnCount       int     `json:"turn_count"`
	RootCount       int     `json:"root_count"`
	CompletedCount  int     `json:"completed_count"`
	TotalCost       float64 `json:"total_cost"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	TotalDurationNs int64   `json:"total_duration_ns"`
	ToolCalls       int     `json:"tool_calls"`
}

// handleListSessions handles GET /v1/sessions.
//
//	@Summary		List session heads
//	@Description	Returns paginated session head records ordered from newest to oldest. Filters apply to the head node of each session.
//	@Tags			sessions
//	@Produce		json
//	@Param			project		query		string	false	"Filter by project name"
//	@Param			agent_name	query		string	false	"Filter by agent name"
//	@Param			model		query		string	false	"Filter by model name"
//	@Param			provider	query		string	false	"Filter by provider name"
//	@Param			since		query		string	false	"Only include sessions updated at or after this RFC3339 timestamp"	format(date-time)
//	@Param			until		query		string	false	"Only include sessions updated before or at this RFC3339 timestamp"	format(date-time)
//	@Param			cursor		query		string	false	"Opaque pagination cursor returned by a previous response"
//	@Param			limit		query		int		false	"Maximum number of sessions to return"	minimum(1)
//	@Success		200			{object}	SessionListResponse
//	@Failure		400			{object}	llm.ErrorResponse	"Invalid query parameters"
//	@Failure		500			{object}	llm.ErrorResponse	"Failed to list sessions"
//	@Router			/v1/sessions [get]
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
//
//	@Summary		Get a session chain
//	@Description	Returns a session ancestry chain in chronological order (root first). When depth is provided, only the last N turns are returned while the full chain depth is still reported.
//	@Tags			sessions
//	@Produce		json
//	@Param			hash	path		string	true	"Session head hash"
//	@Param			depth	query		int		false	"Maximum number of most-recent turns to include"	minimum(1)
//	@Success		200		{object}	SessionResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Missing or invalid hash/depth"
//	@Failure		404		{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to load session"
//	@Router			/v1/sessions/{hash} [get]
func (s *Server) handleGetSession(c *fiber.Ctx) error {
	hash, chain, err := s.loadSessionChain(c)
	if err != nil {
		return s.handleLoadSessionChainError(c, hash, err)
	}

	depth := 0
	if raw := c.Query("depth"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "depth must be a positive integer"})
		}
		depth = parsed
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

var errHashParameterRequired = errors.New("hash parameter required")

func (s *Server) loadSessionChain(c *fiber.Ctx) (string, *storage.Chain, error) {
	hash := c.Params("hash")
	if hash == "" {
		return "", nil, errHashParameterRequired
	}

	chain, err := s.driver.AncestryChain(c.Context(), hash)
	if err != nil {
		return hash, nil, err
	}
	if len(chain.Nodes) == 0 {
		return hash, nil, storage.NotFoundError{Hash: hash}
	}

	return hash, chain, nil
}

func (s *Server) handleLoadSessionChainError(c *fiber.Ctx, hash string, err error) error {
	if errors.Is(err, errHashParameterRequired) {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: errHashParameterRequired.Error()})
	}

	var notFound storage.NotFoundError
	if errors.As(err, &notFound) {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	s.logger.Error("load session ancestry", "hash", hash, "error", err)
	return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
}

// handleStats handles GET /v1/stats.
//
//	@Summary		Get aggregate session stats
//	@Description	Returns counts plus folded cost / token / duration / tool-call / completed-count totals across every node matching the supplied filters. Numeric aggregates come from a single storage-driver SQL aggregate; cost is folded in the handler from the per-model token rollup using the configured pricing table. total_duration_ns is wall-clock MAX-MIN over the matched window (see PCC-514). completed_count uses leaf-status-only classification (assistant leaf with a terminal stop_reason) — see StatsResponse and PCC-515 for the durable chain-aware fix.
//	@Tags			sessions
//	@Produce		json
//	@Param			project		query		string	false	"Filter by project name"
//	@Param			agent_name	query		string	false	"Filter by agent name"
//	@Param			model		query		string	false	"Filter by model name"
//	@Param			provider	query		string	false	"Filter by provider name"
//	@Param			since		query		string	false	"Only include records at or after this RFC3339 timestamp"	format(date-time)
//	@Param			until		query		string	false	"Only include records before or at this RFC3339 timestamp"	format(date-time)
//	@Success		200			{object}	StatsResponse
//	@Failure		400			{object}	llm.ErrorResponse	"Invalid query parameters"
//	@Failure		500			{object}	llm.ErrorResponse	"Failed to compute stats"
//	@Router			/v1/stats [get]
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

	pricing := s.config.Pricing
	if pricing == nil {
		pricing = sessions.DefaultPricing()
	}

	return c.JSON(StatsResponse{
		SessionCount:    stats.SessionCount,
		TurnCount:       stats.TurnCount,
		RootCount:       stats.RootCount,
		CompletedCount:  stats.CompletedCount,
		TotalCost:       totalCostFromPerModel(stats.PerModel, pricing),
		InputTokens:     stats.InputTokens,
		OutputTokens:    stats.OutputTokens,
		TotalDurationNs: stats.TotalDurationNs,
		ToolCalls:       stats.ToolCalls,
	})
}

// totalCostFromPerModel folds the driver's per-model token rollup into a
// single USD total via the pricing table. Models the table doesn't price
// (e.g. unrecognized provider strings) contribute zero — same fall-through
// as pkg/sessions.BuildSummary.
func totalCostFromPerModel(perModel map[string]storage.ModelTokenStats, pricing sessions.PricingTable) float64 {
	var total float64
	for model, t := range perModel {
		price, ok := sessions.PricingForModel(pricing, model)
		if !ok {
			continue
		}
		_, _, cost := sessions.CostForTokensWithCache(price, t.InputTokens, t.OutputTokens, t.CacheCreationTokens, t.CacheReadTokens)
		total += cost
	}
	return total
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

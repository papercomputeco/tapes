package api

import (
	"context"
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
// `preview` field of a stem turn.
const previewMaxChars = 200

// StemListResponse is the response envelope for GET /v1/stems. Items carry
// the rich per-stem aggregates computed by pkg/sessions.BuildSummary.
//
// A "stem" is a root-to-leaf chain of Merkle nodes — what the older API
// called a leaf "session". The type name sessions.SessionSummary is retained
// for wire compatibility with existing consumers (the deck TUI, checkout).
type StemListResponse struct {
	Items      []sessions.SessionSummary `json:"items"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

// StemResponse is the response for GET /v1/stems/:hash — the ancestry chain
// of a single Merkle leaf, root-first.
type StemResponse struct {
	// Hash is the head of the returned chain (== the requested hash).
	Hash string `json:"hash"`

	// HarnessID and HarnessSessionID identify the upstream agent session when
	// session tracking metadata is available. For Claude Code, HarnessID is
	// "claude" and HarnessSessionID is Claude's session id.
	HarnessID        string `json:"harness_id,omitempty"`
	HarnessSessionID string `json:"harness_session_id,omitempty"`

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
//     This is a narrower set than pkg/sessions.DetermineStatus accepts:
//     that classifier also treats "tool_use" / "tool_use_response" as
//     terminal and considers tool errors and git activity from the full
//     chain. Sessions terminating on a tool request, or where the agent
//     shipped work (e.g. `git commit`) without a terminal leaf stop_reason,
//     will undercount here. PCC-515 tracks the durable fix (denormalize
//     derived_status on Put + backfill).
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

// handleGetStem handles GET /v1/stems/:hash.
//
//	@Summary		Get a stem (Merkle leaf chain)
//	@Description	Returns the ancestry chain of a single Merkle leaf in chronological order (root first). When depth is provided, only the last N turns are returned while the full chain depth is still reported.
//	@Tags			stems
//	@Produce		json
//	@Param			hash	path		string	true	"Stem head (leaf) hash"
//	@Param			depth	query		int		false	"Maximum number of most-recent turns to include"	minimum(1)
//	@Success		200		{object}	StemResponse
//	@Failure		400		{object}	llm.ErrorResponse	"Missing or invalid hash/depth"
//	@Failure		404		{object}	llm.ErrorResponse	"Stem not found"
//	@Failure		500		{object}	llm.ErrorResponse	"Failed to load stem"
//	@Router			/v1/stems/{hash} [get]
func (s *Server) handleGetStem(c *fiber.Ctx) error {
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

	identity, err := s.sessionIdentity(c.Context(), orgIDFromCtx(c), hash)
	if err != nil {
		s.logger.Warn("failed to load stem identity", "hash", hash, "error", err)
	}

	resp := StemResponse{
		Hash:          hash,
		Depth:         total,
		Turns:         turns,
		Truncated:     chain.Incomplete,
		MissingParent: chain.MissingParent,
	}
	if identity != nil {
		resp.HarnessID = identity.HarnessID
		resp.HarnessSessionID = identity.HarnessSessionID
	}

	return c.JSON(resp)
}

type sessionIdentityLookup interface {
	SessionIdentityByHash(ctx context.Context, orgID, hash string) (*storage.SessionIdentity, error)
}

func (s *Server) sessionIdentity(ctx context.Context, orgID, hash string) (*storage.SessionIdentity, error) {
	lookup, ok := s.driver.(sessionIdentityLookup)
	if !ok {
		return nil, nil
	}
	return lookup.SessionIdentityByHash(ctx, orgID, hash)
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

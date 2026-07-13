package api

import (
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// StatsResponse is the response for GET /v1/stats.
//
// On backends with the span projection (storage.SpanStatsReader) the
// numbers come from the trace-grain rollups, so they agree with the
// session detail and trace views:
//
//   - InputTokens / OutputTokens / TotalCost are SUMs of span_turns
//     rollups — delta-only per-call usage, never the re-sent history
//     that inflated the node-layer SUMs (each main call re-bills the
//     whole conversation on the wire).
//   - TotalDurationNs is the SUM of trace durations — agent time. Idle
//     time between turns no longer counts (the node-layer value was
//     wall-clock MAX−MIN over the window).
//   - TurnCount counts traces (user-visible turns), not wire nodes.
//     RootCount counts traces opened by a genuine prompt (synthetic
//     compaction continuations excluded). StemCount has no span
//     equivalent and is omitted.
//   - CompletedCount counts distinct sessions whose denormalized
//     derived_status is 'completed' (chain-aware, PCC-515).
//
// Backends without the span projection fall back to the legacy
// node-layer aggregate (see CountSessions); the legacy per-node filters
// (project / agent_name / model / provider) also force that path, since
// trace rollups don't carry those columns.
type StatsResponse struct {
	SessionCount int `json:"session_count"`
	// StemCount is a node-layer (Merkle leaf) concept with no span
	// equivalent; it is only present on the legacy fallback path.
	StemCount       int     `json:"stem_count,omitempty"`
	TurnCount       int     `json:"turn_count"`
	RootCount       int     `json:"root_count"`
	CompletedCount  int     `json:"completed_count"`
	TotalCost       float64 `json:"total_cost"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	TotalDurationNs int64   `json:"total_duration_ns"`
	ToolCalls       int     `json:"tool_calls"`
}

// handleStats handles GET /v1/stats.
//
//	@Summary		Get aggregate session stats
//	@ID			getStats
//	@Description	Returns counts plus cost / token / duration / tool-call / completed-count totals for the window. On span-projection backends the numbers are trace-grain rollup sums (delta-only usage, agent time = sum of trace durations) so they agree with the session and trace views; turn_count counts traces and stem_count is omitted. Supplying any legacy per-node filter (project / agent_name / model / provider) forces the legacy node-layer aggregate, whose sums re-bill re-sent history and whose duration is wall-clock MAX-MIN (PCC-514).
//	@Tags			sessions
//	@Produce		json
//	@Param			project		query		string	false	"Filter by project name (forces the legacy node-layer aggregate)"
//	@Param			agent_name	query		string	false	"Filter by agent name (forces the legacy node-layer aggregate)"
//	@Param			model		query		string	false	"Filter by model name (forces the legacy node-layer aggregate)"
//	@Param			provider	query		string	false	"Filter by provider name (forces the legacy node-layer aggregate)"
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

	// Span-layer path: trace-grain rollups, the accounting the deriver
	// writes. The legacy per-node filters force the node-layer fallback
	// because trace rollups don't carry those columns.
	legacyFilters := opts.Project != "" || opts.Agent != "" || opts.Model != "" || opts.Provider != ""
	if reader, ok := s.driver.(storage.SpanStatsReader); ok && !legacyFilters {
		stats, err := reader.AggregateSpanStats(c.Context(), orgIDFromCtx(c), opts.Since, opts.Until)
		if err != nil {
			s.logger.Error("aggregate span stats", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to compute stats"})
		}
		return c.JSON(StatsResponse{
			SessionCount:    stats.SessionCount,
			TurnCount:       stats.TurnCount,
			RootCount:       stats.RootCount,
			CompletedCount:  stats.CompletedCount,
			TotalCost:       stats.TotalCostUSD,
			InputTokens:     stats.InputTokens,
			OutputTokens:    stats.OutputTokens,
			TotalDurationNs: stats.TotalDurationNS,
			ToolCalls:       stats.ToolCalls,
		})
	}

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
		StemCount:       stats.StemCount,
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

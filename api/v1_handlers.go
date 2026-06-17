package api

import (
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// StatsResponse is the response for GET /v1/stats.
//
// The numbers come from the span-projection trace-grain rollups, so they
// agree with the session detail and trace views:
//
//   - InputTokens / OutputTokens / TotalCost are SUMs of span_turns
//     rollups — delta-only per-call usage, never the re-sent history
//     (each main call re-bills the whole conversation on the wire).
//   - TotalDurationNs is the SUM of trace durations — agent time. Idle
//     time between turns does not count.
//   - TurnCount counts traces (user-visible turns). RootCount counts
//     traces opened by a genuine prompt (synthetic compaction
//     continuations excluded).
//   - CompletedCount counts distinct sessions whose denormalized
//     derived_status is 'completed' (chain-aware, PCC-515).
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

// handleStats handles GET /v1/stats.
//
//	@Summary		Get aggregate session stats
//	@Description	Returns counts plus cost / token / duration / tool-call / completed-count totals for the window. The numbers are span-grain trace rollup sums (delta-only usage, agent time = sum of trace durations) so they agree with the session and trace views; turn_count counts traces. Filter the window with since/until.
//	@Tags			sessions
//	@Produce		json
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

	// Span-layer trace-grain rollups are the only accounting: the deriver
	// is the single writer of session/trace totals. The legacy per-node
	// aggregate was retired with the node layer.
	reader, ok := s.driver.(storage.SpanStatsReader)
	if !ok {
		s.logger.Error("stats unavailable: driver is not a SpanStatsReader")
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to compute stats"})
	}
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

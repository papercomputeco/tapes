package api

import (
	"errors"
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
//   - TotalDurationMs is the SUM of trace durations — agent time. Idle
//     time between turns does not count. Served in milliseconds, not the
//     nanoseconds we store: the summed ns over a wide window overflows a
//     JSON consumer's 2^53 safe-integer range (~104 cumulative days), and
//     sub-ms precision is meaningless for an aggregate agent-time figure.
//   - TurnCount counts traces (user-visible turns).
//   - CompletedCount counts distinct sessions whose denormalized
//     derived_status is 'completed' (chain-aware, PCC-515).
type StatsResponse struct {
	SessionCount    int     `json:"session_count"`
	TurnCount       int     `json:"turn_count"`
	CompletedCount  int     `json:"completed_count"`
	TotalCost       float64 `json:"total_cost"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	TotalDurationMs int64   `json:"total_duration_ms"`
	ToolCalls       int     `json:"tool_calls"`
}

// handleStats handles GET /v1/stats.
//
//	@Summary		Get aggregate session stats
//	@ID			getStats
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
	since, until, err := parseStatsWindow(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	// Span-layer trace-grain rollups are the only accounting: the deriver
	// is the single writer of session/trace totals.
	reader, ok := s.driver.(storage.SpanStatsReader)
	if !ok {
		s.logger.Error("stats unavailable: driver is not a SpanStatsReader")
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to compute stats"})
	}
	stats, err := reader.AggregateSpanStats(c.Context(), orgIDFromCtx(c), since, until)
	if err != nil {
		s.logger.Error("aggregate span stats", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to compute stats"})
	}
	return c.JSON(StatsResponse{
		SessionCount:    stats.SessionCount,
		TurnCount:       stats.TurnCount,
		CompletedCount:  stats.CompletedCount,
		TotalCost:       stats.TotalCostUSD,
		InputTokens:     stats.InputTokens,
		OutputTokens:    stats.OutputTokens,
		TotalDurationMs: stats.TotalDurationNS / int64(time.Millisecond),
		ToolCalls:       stats.ToolCalls,
	})
}

// parseStatsWindow reads the optional since/until time window from query
// params. /v1/stats is a whole-window aggregate: it has no filter or
// pagination parameters, only the time bounds.
//
// Validation errors are returned as plain Go errors so the calling handler
// can map them to a 400 Bad Request response, instead of letting them
// surface from the storage driver as a 500.
func parseStatsWindow(c *fiber.Ctx) (since, until *time.Time, err error) {
	if raw := c.Query("since"); raw != "" {
		t, perr := time.Parse(time.RFC3339, raw)
		if perr != nil {
			return nil, nil, errors.New("since must be an RFC3339 timestamp")
		}
		since = &t
	}

	if raw := c.Query("until"); raw != "" {
		t, perr := time.Parse(time.RFC3339, raw)
		if perr != nil {
			return nil, nil, errors.New("until must be an RFC3339 timestamp")
		}
		until = &t
	}

	return since, until, nil
}

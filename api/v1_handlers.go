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
//   - ToolCalls is the SUM of the turn rollups' tool span counts,
//     windowed on the turn's started_at like every other figure here
//     rather than on each tool span's own timestamp (PCC-936).
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
	// auth_subject is a caller-supplied filter, not an identity claim: it
	// narrows the totals within this tenant and grants nothing. The verified
	// subject is stamped at ingest from the JWT (x-paper-auth-subject); this
	// only chooses which of those rows to add up. Same parameter, same
	// meaning, as the /v1/sessions filter — a personal surface that scopes
	// its rows and its totals passes the one value to both.
	//
	// Absent, it is empty and every total stays org-wide.
	stats, err := reader.AggregateSpanStats(c.Context(), singleTenantOrgID, since, until, c.Query("auth_subject"))
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
// params. /v1/stats has no pagination — it is one aggregate row — so the time
// bounds and the auth_subject filter are the whole of its input; the subject
// needs no parsing and is read at the call site.
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

package api

import (
	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// handleListSessionsSummary handles GET /v1/sessions/summary.
//
// Unlike the lean /v1/sessions endpoint, this walks each session's full
// ancestry chain to compute rich per-item aggregates (cost, tokens, status,
// label, duration, etc.) via pkg/sessions.BuildSummary.
//
// Pagination and filter params match /v1/sessions exactly.
func (s *Server) handleListSessionsSummary(c *fiber.Ctx) error {
	opts, err := parseListOpts(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	page, err := s.driver.ListSessions(c.Context(), opts)
	if err != nil {
		s.logger.Error("list sessions summary", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list sessions"})
	}

	pricing := s.config.Pricing
	if pricing == nil {
		pricing = sessions.DefaultPricing()
	}

	items := make([]sessions.SessionSummary, 0, len(page.Items))
	for _, leaf := range page.Items {
		// Ancestry returns node-first, so we need to reverse for
		// chronological order before calling BuildSummary.
		ancestry, err := s.driver.Ancestry(c.Context(), leaf.Hash)
		if err != nil {
			s.logger.Warn("failed to walk ancestry for summary",
				"hash", leaf.Hash,
				"error", err,
			)
			continue
		}
		chain := reverseNodes(ancestry)

		summary, _, _, err := sessions.BuildSummary(chain, pricing)
		if err != nil {
			s.logger.Warn("failed to build summary",
				"hash", leaf.Hash,
				"error", err,
			)
			continue
		}
		items = append(items, summary)
	}

	return c.JSON(SessionSummaryListResponse{
		Items:      items,
		NextCursor: page.NextCursor,
	})
}

// reverseNodes returns a reversed copy of the slice. Used to convert the
// driver's node-first Ancestry result into chronological (root-first) order
// expected by sessions.BuildSummary.
func reverseNodes(in []*merkle.Node) []*merkle.Node {
	out := make([]*merkle.Node, len(in))
	for i, n := range in {
		out[len(in)-1-i] = n
	}
	return out
}

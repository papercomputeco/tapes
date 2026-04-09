package api

import (
	"context"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// bulkAncestrier is an optional extension interface. Drivers that implement
// it get a batched ancestry path in handleListSessionsSummary, avoiding the
// N (leaves) * D (depth) query fan-out of the per-leaf Ancestry() loop.
type bulkAncestrier interface {
	BulkAncestries(ctx context.Context, leafHashes []string) (map[string][]*merkle.Node, error)
}

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

	// Fast path: if the driver supports bulk ancestry fetching, load all
	// chains for this page in one batched walk instead of per-leaf.
	if bulker, ok := s.driver.(bulkAncestrier); ok && len(page.Items) > 0 {
		leafHashes := make([]string, 0, len(page.Items))
		for _, leaf := range page.Items {
			leafHashes = append(leafHashes, leaf.Hash)
		}
		chains, err := bulker.BulkAncestries(c.Context(), leafHashes)
		if err != nil {
			s.logger.Error("bulk ancestries for summary", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load sessions"})
		}

		items := make([]sessions.SessionSummary, 0, len(page.Items))
		for _, leaf := range page.Items {
			chain, ok := chains[leaf.Hash]
			if !ok || len(chain) == 0 {
				continue
			}
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

	// Slow path: per-leaf Ancestry() loop (used by drivers that don't
	// implement bulkAncestrier, e.g. the in-memory driver in tests).
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

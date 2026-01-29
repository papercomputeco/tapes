package api

import (
	"strconv"

	"github.com/gofiber/fiber/v2"

	apisearch "github.com/papercomputeco/tapes/api/search"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// handleSearchEndpoint handles GET /v1/search requests.
// Query parameters:
//   - query (required): the search query text
//   - top_k (optional, default 5): number of results to return
func (s *Server) handleSearchEndpoint(c *fiber.Ctx) error {
	// Verify search is configured
	if s.config.VectorDriver == nil || s.config.Embedder == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(llm.ErrorResponse{
			Error: "search is not configured: vector driver and embedder are required",
		})
	}

	query := c.Query("query")
	if query == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "query parameter is required",
		})
	}

	topK := 5
	if topKStr := c.Query("top_k"); topKStr != "" {
		parsed, err := strconv.Atoi(topKStr)
		if err != nil || parsed <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
				Error: "top_k must be a positive integer",
			})
		}
		topK = parsed
	}

	output, err := apisearch.Search(
		c.Context(),
		query,
		topK,
		s.config.Embedder,
		s.config.VectorDriver,
		s.dagLoader,
		s.logger,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{
			Error: err.Error(),
		})
	}

	return c.JSON(output)
}

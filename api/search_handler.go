package api

import (
	"strconv"

	"github.com/gofiber/fiber/v2"

	apisearch "github.com/papercomputeco/tapes/api/search"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// handleSearchEndpoint handles GET /v1/search requests.
// Query parameters:
// * query (required): the search query text
// * top_k (optional, default 5): number of results to return
//
//	@Summary		Semantic search over stored sessions
//	@Description	Embeds the query text, searches the configured vector store, and returns matching sessions with their full conversation branch.
//	@Tags			search
//	@Produce		json
//	@Param			query	query		string	true	"Search query"
//	@Param			top_k	query		int		false	"Maximum number of results to return"	default(5)	minimum(1)
//	@Success		200		{object}	apisearch.Output
//	@Failure		400		{object}	llm.ErrorResponse	"Missing or invalid query parameters"
//	@Failure		503		{object}	llm.ErrorResponse	"Search is not configured"
//	@Failure		500		{object}	llm.ErrorResponse	"Search execution failed"
//	@Router			/v1/search [get]
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

	searcher := apisearch.NewSearcher(
		c.Context(),
		s.config.Embedder,
		s.config.VectorDriver,
		s.driver,
		s.logger,
	)
	output, err := searcher.Search(query, topK)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{
			Error: err.Error(),
		})
	}

	return c.JSON(output)
}

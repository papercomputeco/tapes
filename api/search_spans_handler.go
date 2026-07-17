package api

import (
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/spanembed"
)

// Span search — "find the turn where X happened". The query text is
// embedded and matched against the span-embedding projection (main
// llm spans only, delta-only content), and every hit carries its
// span→trace→turn context so a client can jump straight to the turn.

// SpanSearchResult is one span hit with its trace/turn context.
type SpanSearchResult struct {
	TraceID   string  `json:"trace_id"`
	SpanID    string  `json:"span_id"`
	SessionID string  `json:"session_id,omitempty"`
	Score     float32 `json:"score"`
	// UserPrompt is the prompt of the turn (trace) the span belongs to.
	// Served explicitly (not omitempty) so a synthetic turn's empty prompt
	// reaches consumers as "" rather than a dropped key — see TraceItem.
	UserPrompt string `json:"user_prompt"`
	// Snippet previews the matched span's delta-only text.
	Snippet   string    `json:"snippet,omitempty"`
	Model     string    `json:"model,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// SpanSearchOutput is the span search response.
type SpanSearchOutput struct {
	Query   string             `json:"query"`
	Results []SpanSearchResult `json:"results"`
	Count   int                `json:"count"`
}

// handleSearchSpansEndpoint handles GET /v1/search/spans requests.
//
//	@Summary		Semantic search over span embeddings
//	@ID			searchSpans
//	@Description	Embeds the query text and runs vector similarity over the embedded span projection (main llm spans, delta-only content). Each hit carries span, trace, and turn context.
//	@Tags			search
//	@Produce		json
//	@Param			query			query		string	true	"Search query"
//	@Param			top_k			query		int		false	"Maximum number of results to return"	default(5)	minimum(1)
//	@Param			X-Tapes-Org-Id	header		string	false	"Tenant org UUID (defaults to the nil org)"
//	@Success		200				{object}	SpanSearchOutput
//	@Failure		400				{object}	llm.ErrorResponse	"Missing or invalid query parameters"
//	@Failure		503				{object}	llm.ErrorResponse	"Span search is not configured or not yet initialized"
//	@Failure		500				{object}	llm.ErrorResponse	"Search execution failed"
//	@Router			/v1/search/spans [get]
func (s *Server) handleSearchSpansEndpoint(c *fiber.Ctx) error {
	if s.config.SpanSearcher == nil || s.config.Embedder == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(llm.ErrorResponse{
			Error: "span search is not configured: embedder and span embedding store are required",
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

	embedding, err := s.config.Embedder.Embed(c.Context(), query)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{
			Error: "failed to embed query: " + err.Error(),
		})
	}

	hits, err := s.config.SpanSearcher.Search(c.Context(), orgIDFromCtx(c), embedding, topK)
	if errors.Is(err, spanembed.ErrNotInitialized) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(llm.ErrorResponse{
			Error: err.Error(),
		})
	}
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{
			Error: err.Error(),
		})
	}

	results := make([]SpanSearchResult, 0, len(hits))
	for _, h := range hits {
		results = append(results, SpanSearchResult{
			TraceID:    h.TraceID,
			SpanID:     h.SpanID,
			SessionID:  h.SessionID,
			Score:      h.Score,
			UserPrompt: h.UserPrompt,
			Snippet:    h.Snippet,
			Model:      h.Model,
			StartedAt:  h.StartedAt,
		})
	}

	return c.JSON(SpanSearchOutput{
		Query:   query,
		Results: results,
		Count:   len(results),
	})
}

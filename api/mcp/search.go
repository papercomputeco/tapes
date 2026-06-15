package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/papercomputeco/tapes/pkg/spanembed"
)

var (
	searchToolName    = "search"
	searchDescription = "Semantic search over stored LLM sessions. Embeds the query and runs vector similarity over the span projection (main-conversation LLM spans, delta-only content). Each result is an individual span carrying its session, trace, and span identifiers plus a text snippet, so a client can jump straight to the matched turn."
)

// nilOrgID is the sentinel tenant used when no org is supplied. It mirrors
// the HTTP read surface's default (see api/tenant.go) so a header-less MCP
// request scopes to the same nil-org bucket as a header-less HTTP search.
const nilOrgID = "00000000-0000-0000-0000-000000000000"

// SearchInput represents the input arguments for the MCP search tool.
// It uses jsonschema tags specific to the MCP protocol.
type SearchInput struct {
	Query string `json:"query" jsonschema:"the search query text to find relevant spans"`
	TopK  int    `json:"top_k,omitempty" jsonschema:"number of results to return (default: 5)"`
}

// SearchResult is one span hit with its trace/turn context. It mirrors the
// HTTP /v1/search/spans response shape (api.SpanSearchResult).
type SearchResult struct {
	SessionID string  `json:"session_id,omitempty" jsonschema:"session the span belongs to"`
	TraceID   string  `json:"trace_id" jsonschema:"trace (turn) the span belongs to"`
	SpanID    string  `json:"span_id" jsonschema:"the matched span"`
	Score     float32 `json:"score" jsonschema:"vector similarity score"`
	// UserPrompt is the prompt of the turn (trace) the span belongs to.
	UserPrompt string `json:"user_prompt,omitempty" jsonschema:"prompt of the turn the span belongs to"`
	// Snippet previews the matched span's delta-only text.
	Snippet   string    `json:"snippet,omitempty" jsonschema:"preview of the matched span's text"`
	Model     string    `json:"model,omitempty" jsonschema:"model that produced the span"`
	StartedAt time.Time `json:"started_at" jsonschema:"when the span started"`
}

// SearchOutput is the MCP search response. It mirrors the HTTP
// /v1/search/spans response shape (api.SpanSearchOutput).
type SearchOutput struct {
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
	Count   int            `json:"count"`
}

// handleSearch runs the same span search the HTTP /v1/search/spans handler
// uses: it embeds the query, then runs vector similarity over the span
// projection via the shared SpanSearcher.
func (s *Server) handleSearch(ctx context.Context, _ *mcp.CallToolRequest, input SearchInput) (*mcp.CallToolResult, SearchOutput, error) {
	output, err := s.searchSpans(ctx, input.Query, input.TopK)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Search failed: %v", err)},
			},
		}, SearchOutput{}, nil
	}

	// Serialize the structured output as JSON for the text field.
	// Per MCP spec: tools returning structured content should also return
	// serialized JSON in a TextContent block for backwards compatibility.
	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Failed to serialize results: %v", err)},
			},
		}, SearchOutput{}, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, *output, nil
}

// searchSpans embeds the query and runs the span search, mirroring the HTTP
// handleSearchSpansEndpoint path so the two surfaces share behavior.
func (s *Server) searchSpans(ctx context.Context, query string, topK int) (*SearchOutput, error) {
	if topK <= 0 {
		topK = 5
	}

	s.config.Logger.Debug("mcp span search request",
		"query", query,
		"topK", topK,
	)

	embedding, err := s.config.Embedder.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to embed query: %w", err)
	}

	hits, err := s.config.SpanSearcher.Search(ctx, nilOrgID, embedding, topK)
	if errors.Is(err, spanembed.ErrNotInitialized) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed to search spans: %w", err)
	}

	results := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		results = append(results, SearchResult{
			SessionID:  h.SessionID,
			TraceID:    h.TraceID,
			SpanID:     h.SpanID,
			Score:      h.Score,
			UserPrompt: h.UserPrompt,
			Snippet:    h.Snippet,
			Model:      h.Model,
			StartedAt:  h.StartedAt,
		})
	}

	return &SearchOutput{
		Query:   query,
		Results: results,
		Count:   len(results),
	}, nil
}

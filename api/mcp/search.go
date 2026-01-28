package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/vector"
)

var (
	searchToolName    = "search"
	searchDescription = "Search over stored LLM sessions using semantic search. Returns the most relevant sessions based on the query text, including full conversation history."
)

// SearchInput represents the input arguments for the search tool.
type SearchInput struct {
	Query string `json:"query" jsonschema:"the search query text to find relevant sessions"`
	TopK  int    `json:"top_k,omitempty" jsonschema:"number of results to return (default: 5)"`
}

// SearchResult represents a single search result.
type SearchResult struct {
	Hash    string  `json:"hash"`
	Score   float32 `json:"score"`
	Role    string  `json:"role"`
	Preview string  `json:"preview"`
	Depth   int     `json:"depth"`
	Lineage []Turn  `json:"lineage"`
}

// Turn represents a single turn in a conversation.
type Turn struct {
	Hash string `json:"hash"`
	Role string `json:"role"`
	Text string `json:"text"`
}

// SearchOutput represents the output of the search tool.
type SearchOutput struct {
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
	Count   int            `json:"count"`
}

// handleSearch processes a search request.
func (s *Server) handleSearch(ctx context.Context, req *mcp.CallToolRequest, input SearchInput) (*mcp.CallToolResult, SearchOutput, error) {
	logger := s.config.Logger

	// Default topK if not specified
	topK := input.TopK
	if topK <= 0 {
		topK = 5
	}

	logger.Debug("MCP search request",
		zap.String("query", input.Query),
		zap.Int("topK", topK),
	)

	// Embed the query
	queryEmbedding, err := s.config.Embedder.Embed(ctx, input.Query)
	if err != nil {
		logger.Error("failed to embed query", zap.Error(err))
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Failed to embed query: %v", err)},
			},
		}, SearchOutput{}, nil
	}

	// Query the vector store
	results, err := s.config.VectorDriver.Query(ctx, queryEmbedding, topK)
	if err != nil {
		logger.Error("failed to query vector store", zap.Error(err))
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Failed to query vector store: %v", err)},
			},
		}, SearchOutput{}, nil
	}

	// Build search results with lineage
	searchResults := make([]SearchResult, 0, len(results))
	for _, result := range results {
		lineage, err := s.config.StorageDriver.Ancestry(ctx, result.Hash)
		if err != nil {
			logger.Warn("failed to get lineage for result",
				zap.String("hash", result.Hash),
				zap.Error(err),
			)
			continue
		}

		searchResult := buildSearchResult(result, lineage)
		searchResults = append(searchResults, searchResult)
	}

	output := SearchOutput{
		Query:   input.Query,
		Results: searchResults,
		Count:   len(searchResults),
	}

	// Serialize the structured output as JSON for the text field
	// Per MCP spec: tools returning structured content should also return
	// serialized JSON in a TextContent block for backwards compatibility
	jsonBytes, err := json.Marshal(output)
	if err != nil {
		logger.Error("failed to marshal search output", zap.Error(err))
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
	}, output, nil
}

// buildSearchResult converts a vector query result and lineage into a SearchResult.
func buildSearchResult(result vector.QueryResult, lineage []*merkle.Node) SearchResult {
	turns := make([]Turn, len(lineage))

	// Build turns from root to matched node (reverse order)
	for i := len(lineage) - 1; i >= 0; i-- {
		node := lineage[i]
		idx := len(lineage) - 1 - i

		turns[idx] = Turn{
			Hash: node.Hash,
			Role: node.Bucket.Role,
			Text: node.Bucket.ExtractText(),
		}
	}

	// Get preview from the matched node (first in lineage)
	preview := ""
	role := ""
	if len(lineage) > 0 {
		matchedNode := lineage[0]
		preview = matchedNode.Bucket.ExtractText()
		role = matchedNode.Bucket.Role
	}

	return SearchResult{
		Hash:    result.Hash,
		Score:   result.Score,
		Role:    role,
		Preview: preview,
		Depth:   len(lineage),
		Lineage: turns,
	}
}

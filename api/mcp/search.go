package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	apisearch "github.com/papercomputeco/tapes/api/search"
)

var (
	searchToolName    = "search"
	searchDescription = "Search over stored LLM sessions using semantic search. Returns the most relevant sessions based on the query text, including the full conversation branch (ancestors and descendants)."
)

// SearchInput represents the input arguments for the MCP search tool.
// It uses jsonschema tags specific to the MCP protocol.
type SearchInput struct {
	Query string `json:"query" jsonschema:"the search query text to find relevant sessions"`
	TopK  int    `json:"top_k,omitempty" jsonschema:"number of results to return (default: 5)"`
}

// handleSearch processes a search request via MCP.
// It delegates to the shared search package for the core search logic.
func (s *Server) handleSearch(ctx context.Context, _ *mcp.CallToolRequest, input SearchInput) (*mcp.CallToolResult, apisearch.Output, error) {
	searcher := apisearch.NewSearcher(
		ctx,
		s.config.Embedder,
		s.config.VectorDriver,
		s.config.DagLoader,
		s.config.Logger,
	)
	output, err := searcher.Search(input.Query, input.TopK)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Search failed: %v", err)},
			},
		}, apisearch.Output{}, nil
	}

	// Serialize the structured output as JSON for the text field
	// Per MCP spec: tools returning structured content should also return
	// serialized JSON in a TextContent block for backwards compatibility
	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Failed to serialize results: %v", err)},
			},
		}, apisearch.Output{}, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, *output, nil
}

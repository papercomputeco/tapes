// Package search provides shared search types and logic for semantic search
// over stored LLM sessions. It is used by both the REST API endpoint and
// the MCP server tool.
package search

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/vector"
)

// SearchInput represents the input arguments for a search request.
type SearchInput struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k,omitempty"`
}

// SearchResult represents a single search result.
type SearchResult struct {
	Hash    string  `json:"hash"`
	Score   float32 `json:"score"`
	Role    string  `json:"role"`
	Preview string  `json:"preview"`
	Turns   int     `json:"turns"`
	Branch  []Turn  `json:"branch"`
}

// Turn represents a single turn in a conversation.
type Turn struct {
	Hash    string `json:"hash"`
	Role    string `json:"role"`
	Text    string `json:"text"`
	Matched bool   `json:"matched,omitempty"`
}

// SearchOutput represents the output of a search operation.
type SearchOutput struct {
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
	Count   int            `json:"count"`
}

// Search performs a semantic search over stored LLM sessions.
// It embeds the query text, queries the vector store for similar documents,
// then loads the full conversation branch from the Merkle DAG for each result.
func Search(
	ctx context.Context,
	query string,
	topK int,
	embedder embeddings.Embedder,
	vectorDriver vector.Driver,
	dagLoader merkle.DagLoader,
	logger *zap.Logger,
) (*SearchOutput, error) {
	if topK <= 0 {
		topK = 5
	}

	logger.Debug("search request",
		zap.String("query", query),
		zap.Int("topK", topK),
	)

	// Embed the query
	queryEmbedding, err := embedder.Embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to embed query: %w", err)
	}

	// Query the vector store
	results, err := vectorDriver.Query(ctx, queryEmbedding, topK)
	if err != nil {
		return nil, fmt.Errorf("failed to query vector store: %w", err)
	}

	// Build search results with full branch using merkle.LoadDag
	searchResults := make([]SearchResult, 0, len(results))
	for _, result := range results {
		dag, err := merkle.LoadDag(ctx, dagLoader, result.Hash)
		if err != nil {
			logger.Warn("failed to load branch for result",
				zap.String("hash", result.Hash),
				zap.Error(err),
			)
			continue
		}

		searchResult := BuildSearchResult(result, dag)
		searchResults = append(searchResults, searchResult)
	}

	return &SearchOutput{
		Query:   query,
		Results: searchResults,
		Count:   len(searchResults),
	}, nil
}

// BuildSearchResult converts a vector query result and DAG into a SearchResult.
func BuildSearchResult(result vector.QueryResult, dag *merkle.Dag) SearchResult {
	turns := []Turn{}
	preview := ""
	role := ""

	// Build turns from the DAG using Walk (depth-first from root to leaves)
	dag.Walk(func(node *merkle.DagNode) (bool, error) {
		isMatched := node.Hash == result.Hash
		turns = append(turns, Turn{
			Hash:    node.Hash,
			Role:    node.Bucket.Role,
			Text:    node.Bucket.ExtractText(),
			Matched: isMatched,
		})

		// Get preview from the matched node
		if isMatched {
			preview = node.Bucket.ExtractText()
			role = node.Bucket.Role
		}
		return true, nil
	})

	return SearchResult{
		Hash:    result.Hash,
		Score:   result.Score,
		Role:    role,
		Preview: preview,
		Turns:   len(turns),
		Branch:  turns,
	}
}

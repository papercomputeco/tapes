// Package search provides shared search types and logic for semantic search
// over stored LLM sessions. It is used by both the REST API endpoint and
// the MCP server tool.
package search

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/vector"
)

// Input represents the input arguments for a search request.
type Input struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k,omitempty"`
}

// Result represents a single search result.
type Result struct {
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

// Output represents the output of a search operation.
type Output struct {
	Query   string   `json:"query"`
	Results []Result `json:"results"`
	Count   int      `json:"count"`
}

type Searcher struct {
	ctx context.Context

	embedder     embeddings.Embedder
	vectorDriver vector.Driver
	dagLoader    merkle.DagLoader
	logger       *slog.Logger
}

func NewSearcher(
	ctx context.Context,
	embedder embeddings.Embedder,
	vectorDriver vector.Driver,
	dagLoader merkle.DagLoader,
	log *slog.Logger,
) *Searcher {
	return &Searcher{
		ctx,
		embedder,
		vectorDriver,
		dagLoader,
		log,
	}
}

// Search performs a semantic search over stored LLM sessions.
// It embeds the query text, queries the vector store for similar documents,
// then loads the full conversation branch from the Merkle DAG for each result.
func (s *Searcher) Search(
	query string,
	topK int,
) (*Output, error) {
	if topK <= 0 {
		topK = 5
	}

	s.logger.Debug("search request",
		"query", query,
		"topK", topK,
	)

	// Embed the query
	queryEmbedding, err := s.embedder.Embed(s.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to embed query: %w", err)
	}

	// Query the vector store
	results, err := s.vectorDriver.Query(s.ctx, queryEmbedding, topK)
	if err != nil {
		return nil, fmt.Errorf("failed to query vector store: %w", err)
	}

	// Build search results with full branch using merkle.LoadDag
	searchResults := make([]Result, 0, len(results))
	for _, result := range results {
		dag, err := merkle.LoadDag(s.ctx, s.dagLoader, result.Hash)
		if err != nil {
			s.logger.Warn("failed to load branch for result",
				"hash", result.Hash,
				"error", err,
			)
			continue
		}

		searchResult := s.BuildResult(result, dag)
		searchResults = append(searchResults, searchResult)
	}

	return &Output{
		Query:   query,
		Results: searchResults,
		Count:   len(searchResults),
	}, nil
}

// BuildResult converts a vector query result and DAG into a Result.
func (s *Searcher) BuildResult(result vector.QueryResult, dag *merkle.Dag) Result {
	turns := []Turn{}
	preview := ""
	role := ""

	// Build turns from the DAG using Walk (depth-first from root to leaves)
	err := dag.Walk(func(node *merkle.DagNode) (bool, error) {
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
	if err != nil {
		s.logger.Error(
			"could not walk graph during search",
			"error", err,
		)
	}

	return Result{
		Hash:    result.Hash,
		Score:   result.Score,
		Role:    role,
		Preview: preview,
		Turns:   len(turns),
		Branch:  turns,
	}
}

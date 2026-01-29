// Package searchcmder provides the search command for semantic search over sessions.
package searchcmder

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/embeddings/ollama"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	"github.com/papercomputeco/tapes/pkg/vector"
	vectorutils "github.com/papercomputeco/tapes/pkg/vector/utils"
)

type searchCommander struct {
	query string
	topK  int

	vectorStoreProvider string
	vectorStoreTarget   string

	embeddingProvider string
	embeddingTarget   string
	embeddingModel    string

	sqlitePath string
	debug      bool
	logger     *zap.Logger
}

const searchLongDesc string = `Search session data.

Search over stored sessions, returning the most relevant sessions based on the
query text. Requires a configured vector store provider, embedding provider,
and storage provider.

For each result, the full session branch is displayed, including all ancestors
(from root to matched node) and all descendants (from matched node to leaves).

Example:
  tapes search "how to configure logging" \
	--vector-store-provider chroma \
	--vector-store-target http://localhost:8000 \
	--embedding-provider ollama \
	--embedding-target http://localhost:11434 \
	--embedding-model nomic-embed-text \
	--sqlite ./tapes.sqlite`

const searchShortDesc string = "Search session data"

func NewSearchCmd() *cobra.Command {
	cmder := &searchCommander{}

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: searchShortDesc,
		Long:  searchLongDesc,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmder.query = args[0]

			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}

			return cmder.run()
		},
	}

	cmd.Flags().IntVarP(&cmder.topK, "top", "k", 5, "Number of results to return")
	cmd.Flags().StringVar(&cmder.vectorStoreProvider, "vector-store-provider", "", "Vector store provider type (e.g., chroma)")
	cmd.Flags().StringVar(&cmder.vectorStoreTarget, "vector-store-target", "", "Vector store URL (e.g., http://localhost:8000)")
	cmd.Flags().StringVar(&cmder.embeddingProvider, "embedding-provider", "ollama", "Embedding provider type (e.g., ollama)")
	cmd.Flags().StringVar(&cmder.embeddingTarget, "embedding-target", ollama.DefaultBaseURL, "Embedding provider URL")
	cmd.Flags().StringVar(&cmder.embeddingModel, "embedding-model", ollama.DefaultEmbeddingModel, "Embedding model name (e.g., nomic-embed-text)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (required)")

	cmd.MarkFlagRequired("vector-store-provider")
	cmd.MarkFlagRequired("vector-store-target")
	cmd.MarkFlagRequired("embedding-provider")
	cmd.MarkFlagRequired("embedding-target")
	cmd.MarkFlagRequired("embedding-model")
	cmd.MarkFlagRequired("sqlite")

	return cmd
}

func (c *searchCommander) run() error {
	ctx := context.Background()
	c.logger = logger.NewLogger(c.debug)
	defer c.logger.Sync()

	embedder, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: c.embeddingProvider,
		TargetURL:    c.embeddingTarget,
		Model:        c.embeddingModel,
	})
	if err != nil {
		return fmt.Errorf("could not create embedder: %w", err)
	}
	defer embedder.Close()

	// Embed the query
	c.logger.Debug("embedding query", zap.String("query", c.query))
	queryEmbedding, err := embedder.Embed(ctx, c.query)
	if err != nil {
		return fmt.Errorf("embedding query: %w", err)
	}

	// Create vector driver and query
	vectorDriver, err := vectorutils.NewVectorDriver(&vectorutils.NewVectorDriverOpts{
		ProviderType: c.vectorStoreProvider,
		TargetURL:    c.vectorStoreTarget,
		Logger:       c.logger,
	})
	if err != nil {
		return fmt.Errorf("creating vector driver: %w", err)
	}
	defer vectorDriver.Close()

	c.logger.Debug("querying vector store", zap.Int("topK", c.topK))
	results, err := vectorDriver.Query(ctx, queryEmbedding, c.topK)
	if err != nil {
		return fmt.Errorf("querying vector store: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No results found.")
		return nil
	}

	// Create storage driver to look up full nodes
	dagLoader, err := sqlite.NewSQLiteDriver(c.sqlitePath)
	if err != nil {
		return fmt.Errorf("opening SQLite database: %w", err)
	}
	defer dagLoader.Close()

	// Print results header
	fmt.Printf("\nSearch Results for: %q\n", c.query)
	fmt.Println(strings.Repeat("=", 60))

	// For each result, load the full branch using merkle.LoadDag and print
	for i, result := range results {
		dag, err := merkle.LoadDag(ctx, dagLoader, result.Hash)
		if err != nil {
			c.logger.Warn("failed to load branch for result",
				zap.String("hash", result.Hash),
				zap.Error(err),
			)
			continue
		}

		c.printResult(i+1, result, dag)
	}

	return nil
}

func (c *searchCommander) printResult(rank int, result vector.QueryResult, dag *merkle.Dag) {
	fmt.Printf("\n[%d] Score: %.4f\n", rank, result.Score)
	fmt.Printf("    Hash: %s\n", result.Hash)

	if dag == nil || dag.Size() == 0 {
		fmt.Println("    (No session found)")
		return
	}

	// Find the matched node in the DAG to get preview info
	matchedNode := dag.Get(result.Hash)
	if matchedNode != nil {
		fmt.Printf("    Role: %s\n", matchedNode.Bucket.Role)
		fmt.Printf("    Preview: %s\n", matchedNode.Bucket.ExtractText())
	}

	fmt.Printf("\n    Session (%d turns):\n", dag.Size())

	// Print the full branch using Walk (depth-first from root to leaves)
	dag.Walk(func(node *merkle.DagNode) (bool, error) {
		prefix := "    |-- "
		if node.Hash == result.Hash {
			prefix = "    >>> " // Mark the matched node
		}

		text := node.Bucket.ExtractText()
		if text == "" {
			text = "(no text content)"
		}

		fmt.Printf("%s[%s] %s - %s\n", prefix, node.Bucket.Role, text, node.Hash)
		return true, nil
	})

	fmt.Println()
}

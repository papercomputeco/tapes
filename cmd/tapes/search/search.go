// Package searchcmder provides the search command for semantic search over sessions.
package searchcmder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	apisearch "github.com/papercomputeco/tapes/api/search"
	"github.com/papercomputeco/tapes/pkg/logger"
)

type searchCommander struct {
	query string
	topK  int

	apiTarget string

	debug  bool
	logger *zap.Logger
}

const searchLongDesc string = `Search session data via the Tapes API.

Search over stored sessions, returning the most relevant sessions based on the
query text. Requires a running Tapes API server with search configured
(vector store and embedder).

For each result, the full session branch is displayed, including all ancestors
(from root to matched node) and all descendants (from matched node to leaves).

Example:
  tapes search "how to configure logging"
  tapes search "error handling patterns" --api-target http://localhost:8081
  tapes search "how to configure logging" --top 10`

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
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			return cmder.run()
		},
	}

	cmd.Flags().IntVarP(&cmder.topK, "top", "k", 5, "Number of results to return")
	cmd.Flags().StringVar(&cmder.apiTarget, "api-target", "http://localhost:8081", "Tapes API server URL")

	return cmd
}

func (c *searchCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer func() { _ = c.logger.Sync() }()
	client := &http.Client{}

	c.logger.Debug("searching via API",
		zap.String("api_target", c.apiTarget),
		zap.String("query", c.query),
		zap.Int("topK", c.topK),
	)

	// Build the request URL
	searchURL, err := url.Parse(c.apiTarget)
	if err != nil {
		return fmt.Errorf("invalid API target URL: %w", err)
	}
	searchURL.Path = "/v1/search"
	q := searchURL.Query()
	q.Set("query", c.query)
	q.Set("top_k", strconv.Itoa(c.topK))
	searchURL.RawQuery = q.Encode()

	c.logger.Debug("requesting search", zap.String("url", searchURL.String()))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, searchURL.String(), nil)
	if err != nil {
		return fmt.Errorf("creating search request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Tapes API at %s: %w", c.apiTarget, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("search request failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var output apisearch.Output
	if err := json.Unmarshal(body, &output); err != nil {
		return fmt.Errorf("failed to parse search response: %w", err)
	}

	if output.Count == 0 {
		fmt.Println("No results found.")
		return nil
	}

	// Print results header
	fmt.Printf("\nSearch Results for: %q\n", output.Query)
	fmt.Println(strings.Repeat("=", 60))

	for i, result := range output.Results {
		c.printResult(i+1, result)
	}

	return nil
}

func (c *searchCommander) printResult(rank int, result apisearch.Result) {
	fmt.Printf("\n[%d] Score: %.4f\n", rank, result.Score)
	fmt.Printf("    Hash: %s\n", result.Hash)

	if result.Turns == 0 {
		fmt.Println("    (No session found)")
		return
	}

	fmt.Printf("    Role: %s\n", result.Role)
	fmt.Printf("    Preview: %s\n", result.Preview)

	fmt.Printf("\n    Session (%d turns):\n", result.Turns)

	for _, turn := range result.Branch {
		prefix := "    |-- "
		if turn.Matched {
			prefix = "    >>> " // Mark the matched node
		}

		text := turn.Text
		if text == "" {
			text = "(no text content)"
		}

		fmt.Printf("%s[%s] %s - %s\n", prefix, turn.Role, text, turn.Hash)
	}

	fmt.Println()
}

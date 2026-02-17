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

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	apisearch "github.com/papercomputeco/tapes/api/search"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
)

var (
	rankStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Bold(true)
	scoreStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	hashStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	roleStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	previewStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
	matchedStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Bold(true)
	branchStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	headerStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Bold(true)
	dimStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
)

type searchCommander struct {
	query string
	topK  int
	quiet bool

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

Use --quiet to output only leaf hashes, one per line. This is useful for piping
into other commands like tapes skill generate.

Example:
  tapes search "how to configure logging"
  tapes search "error handling patterns" --api-target http://localhost:8081
  tapes search "how to configure logging" --top 10
  tapes search "gum glow charm" --quiet
  tapes skill generate $(tapes search "charm CLI" --quiet --top 1) --name charm-patterns`

const searchShortDesc string = "Search session data"

func NewSearchCmd() *cobra.Command {
	cmder := &searchCommander{}

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: searchShortDesc,
		Long:  searchLongDesc,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			cfger, err := config.NewConfiger(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			cfg, err := cfger.LoadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			if !cmd.Flags().Changed("api-target") {
				cmder.apiTarget = cfg.Client.APITarget
			}
			return nil
		},
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

	defaults := config.NewDefaultConfig()
	cmd.Flags().IntVarP(&cmder.topK, "top", "k", 5, "Number of results to return")
	cmd.Flags().BoolVarP(&cmder.quiet, "quiet", "q", false, "Output only leaf hashes, one per line (for piping)")
	cmd.Flags().StringVar(&cmder.apiTarget, "api-target", defaults.Client.APITarget, "Tapes API server URL")

	return cmd
}

func (c *searchCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer func() { _ = c.logger.Sync() }()

	output, err := SearchAPI(c.apiTarget, c.query, c.topK)
	if err != nil {
		return err
	}

	if output.Count == 0 {
		if !c.quiet {
			fmt.Println("No results found.")
		}
		return nil
	}

	if c.quiet {
		for _, result := range output.Results {
			fmt.Println(LeafHash(result))
		}
		return nil
	}

	fmt.Printf("\n%s %s\n\n",
		headerStyle.Render("Search Results for:"),
		hashStyle.Render(fmt.Sprintf("%q", output.Query)),
	)

	for i, result := range output.Results {
		c.printResult(i+1, result)
	}

	return nil
}

func (c *searchCommander) printResult(rank int, result apisearch.Result) {
	fmt.Printf("  %s  %s  %s\n",
		rankStyle.Render(fmt.Sprintf("#%d", rank)),
		scoreStyle.Render(fmt.Sprintf("score: %.4f", result.Score)),
		hashStyle.Render(result.Hash),
	)

	if result.Turns == 0 {
		fmt.Printf("  %s\n\n", dimStyle.Render("(no session found)"))
		return
	}

	preview := result.Preview
	if len(preview) > 80 {
		preview = preview[:77] + "..."
	}
	preview = strings.ReplaceAll(preview, "\n", " ")

	fmt.Printf("  %s %s\n", roleStyle.Render(result.Role+":"), previewStyle.Render(preview))
	fmt.Printf("  %s\n", dimStyle.Render(fmt.Sprintf("%d turns", result.Turns)))

	if len(result.Branch) > 0 {
		for _, turn := range result.Branch {
			text := turn.Text
			if text == "" {
				text = "(no text content)"
			}
			if len(text) > 60 {
				text = text[:57] + "..."
			}
			text = strings.ReplaceAll(text, "\n", " ")

			if turn.Matched {
				fmt.Printf("  %s %s %s %s\n",
					matchedStyle.Render(">>>"),
					roleStyle.Render("["+turn.Role+"]"),
					previewStyle.Render(text),
					dimStyle.Render(turn.Hash[:12]),
				)
			} else {
				fmt.Printf("  %s %s %s %s\n",
					branchStyle.Render(" ├─"),
					roleStyle.Render("["+turn.Role+"]"),
					branchStyle.Render(text),
					dimStyle.Render(turn.Hash[:12]),
				)
			}
		}
	}

	fmt.Println()
}

// SearchAPI calls the tapes search API and returns the parsed output.
// Exported so other commands (e.g. skill generate --search) can reuse it.
func SearchAPI(apiTarget, query string, topK int) (*apisearch.Output, error) {
	searchURL, err := url.Parse(apiTarget)
	if err != nil {
		return nil, fmt.Errorf("invalid API target URL: %w", err)
	}
	searchURL.Path = "/v1/search"
	q := searchURL.Query()
	q.Set("query", query)
	q.Set("top_k", strconv.Itoa(topK))
	searchURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, searchURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Tapes API at %s: %w", apiTarget, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("search request failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	var output apisearch.Output
	if err := json.Unmarshal(body, &output); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	return &output, nil
}

// LeafHash returns the leaf (last) hash from a search result's branch.
// Falls back to the matched node hash if the branch is empty.
func LeafHash(result apisearch.Result) string {
	if len(result.Branch) > 0 {
		return result.Branch[len(result.Branch)-1].Hash
	}
	return result.Hash
}

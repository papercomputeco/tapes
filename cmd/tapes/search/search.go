// Package searchcmder provides the search command for semantic search over sessions.
package searchcmder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	apisearch "github.com/papercomputeco/tapes/api/search"
	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type searchCommander struct {
	flags config.FlagSet

	query       string
	topK        int
	quiet       bool
	spans       bool
	orgID       string
	apiTarget   string
	debug       bool
	resultCount int

	logger *slog.Logger
}

var searchFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const searchLongDesc string = `Search session data via the Tapes API.

Search over stored sessions, returning the most relevant sessions based on the
query text. Requires a running Tapes API server with search configured
(vector store and embedder).

For each result, the full session branch is displayed, including all ancestors
(from root to matched node) and all descendants (from matched node to leaves).

Use --quiet to output only leaf hashes, one per line. This is useful for piping
into other commands like tapes skill generate.

Use --spans to search the span projection instead: hits are individual
main-conversation llm spans with their trace and turn context ("find the
turn where X happened"). Requires the span embeddings to have been written
(tapes serve derive-worker --embed-spans, or tapes dev embed-spans).

Example:
  tapes search "how to configure logging"
  tapes search "error handling patterns" --api-target http://localhost:8081
  tapes search "how to configure logging" --top 10
  tapes search "gum glow charm" --quiet
  tapes search "retry backoff change" --spans
  tapes skill generate $(tapes search "charm CLI" --quiet --top 1) --name charm-patterns`

const searchShortDesc string = "Search session data"

func NewSearchCmd() *cobra.Command {
	cmder := &searchCommander{
		flags: searchFlags,
	}

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: searchShortDesc,
		Long:  searchLongDesc,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagAPITarget,
			})

			cmder.apiTarget = v.GetString("client.api_target")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cmder.query = args[0]

			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			if err := cmder.run(); err != nil {
				return err
			}
			telemetry.FromContext(cmd.Context()).CaptureSearch(cmder.resultCount)
			return nil
		},
	}

	cmd.Flags().IntVarP(&cmder.topK, "top", "k", 5, "Number of results to return")
	cmd.Flags().BoolVarP(&cmder.quiet, "quiet", "q", false, "Output only leaf hashes, one per line (for piping)")
	cmd.Flags().BoolVar(&cmder.spans, "spans", false, "Search span embeddings (per-turn hits with trace context)")
	cmd.Flags().StringVar(&cmder.orgID, "org", "", "Tenant org UUID sent as X-Tapes-Org-Id (default: the nil org)")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *searchCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	if c.spans {
		return c.runSpans()
	}

	output, err := SearchAPI(c.apiTarget, c.query, c.topK)
	if err != nil {
		return err
	}
	c.resultCount = output.Count

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
		cliui.HeaderStyle.Render("Search Results for:"),
		cliui.HashStyle.Render(fmt.Sprintf("%q", output.Query)),
	)

	for i, result := range output.Results {
		c.printResult(i+1, result)
	}

	return nil
}

func (c *searchCommander) printResult(rank int, result apisearch.Result) {
	fmt.Printf("  %s  %s  %s\n",
		cliui.RankStyle.Render(fmt.Sprintf("#%d", rank)),
		cliui.ScoreStyle.Render(fmt.Sprintf("score: %.4f", result.Score)),
		cliui.HashStyle.Render(result.Hash),
	)

	if result.Turns == 0 {
		fmt.Printf("  %s\n\n", cliui.DimStyle.Render("(no session found)"))
		return
	}

	preview := result.Preview
	if len(preview) > 80 {
		preview = preview[:77] + "..."
	}
	preview = strings.ReplaceAll(preview, "\n", " ")

	fmt.Printf("  %s %s\n", cliui.RoleStyle.Render(result.Role+":"), cliui.PreviewStyle.Render(preview))
	fmt.Printf("  %s\n", cliui.DimStyle.Render(fmt.Sprintf("%d turns", result.Turns)))

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
					cliui.MatchedStyle.Render(">>>"),
					cliui.RoleStyle.Render("["+turn.Role+"]"),
					cliui.PreviewStyle.Render(text),
					cliui.DimStyle.Render(turn.Hash[:12]),
				)
			} else {
				fmt.Printf("  %s %s %s %s\n",
					cliui.BranchStyle.Render(" ├─"),
					cliui.RoleStyle.Render("["+turn.Role+"]"),
					cliui.BranchStyle.Render(text),
					cliui.DimStyle.Render(turn.Hash[:12]),
				)
			}
		}
	}

	fmt.Println()
}

// runSpans executes a span search and renders per-span hits with
// their trace/turn context.
func (c *searchCommander) runSpans() error {
	output, err := SearchSpansAPI(c.apiTarget, c.query, c.orgID, c.topK)
	if err != nil {
		return err
	}
	c.resultCount = output.Count

	if output.Count == 0 {
		if !c.quiet {
			fmt.Println("No results found.")
		}
		return nil
	}

	if c.quiet {
		for _, result := range output.Results {
			fmt.Printf("%s %s\n", result.TraceID, result.SpanID)
		}
		return nil
	}

	fmt.Printf("\n%s %s\n\n",
		cliui.HeaderStyle.Render("Span Search Results for:"),
		cliui.HashStyle.Render(fmt.Sprintf("%q", output.Query)),
	)
	for i, result := range output.Results {
		c.printSpanResult(i+1, result)
	}
	return nil
}

func (c *searchCommander) printSpanResult(rank int, result api.SpanSearchResult) {
	fmt.Printf("  %s  %s  %s\n",
		cliui.RankStyle.Render(fmt.Sprintf("#%d", rank)),
		cliui.ScoreStyle.Render(fmt.Sprintf("score: %.4f", result.Score)),
		cliui.HashStyle.Render(result.TraceID+" / "+result.SpanID),
	)

	prompt := strings.ReplaceAll(result.UserPrompt, "\n", " ")
	if prompt == "" {
		prompt = "(synthetic turn)"
	}
	if len(prompt) > 80 {
		prompt = prompt[:77] + "..."
	}
	fmt.Printf("  %s %s\n", cliui.RoleStyle.Render("turn:"), cliui.PreviewStyle.Render(prompt))

	snippet := strings.ReplaceAll(result.Snippet, "\n", " ")
	if len(snippet) > 100 {
		snippet = snippet[:97] + "..."
	}
	if snippet != "" {
		fmt.Printf("  %s %s\n", cliui.BranchStyle.Render(" ├─"), cliui.BranchStyle.Render(snippet))
	}

	meta := result.StartedAt.Format(time.RFC3339)
	if result.SessionID != "" {
		meta += "  session " + result.SessionID
	}
	fmt.Printf("  %s\n\n", cliui.DimStyle.Render(meta))
}

// SearchSpansAPI calls the tapes span search API and returns the
// parsed output. orgID may be empty (the nil tenant).
func SearchSpansAPI(apiTarget, query, orgID string, topK int) (*api.SpanSearchOutput, error) {
	body, err := getSearch(apiTarget, "/v1/search/spans", query, orgID, topK)
	if err != nil {
		return nil, err
	}
	var output api.SpanSearchOutput
	if err := json.Unmarshal(body, &output); err != nil {
		return nil, fmt.Errorf("failed to parse span search response: %w", err)
	}
	return &output, nil
}

// SearchAPI calls the tapes search API and returns the parsed output.
// Exported so other commands (e.g. skill generate --search) can reuse it.
func SearchAPI(apiTarget, query string, topK int) (*apisearch.Output, error) {
	body, err := getSearch(apiTarget, "/v1/search", query, "", topK)
	if err != nil {
		return nil, err
	}
	var output apisearch.Output
	if err := json.Unmarshal(body, &output); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}
	return &output, nil
}

// getSearch issues one search GET against the tapes API and returns
// the raw response body.
func getSearch(apiTarget, path, query, orgID string, topK int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	searchURL, err := url.Parse(apiTarget)
	if err != nil {
		return nil, fmt.Errorf("invalid API target URL: %w", err)
	}
	searchURL.Path = path
	q := searchURL.Query()
	q.Set("query", query)
	q.Set("top_k", strconv.Itoa(topK))
	searchURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, searchURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}
	if orgID != "" {
		req.Header.Set("X-Tapes-Org-Id", orgID)
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
	return body, nil
}

// LeafHash returns the leaf (last) hash from a search result's branch.
// Falls back to the matched node hash if the branch is empty.
func LeafHash(result apisearch.Result) string {
	if len(result.Branch) > 0 {
		return result.Branch[len(result.Branch)-1].Hash
	}
	return result.Hash
}

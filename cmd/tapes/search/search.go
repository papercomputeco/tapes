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

Searches the span projection: hits are individual main-conversation llm
spans with their trace and turn context ("find the turn where X
happened"). Requires a running Tapes API server with the span embeddings
written (tapes serve, tapes serve embed-worker, or the tapes dev
embed-spans backfill).

Use --quiet to output only session IDs, one per line, deduplicated in
score order. This is useful for piping into other commands like
tapes skill generate.

Example:
  tapes search "how to configure logging"
  tapes search "error handling patterns" --api-target http://localhost:8081
  tapes search "how to configure logging" --top 10
  tapes search "gum glow charm" --quiet
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
	cmd.Flags().BoolVarP(&cmder.quiet, "quiet", "q", false, "Output only session IDs, one per line (for piping)")
	cmd.Flags().BoolVar(&cmder.spans, "spans", false, "Deprecated: span search is the default; this flag is a no-op")
	_ = cmd.Flags().MarkDeprecated("spans", "span search is the default mode now")
	cmd.Flags().StringVar(&cmder.orgID, "org", "", "Tenant org UUID sent as X-Tapes-Org-Id (default: the nil org)")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *searchCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	// Span search is the only mode; the deprecated --spans flag is
	// accepted as a no-op for muscle memory.
	return c.runSpans()
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
		// Session IDs, deduplicated in score order — the shape
		// tapes skill generate takes as positional arguments.
		seen := map[string]bool{}
		for _, result := range output.Results {
			if result.SessionID == "" || seen[result.SessionID] {
				continue
			}
			seen[result.SessionID] = true
			fmt.Println(result.SessionID)
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

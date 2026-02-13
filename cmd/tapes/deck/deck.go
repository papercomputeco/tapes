// Package deckcmder provides the deck command for session ROI dashboards.
package deckcmder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/sqlitepath"
	"github.com/papercomputeco/tapes/pkg/deck"
)

const (
	deckLongDesc = `Deck is an ROI dashboard for agent sessions.

Summarize recent sessions with a TUI and drill down into a single session.

Examples:
  tapes deck
  tapes deck --since 24h
  tapes deck --from 2026-01-30 --to 2026-01-31
  tapes deck --sort cost --model claude-sonnet-4.5
  tapes deck --session sess_a8f2c1d3
  tapes deck --web
  tapes deck --web --insights --insights-model gemma3:latest
  tapes deck --web --port 9999
  tapes deck --pricing ./pricing.json
  tapes deck --demo
  tapes deck --demo --overwrite
  tapes deck -m
  tapes deck -m -f
`
	deckShortDesc = "Deck - ROI dashboard for agent sessions"
	sortDirDesc   = "desc"
)

type deckCommander struct {
	sqlitePath     string
	pricingPath    string
	since          string
	from           string
	to             string
	sort           string
	sortDir        string
	model          string
	status         string
	session        string
	refresh        uint
	web            bool
	port           int
	insights       bool
	insightsModel  string
	insightsTarget string
	demo           bool
	overwrite      bool
}

func NewDeckCmd() *cobra.Command {
	cmder := &deckCommander{}

	cmd := &cobra.Command{
		Use:   "deck",
		Short: deckShortDesc,
		Long:  deckLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd.Context(), cmd)
		},
	}

	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database")
	cmd.Flags().StringVar(&cmder.pricingPath, "pricing", "", "Path to pricing JSON overrides")
	cmd.Flags().StringVar(&cmder.since, "since", "", "Look back duration (e.g. 24h)")
	cmd.Flags().StringVar(&cmder.from, "from", "", "Start time (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.to, "to", "", "End time (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.sort, "sort", "cost", "Sort sessions by cost|time|tokens|duration")
	cmd.Flags().StringVar(&cmder.sortDir, "sort-dir", sortDirDesc, "Sort direction asc|desc")
	cmd.Flags().StringVar(&cmder.model, "model", "", "Filter by model")
	cmd.Flags().StringVar(&cmder.status, "status", "", "Filter by status (completed|failed|abandoned)")
	cmd.Flags().StringVar(&cmder.session, "session", "", "Drill into a specific session ID")
	cmd.Flags().UintVar(&cmder.refresh, "refresh", 10, "Auto-refresh interval in seconds (0 to disable)")
	cmd.Flags().BoolVar(&cmder.web, "web", false, "Serve the web dashboard locally")
	cmd.Flags().IntVar(&cmder.port, "port", 8888, "Web server port")
	cmd.Flags().BoolVar(&cmder.insights, "insights", false, "Enable AI insights (requires Ollama)")
	cmd.Flags().StringVar(&cmder.insightsModel, "insights-model", "auto", "Ollama model for AI insights (auto uses first available)")
	cmd.Flags().StringVar(&cmder.insightsTarget, "insights-target", "http://localhost:11434", "Ollama base URL for AI insights")
	cmd.Flags().BoolVarP(&cmder.demo, "demo", "m", false, "Seed demo data and open the deck UI")
	cmd.Flags().BoolVarP(&cmder.overwrite, "overwrite", "f", false, "Overwrite demo database before seeding (default for demo db)")

	return cmd
}

func (c *deckCommander) run(ctx context.Context, cmd *cobra.Command) error {
	pricing, err := deck.LoadPricing(c.pricingPath)
	if err != nil {
		return err
	}

	if c.overwrite && !c.demo {
		return errors.New("--overwrite requires --demo")
	}
	if c.demo && strings.TrimSpace(c.sqlitePath) == "" {
		c.sqlitePath = deck.DemoSQLitePath
		if !c.overwrite {
			c.overwrite = true
		}
	}

	sqlitePath, err := sqlitepath.ResolveSQLitePath(c.sqlitePath)
	if err != nil {
		return err
	}

	if c.demo {
		sessionCount, messageCount, err := deck.SeedDemo(ctx, sqlitePath, c.overwrite)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Seeded %d demo sessions (%d messages) into %s\n", sessionCount, messageCount, sqlitePath)
	}

	query, closeFn, err := deck.NewQuery(ctx, sqlitePath, pricing)
	if err != nil {
		return err
	}
	defer func() { _ = closeFn() }()

	filters, err := c.parseFilters()
	if err != nil {
		return err
	}

	if c.web {
		opts := insightOptions{target: c.insightsTarget}
		if c.insights {
			model, err := resolveInsightsModel(ctx, true, c.insightsModel, c.insightsTarget)
			if err != nil {
				return err
			}
			opts.enabled = true
			opts.model = model
		}
		return runDeckWeb(ctx, query, filters, c.port, opts)
	}

	refreshDuration, err := refreshDuration(c.refresh)
	if err != nil {
		return err
	}

	return RunDeckTUI(ctx, query, filters, refreshDuration)
}

func refreshDuration(refresh uint) (time.Duration, error) {
	if refresh == 0 {
		return 0, nil
	}

	maxSeconds := uint64(int64(^uint64(0)>>1) / int64(time.Second))
	refreshSeconds := uint64(refresh)
	if refreshSeconds > maxSeconds {
		return 0, errors.New("refresh exceeds maximum duration")
	}

	return time.Duration(int64(refreshSeconds)) * time.Second, nil
}

func (c *deckCommander) parseFilters() (deck.Filters, error) {
	filters := deck.Filters{
		Sort:    strings.ToLower(strings.TrimSpace(c.sort)),
		SortDir: strings.ToLower(strings.TrimSpace(c.sortDir)),
		Model:   strings.TrimSpace(c.model),
		Status:  strings.TrimSpace(c.status),
		Session: strings.TrimSpace(c.session),
	}

	if filters.SortDir == "" {
		filters.SortDir = sortDirDesc
	}

	if c.since != "" {
		duration, err := time.ParseDuration(c.since)
		if err != nil {
			return filters, fmt.Errorf("invalid since duration: %w", err)
		}
		filters.Since = duration
	}

	if c.from != "" {
		parsed, err := parseTime(c.from)
		if err != nil {
			return filters, fmt.Errorf("invalid from time: %w", err)
		}
		filters.From = &parsed
	}

	if c.to != "" {
		parsed, err := parseTime(c.to)
		if err != nil {
			return filters, fmt.Errorf("invalid to time: %w", err)
		}
		filters.To = &parsed
	}

	return filters, nil
}

func resolveInsightsModel(ctx context.Context, enabled bool, model string, target string) (string, error) {
	if !enabled {
		return model, nil
	}

	if err := ensureOllamaAvailable(ctx, target); err != nil {
		return "", err
	}

	requested := strings.TrimSpace(model)
	models, err := listOllamaModels(ctx)
	if err != nil {
		return "", err
	}
	if len(models) == 0 {
		return "", errors.New("no Ollama models found (run `ollama list` to see available models)")
	}

	if requested == "" || strings.EqualFold(requested, "auto") {
		model := pickAutoModel(models)
		if model == "" {
			return "", errors.New("no chat-capable Ollama models found (embedding-only models present)")
		}
		return model, nil
	}

	if slices.Contains(models, requested) {
		return requested, nil
	}

	return "", fmt.Errorf("ollama model %q not found (available: %s)", requested, strings.Join(models, ", "))
}

func pickAutoModel(models []string) string {
	if len(models) == 0 {
		return ""
	}

	filtered := filterChatModels(models)
	if len(filtered) == 0 {
		return ""
	}
	if len(filtered) == 1 {
		return filtered[0]
	}

	priorities := []string{"gemma", "gpt", "kimi", "qwen"}
	slices.SortStableFunc(filtered, func(a, b string) int {
		rankA, nameA := modelRank(a, priorities)
		rankB, nameB := modelRank(b, priorities)
		if rankA != rankB {
			return rankA - rankB
		}
		if nameA < nameB {
			return -1
		}
		if nameA > nameB {
			return 1
		}
		return 0
	})

	return filtered[0]
}

func filterChatModels(models []string) []string {
	filtered := make([]string, 0, len(models))
	for _, model := range models {
		if isEmbeddingModel(model) {
			continue
		}
		filtered = append(filtered, model)
	}
	return filtered
}

func isEmbeddingModel(model string) bool {
	name := strings.ToLower(model)
	return strings.Contains(name, "embed") || strings.Contains(name, "embedding")
}

func modelRank(model string, priorities []string) (int, string) {
	name := strings.ToLower(model)
	for idx, prefix := range priorities {
		if strings.Contains(name, prefix) {
			return idx, name
		}
	}
	return len(priorities), name
}

func listOllamaModels(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(ctx, "ollama", "list")
	output, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(output))
		if msg == "" {
			return nil, fmt.Errorf("ollama list failed: %w", err)
		}
		return nil, fmt.Errorf("ollama list failed: %s", msg)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) <= 1 {
		return []string{}, nil
	}

	models := make([]string, 0, len(lines)-1)
	for i, line := range lines {
		if i == 0 {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		models = append(models, fields[0])
	}

	return models, nil
}

func ensureOllamaAvailable(ctx context.Context, target string) error {
	if err := ollamaPing(ctx, target); err == nil {
		return nil
	}

	if !isLocalOllamaTarget(target) {
		return fmt.Errorf("ollama not reachable at %s; start it manually", target)
	}

	cmd := exec.CommandContext(ctx, "ollama", "serve")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start ollama: %w", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := ollamaPing(ctx, target); err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}

	return fmt.Errorf("ollama did not become ready at %s", target)
}

func ollamaPing(ctx context.Context, target string) error {
	urlStr := strings.TrimRight(target, "/") + "/api/tags"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama status %d", resp.StatusCode)
	}

	return nil
}

func isLocalOllamaTarget(target string) bool {
	parsed, err := url.Parse(target)
	if err != nil {
		return false
	}

	host := parsed.Hostname()
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("empty time")
	}

	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}

	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed, nil
	}

	return time.Time{}, errors.New("expected RFC3339 or YYYY-MM-DD")
}

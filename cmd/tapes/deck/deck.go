// Package deckcmder provides the deck command for session ROI dashboards.
package deckcmder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/sqlitepath"
	"github.com/papercomputeco/tapes/pkg/credentials"
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
  tapes deck --web --port 9999
  tapes deck --pricing ./pricing.json
  tapes deck --demo
  tapes deck --demo --overwrite
  tapes deck -m
  tapes deck -m -f
  tapes deck --web --insights
  tapes deck --web --insights --insights-provider anthropic
`
	deckShortDesc = "Deck - ROI dashboard for agent sessions"
	sortDirDesc   = "desc"
)

type deckCommander struct {
	sqlitePath       string
	pricingPath      string
	since            string
	from             string
	to               string
	sort             string
	sortDir          string
	model            string
	status           string
	project          string
	session          string
	refresh          uint
	web              bool
	port             int
	demo             bool
	overwrite        bool
	insights         bool
	insightsModel    string
	insightsProvider string
	insightsKey      string
	theme            string
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
	cmd.Flags().StringVar(&cmder.project, "project", "", "Filter by project name")
	cmd.Flags().StringVar(&cmder.session, "session", "", "Drill into a specific session ID")
	cmd.Flags().UintVar(&cmder.refresh, "refresh", 10, "Auto-refresh interval in seconds (0 to disable)")
	cmd.Flags().BoolVar(&cmder.web, "web", false, "Serve the web dashboard locally")
	cmd.Flags().IntVar(&cmder.port, "port", 8888, "Web server port")
	cmd.Flags().BoolVarP(&cmder.demo, "demo", "m", false, "Seed demo data and open the deck UI")
	cmd.Flags().BoolVarP(&cmder.overwrite, "overwrite", "f", false, "Overwrite demo database before seeding (default for demo db)")
	cmd.Flags().BoolVar(&cmder.insights, "insights", false, "Enable AI insights via LLM facet extraction")
	cmd.Flags().StringVar(&cmder.insightsModel, "insights-model", "gpt-4o-mini", "Model for AI insights extraction")
	cmd.Flags().StringVar(&cmder.insightsProvider, "insights-provider", "openai", "Provider for AI insights (openai|anthropic|ollama)")
	cmd.Flags().StringVar(&cmder.insightsKey, "insights-key", "", "API key for AI insights provider")
	cmd.Flags().StringVar(&cmder.theme, "theme", "", "Force color theme: dark or light (auto-detected by default)")

	return cmd
}

func (c *deckCommander) run(ctx context.Context, cmd *cobra.Command) error {
	// Apply theme override before anything renders. The init() palette
	// is based on auto-detection; re-apply if the user explicitly chose.
	if c.theme != "" {
		switch c.theme {
		case "dark", "light":
			themeOverride = c.theme
			if isDarkTheme() {
				applyPalette(darkPalette)
			} else {
				applyPalette(lightPalette)
			}
		default:
			return fmt.Errorf("invalid --theme value %q: expected dark or light", c.theme)
		}
	}

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

	// Build facet deps — auto-enable when credentials are available
	var facets *facetDeps
	var facetWorker *deck.FacetWorker
	var facetAnalyticsFunc func(context.Context) (*deck.FacetAnalytics, error)

	facets, facetWorker, facetAnalyticsFunc = c.buildFacetDeps(cmd, query)

	if c.web {
		return runDeckWeb(ctx, query, filters, c.port, facets)
	}

	refreshDuration, err := refreshDuration(c.refresh)
	if err != nil {
		return err
	}

	return RunDeckTUI(ctx, query, filters, refreshDuration, facetWorker, facetAnalyticsFunc)
}

// buildFacetDeps auto-detects API credentials and creates facet extraction
// dependencies. Returns nil values if no credentials are available and
// --insights was not explicitly set.
func (c *deckCommander) buildFacetDeps(cmd *cobra.Command, query *deck.Query) (*facetDeps, *deck.FacetWorker, func(context.Context) (*deck.FacetAnalytics, error)) {
	credMgr, err := credentials.NewManager("")
	if err != nil {
		credMgr = nil
	}

	cfg := deck.LLMCallerConfig{
		Provider: c.insightsProvider,
		Model:    c.insightsModel,
		APIKey:   c.insightsKey,
		CredMgr:  credMgr,
	}

	// If not explicitly enabled, check whether any API key can be resolved.
	// Skip auto-enable if nothing is available — don't fall back to ollama silently.
	if !c.insights {
		if !deck.HasLLMCredentials(cfg) {
			return nil, nil, nil
		}
	}

	llmCaller, err := deck.NewLLMCaller(cfg)
	if err != nil {
		return nil, nil, nil
	}

	store := deck.NewEntFacetStore(query.EntClient())
	extractor := deck.NewFacetExtractor(query, llmCaller, store)
	worker := deck.NewFacetWorker(extractor, store, query)

	fmt.Fprintf(cmd.OutOrStdout(), "AI insights enabled (%s/%s)\n", c.insightsProvider, c.insightsModel)

	return &facetDeps{
			extractor: extractor,
			worker:    worker,
			store:     store,
		},
		worker,
		extractor.AggregateFacets
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
		Project: strings.TrimSpace(c.project),
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

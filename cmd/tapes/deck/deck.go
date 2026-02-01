// Package deckcmder provides the deck command for session ROI dashboards.
package deckcmder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/deck"
)

const deckLongDesc string = `Deck is an ROI dashboard for agent sessions.

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
`

const deckShortDesc string = "Deck - ROI dashboard for agent sessions"

type deckCommander struct {
	sqlitePath  string
	pricingPath string
	since       string
	from        string
	to          string
	sort        string
	model       string
	status      string
	session     string
	web         bool
	port        int
}

func NewDeckCmd() *cobra.Command {
	cmder := &deckCommander{}

	cmd := &cobra.Command{
		Use:   "deck",
		Short: deckShortDesc,
		Long:  deckLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd.Context())
		},
	}

	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database")
	cmd.Flags().StringVar(&cmder.pricingPath, "pricing", "", "Path to pricing JSON overrides")
	cmd.Flags().StringVar(&cmder.since, "since", "", "Look back duration (e.g. 24h)")
	cmd.Flags().StringVar(&cmder.from, "from", "", "Start time (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.to, "to", "", "End time (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.sort, "sort", "cost", "Sort sessions by cost|time|tokens|duration")
	cmd.Flags().StringVar(&cmder.model, "model", "", "Filter by model")
	cmd.Flags().StringVar(&cmder.status, "status", "", "Filter by status (completed|failed|abandoned)")
	cmd.Flags().StringVar(&cmder.session, "session", "", "Drill into a specific session ID")
	cmd.Flags().BoolVar(&cmder.web, "web", false, "Serve the web dashboard locally")
	cmd.Flags().IntVar(&cmder.port, "port", 8888, "Web server port")

	return cmd
}

func (c *deckCommander) run(ctx context.Context) error {
	pricing, err := deck.LoadPricing(c.pricingPath)
	if err != nil {
		return err
	}

	sqlitePath, err := resolveSQLitePath(c.sqlitePath)
	if err != nil {
		return err
	}

	query, closeFn, err := deck.NewQuery(sqlitePath, pricing)
	if err != nil {
		return err
	}
	defer closeFn()

	filters, err := c.parseFilters()
	if err != nil {
		return err
	}

	if c.web {
		return runDeckWeb(ctx, query, filters, c.port)
	}

	return runDeckTUI(ctx, query, filters)
}

func (c *deckCommander) parseFilters() (deck.Filters, error) {
	filters := deck.Filters{
		Sort:    strings.ToLower(strings.TrimSpace(c.sort)),
		Model:   strings.TrimSpace(c.model),
		Status:  strings.TrimSpace(c.status),
		Session: strings.TrimSpace(c.session),
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
		return time.Time{}, fmt.Errorf("empty time")
	}

	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}

	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed, nil
	}

	return time.Time{}, fmt.Errorf("expected RFC3339 or YYYY-MM-DD")
}

func resolveSQLitePath(override string) (string, error) {
	if override != "" {
		return override, nil
	}

	candidates := []string{
		"tapes.db",
		"tapes.sqlite",
		filepath.Join(".tapes", "tapes.db"),
		filepath.Join(".tapes", "tapes.sqlite"),
	}

	home, err := os.UserHomeDir()
	if err == nil {
		candidates = append(candidates,
			filepath.Join(home, ".tapes", "tapes.db"),
			filepath.Join(home, ".tapes", "tapes.sqlite"),
		)
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("could not find tapes SQLite database; pass --sqlite")
}

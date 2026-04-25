// Package deckcmder provides the deck command for session ROI dashboards.
package deckcmder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/deck"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

const (
	deckLongDesc = `Deck is an ROI dashboard for agent sessions.

Summarize recent sessions with a TUI and drill down into a single session.

Examples:
  tapes deck
  tapes deck --api-target http://localhost:8081
  tapes deck --since 24h
  tapes deck --from 2026-01-30 --to 2026-01-31
  tapes deck --sort cost --model claude-sonnet-4.5
  tapes deck --session sess_a8f2c1d3
  tapes deck --pricing ./pricing.json
  tapes deck --demo
`
	deckShortDesc = "Deck - ROI dashboard for agent sessions"
	sortDirDesc   = "desc"
)

type deckCommander struct {
	apiTarget   string
	pricingPath string
	since       string
	from        string
	to          string
	sort        string
	sortDir     string
	model       string
	status      string
	project     string
	session     string
	refresh     uint
	demo        bool
	theme       string
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

	cmd.Flags().StringVarP(&cmder.apiTarget, "api-target", "a", "http://localhost:8081", "URL of the tapes API server")
	cmd.Flags().StringVar(&cmder.pricingPath, "pricing", "", "Path to pricing JSON overrides")
	cmd.Flags().StringVar(&cmder.since, "since", "", "Look back duration (e.g. 24h)")
	cmd.Flags().StringVar(&cmder.from, "from", "", "Start time (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.to, "to", "", "End time (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&cmder.sort, "sort", "date", "Sort sessions by cost|date|tokens|duration")
	cmd.Flags().StringVar(&cmder.sortDir, "sort-dir", sortDirDesc, "Sort direction asc|desc")
	cmd.Flags().StringVar(&cmder.model, "model", "", "Filter by model")
	cmd.Flags().StringVar(&cmder.status, "status", "", "Filter by status (completed|failed|abandoned)")
	cmd.Flags().StringVar(&cmder.project, "project", "", "Filter by project name")
	cmd.Flags().StringVar(&cmder.session, "session", "", "Drill into a specific session ID")
	cmd.Flags().UintVar(&cmder.refresh, "refresh", 0, "Auto-refresh interval in seconds (0 to disable)")
	cmd.Flags().BoolVarP(&cmder.demo, "demo", "m", false, "Seed demo data and open the deck UI")
	cmd.Flags().StringVar(&cmder.theme, "theme", "", "Force color theme: dark or light (auto-detected by default)")

	return cmd
}

func (c *deckCommander) run(ctx context.Context, cmd *cobra.Command) error {
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

	pricing, err := sessions.LoadPricing(c.pricingPath)
	if err != nil {
		return err
	}

	apiTarget := normalizeAPITarget(c.apiTarget)
	if c.demo {
		sessionCount, messageCount, err := deck.SeedDemoViaAPI(ctx, apiTarget, false)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Seeded %d demo sessions (%d messages) via API\n", sessionCount, messageCount)
	}

	query := deck.NewHTTPQuery(apiTarget, pricing)

	filters, err := c.parseFilters()
	if err != nil {
		return err
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
	} else if c.from == "" && c.to == "" {
		// Bound the default overview to a recent window so the API-backed deck
		// stays snappy on large stores when no explicit time filter is provided.
		filters.Since = 30 * 24 * time.Hour
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

func normalizeAPITarget(target string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		return "http://localhost:8081"
	}
	if !strings.Contains(target, "://") {
		return "http://" + target
	}
	return target
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

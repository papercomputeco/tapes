// Package sessionscmder provides the `tapes sessions` command: a quick list
// of captured sessions and their ids, so other commands (export, search)
// have something to point at.
package sessionscmder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/inprocessapi"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/skill"
)

type sessionsCommander struct {
	flags config.FlagSet

	apiTarget string
	postgres  string
	quiet     bool
}

var sessionsFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", Shorthand: "a", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const sessionsLongDesc string = `List captured sessions.

Prints the most recent sessions with their full id, turn count, cost, model,
and a prompt preview. Use a session id with tapes export (or its short
prefix) to export a conversation.

Connect to a running API with --api-target, or read a database directly with
--postgres.

Examples:
  tapes sessions
  tapes sessions -q | head -1
  tapes sessions --postgres "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable"`

// NewSessionsCmd builds the `tapes sessions` list command.
func NewSessionsCmd() *cobra.Command {
	cmder := &sessionsCommander{flags: sessionsFlags}

	cmd := &cobra.Command{
		Use:   "sessions",
		Short: "List captured sessions",
		Long:  sessionsLongDesc,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to default
			}
			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{config.FlagAPITarget})
			cmder.apiTarget = v.GetString("client.api_target")
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd)
		},
	}

	cmd.Flags().StringVar(&cmder.postgres, "postgres", "", "PostgreSQL connection string for a local in-process API")
	cmd.Flags().BoolVarP(&cmder.quiet, "quiet", "q", false, "Output only session IDs, one per line (for piping)")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *sessionsCommander) run(cmd *cobra.Command) error {
	client, closeFn, err := connect(cmd.Context(), c.apiTarget, c.postgres)
	if err != nil {
		return err
	}
	defer closeFn()

	sessions, err := client.Sessions(cmd.Context())
	if err != nil {
		return err
	}

	if c.quiet {
		for _, s := range sessions {
			fmt.Fprintln(cmd.OutOrStdout(), s.ID)
		}
		return nil
	}

	if len(sessions) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No sessions captured yet. Seed demo data with `tapes seed --demo`, or point an agent at the proxy.")
		return nil
	}

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "SESSION ID\tSTARTED\tTURNS\tCOST\tMODEL\tPROMPT")
	for _, s := range sessions {
		fmt.Fprintf(tw, "%s\t%s\t%d\t$%.4f\t%s\t%s\n",
			s.ID,
			s.StartedAt.Local().Format("2006-01-02 15:04"),
			s.TurnCount,
			s.TotalCostUSD,
			fallback(s.Model, "—"),
			truncate(firstLine(s.Preview), 48),
		)
	}
	return tw.Flush()
}

// connect resolves an APIClient against either a running API (--api-target)
// or an in-process API over --postgres.
func connect(ctx context.Context, apiTarget, postgres string) (*skill.APIClient, func(), error) {
	if strings.TrimSpace(apiTarget) != "" {
		return skill.NewAPIClient(apiTarget), func() {}, nil
	}
	if strings.TrimSpace(postgres) == "" {
		return nil, nil, errors.New("no API target configured: pass --api-target or --postgres")
	}
	target, stop, err := inprocessapi.Start(ctx, postgres, nil)
	if err != nil {
		return nil, nil, err
	}
	return skill.NewAPIClient(target), stop, nil
}

func firstLine(s string) string {
	if before, _, ok := strings.Cut(s, "\n"); ok {
		return before
	}
	return s
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func fallback(s, alt string) string {
	if strings.TrimSpace(s) == "" {
		return alt
	}
	return s
}

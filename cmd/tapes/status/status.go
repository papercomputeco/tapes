// Package statuscmder provides the status command: a quick readout of the
// local tapes setup — which .tapes/ directory and configuration are in use,
// and whether the configured API target is reachable.
package statuscmder

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/skill"
)

const statsProbeTimeout = 3 * time.Second

type statusCommander struct {
	flags config.FlagSet

	apiTarget string
	provider  string
	upstream  string
	postgres  string
}

var statusFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", Shorthand: "a", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const statusLongDesc string = `Show the local tapes setup.

Reports which .tapes/ directory and configuration are in use, the configured
provider and upstream, and whether the API target is reachable — with a quick
capture summary when it is.

Examples:
  tapes status
  tapes status --api-target http://localhost:8081`

const statusShortDesc string = "Show the local tapes setup"

// NewStatusCmd builds the `tapes status` command.
func NewStatusCmd() *cobra.Command {
	cmder := &statusCommander{flags: statusFlags}

	cmd := &cobra.Command{
		Use:   "status",
		Short: statusShortDesc,
		Long:  statusLongDesc,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to defaults
			}
			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{config.FlagAPITarget})
			cmder.apiTarget = v.GetString("client.api_target")
			cmder.provider = v.GetString("proxy.provider")
			cmder.upstream = v.GetString("proxy.upstream")
			cmder.postgres = v.GetString("storage.postgres_dsn")
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd)
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *statusCommander) run(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()

	dir, err := dotdir.NewManager().Target("")
	if err != nil {
		return fmt.Errorf("resolving .tapes directory: %w", err)
	}

	fmt.Fprintln(out)
	if dir == "" {
		fmt.Fprintf(out, "  %s  %s\n",
			cliui.KeyStyle.Render("Config dir:"),
			cliui.DimStyle.Render("none — run `tapes init` (falling back to defaults)"),
		)
	} else {
		fmt.Fprintf(out, "  %s  %s\n", cliui.KeyStyle.Render("Config dir:"), cliui.ValueStyle.Render(dir))
	}

	if c.provider != "" || c.upstream != "" {
		fmt.Fprintf(out, "  %s  %s %s\n",
			cliui.KeyStyle.Render("Provider:  "),
			cliui.ValueStyle.Render(c.provider),
			cliui.DimStyle.Render("→ "+c.upstream),
		)
	}

	storage := "not configured"
	if c.postgres != "" {
		storage = "postgres configured"
	}
	fmt.Fprintf(out, "  %s  %s\n", cliui.KeyStyle.Render("Storage:   "), cliui.ValueStyle.Render(storage))

	fmt.Fprintf(out, "  %s  %s\n", cliui.KeyStyle.Render("API target:"), cliui.ValueStyle.Render(c.apiTarget))

	ctx, cancel := context.WithTimeout(cmd.Context(), statsProbeTimeout)
	defer cancel()

	stats, err := skill.NewAPIClient(c.apiTarget).Stats(ctx)
	if err != nil {
		fmt.Fprintf(out, "  %s  %s %s\n\n",
			cliui.KeyStyle.Render("API:       "),
			cliui.FailMark,
			cliui.DimStyle.Render("unreachable — start one with `tapes local up` then `tapes serve`"),
		)
		return nil
	}

	fmt.Fprintf(out, "  %s  %s %s\n\n",
		cliui.KeyStyle.Render("API:       "),
		cliui.SuccessMark,
		cliui.ValueStyle.Render(fmt.Sprintf("%d sessions · %d turns · $%.4f captured", stats.SessionCount, stats.TurnCount, stats.TotalCost)),
	)
	return nil
}

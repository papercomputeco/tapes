package configcmder

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
)

const listLongDesc string = `List all configuration values.

Displays all configuration keys and their current values from the
config.toml file stored in the .tapes/ directory.

Each value carries its source in parentheses: (default) for built-in
defaults, (config file) for config.toml values, and (environment) for
TAPES_... environment overrides. Keys with no value in any layer show
<not set>.

Examples:
  tapes config list`

const listShortDesc string = "List all configuration values"

func newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: listShortDesc,
		Long:  listLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			return runList(configDir)
		},
	}

	return cmd
}

func runList(configDir string) error {
	cfger, err := config.NewConfiger(configDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	target := cfger.GetTarget()
	if target != "" {
		fmt.Printf("\n  %s %s\n\n",
			cliui.KeyStyle.Render("Config file:"),
			cliui.DimStyle.Render(target),
		)
	} else {
		fmt.Printf("\n  %s\n\n", cliui.DimStyle.Render("No config file found. Using defaults."))
	}

	keys := config.ValidConfigKeys()

	// Find the longest key name for alignment.
	maxLen := 0
	for _, k := range keys {
		if len(k) > maxLen {
			maxLen = len(k)
		}
	}

	for _, key := range keys {
		value, source, err := cfger.GetConfigValueSource(key)
		if err != nil {
			return err
		}

		if value == "" {
			fmt.Printf("  %-*s  %s\n",
				maxLen,
				cliui.KeyStyle.Render(key),
				cliui.DimStyle.Render("<not set>"),
			)
		} else {
			fmt.Printf("  %-*s  %s %s\n",
				maxLen,
				cliui.KeyStyle.Render(key),
				cliui.ValueStyle.Render(value),
				cliui.DimStyle.Render("("+string(source)+")"),
			)
		}
	}

	fmt.Println()
	return nil
}

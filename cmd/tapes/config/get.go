package configcmder

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
)

const getLongDesc string = `Get a configuration value.

Reads the value for the given key from the config.toml file
stored in the .tapes/ directory. Keys use dotted notation matching
the TOML section structure.

Examples:
  tapes config get proxy.provider
  tapes config get embedding.model`

const getShortDesc string = "Get a configuration value"

func newGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: getShortDesc,
		Long:  getLongDesc,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			return runGet(args[0], configDir)
		},
		ValidArgsFunction: func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return config.ValidConfigKeys(), cobra.ShellCompDirectiveNoFileComp
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}

	return cmd
}

func runGet(key, configDir string) error {
	if !config.IsValidConfigKey(key) {
		return fmt.Errorf("unknown config key: %q\n\nValid keys: %s",
			key, strings.Join(config.ValidConfigKeys(), ", "))
	}

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

	value, err := cfger.GetConfigValue(key)
	if err != nil {
		return err
	}

	if value == "" {
		fmt.Printf("  %s  %s\n\n", cliui.KeyStyle.Render(key), cliui.DimStyle.Render("<not set>"))
	} else {
		fmt.Printf("  %s  %s\n\n", cliui.KeyStyle.Render(key), cliui.ValueStyle.Render(value))
	}

	return nil
}

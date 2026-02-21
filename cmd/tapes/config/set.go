package configcmder

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
)

const setLongDesc string = `Set a configuration value.

Sets the given key to the provided value in the config.toml file
stored in the .tapes/ directory. Keys use dotted notation matching
the TOML section structure.

Valid keys:
  storage.sqlite_path,
  proxy.provider, proxy.upstream, proxy.listen,
  api.listen,
  client.proxy_target, client.api_target,
  vector_store.provider, vector_store.target,
  embedding.provider, embedding.target, embedding.model, embedding.dimensions

Examples:
  tapes config set proxy.provider anthropic
  tapes config set proxy.upstream https://api.anthropic.com
  tapes config set embedding.dimensions 768`

const setShortDesc string = "Set a configuration value"

func newSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: setShortDesc,
		Long:  setLongDesc,
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			return runSet(args[0], args[1], configDir)
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

func runSet(key, value, configDir string) error {
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

	err = cfger.SetConfigValue(key, value)
	if err != nil {
		return err
	}

	fmt.Printf("  %s Set %s = %s\n\n",
		cliui.SuccessMark,
		cliui.KeyStyle.Render(key),
		cliui.ValueStyle.Render(value),
	)
	return nil
}

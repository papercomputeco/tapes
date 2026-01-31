package configcmder

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
)

const listLongDesc string = `List all configuration values.

Displays all configuration keys and their current values from the
config.toml file stored in the .tapes/ directory.

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
		fmt.Printf("Using config file: %s\n\n", cfger.GetTarget())
	} else {
		fmt.Print("No config file found. Using default config.\n\n")
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
		value, err := cfger.GetConfigValue(key)
		if err != nil {
			return err
		}

		if value == "" {
			fmt.Printf("%-*s = <not set>\n", maxLen, key)
		} else {
			fmt.Printf("%-*s = %q\n", maxLen, key, value)
		}
	}

	return nil
}

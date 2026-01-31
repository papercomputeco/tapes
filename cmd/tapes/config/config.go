// Package configcmder provides the config command for managing persistent
// tapes configuration stored in the .tapes/ directory.
package configcmder

import (
	"github.com/spf13/cobra"
)

const configLongDesc string = `Manage persistent tapes configuration.

Configuration is stored as config.toml in the .tapes/ directory and provides
default values for command flags. CLI flags always take precedence over
config file values.

Keys use dotted notation matching the TOML section structure:
  proxy.provider, proxy.upstream, proxy.listen,
  api.listen, storage.sqlite_path,
  client.proxy_target, client.api_target,
  vector_store.provider, vector_store.target,
  embedding.provider, embedding.target, embedding.model, embedding.dimensions

Use subcommands to get, set, or list configuration values:
  tapes config set <key> <value>    Set a configuration value
  tapes config get <key>            Get a configuration value
  tapes config list                 List all configuration values

Examples:
  tapes config set proxy.provider anthropic
  tapes config set embedding.model nomic-embed-text
  tapes config get proxy.provider
  tapes config list`

const configShortDesc string = "Manage persistent tapes configuration"

func NewConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: configShortDesc,
		Long:  configLongDesc,
	}

	cmd.AddCommand(newSetCmd())
	cmd.AddCommand(newGetCmd())
	cmd.AddCommand(newListCmd())

	return cmd
}

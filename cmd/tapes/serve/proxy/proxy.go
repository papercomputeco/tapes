// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
	"github.com/papercomputeco/tapes/proxy"
)

type proxyCommander struct {
	flags config.FlagSet

	listen       string
	upstream     string
	providerType string
	postgresDSN  string
	project      string

	logger *slog.Logger
}

// proxyFlags defines the flags for the standalone proxy subcommand.
// Uses FlagProxyListenStandalone (--listen/-l) instead of the parent's
// --proxy-listen/-p, and omits --api-listen since this is proxy-only.
var proxyFlags = config.FlagSet{
	config.FlagProxyListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagUpstream:              {Name: "upstream", Shorthand: "u", ViperKey: "proxy.upstream", Description: "Upstream LLM provider URL"},
	config.FlagProvider:              {Name: "provider", ViperKey: "proxy.provider", Description: "LLM provider type (anthropic, openai, ollama)"},
	config.FlagPostgres:              {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:               {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
}

const proxyLongDesc string = `Run the proxy server.

The proxy intercepts all requests and transparently forwards them to the
configured upstream URL, recording request/response conversation turns.

Supported provider types: anthropic, openai, ollama
`

const proxyShortDesc string = "Run the Tapes proxy server"

func NewProxyCmd() *cobra.Command {
	cmder := &proxyCommander{
		flags: proxyFlags,
	}

	cmd := &cobra.Command{
		Use:   "proxy",
		Short: proxyShortDesc,
		Long:  proxyLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagProxyListenStandalone,
				config.FlagUpstream,
				config.FlagProvider,
				config.FlagPostgres,
				config.FlagProject,
			})

			cmder.listen = v.GetString("proxy.listen")
			cmder.upstream = v.GetString("proxy.upstream")
			cmder.providerType = v.GetString("proxy.provider")
			cmder.project = v.GetString("proxy.project")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")

			if cmder.project == "" {
				cmder.project = git.RepoName(cmd.Context())
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmder.logger = logger.FromContext(cmd.Context())
			telemetry.FromContext(cmd.Context()).CaptureServerStarted("proxy")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagProxyListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagUpstream, &cmder.upstream)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProvider, &cmder.providerType)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)

	return cmd
}

func (c *proxyCommander) run() error {
	driver, err := postgres.NewDriver(context.TODO(), c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	proxyConfig := proxy.Config{
		ListenAddr:   c.listen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
		Project:      c.project,
	}

	p, err := proxy.New(proxyConfig, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer p.Close()

	c.logger.Info("starting proxy server",
		"listen", c.listen,
		"upstream", c.upstream,
		"provider", c.providerType,
	)

	return p.Run()
}

// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
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
	debug        bool
	postgresDSN  string
	project      string

	vectorStoreTarget string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint
	embeddingAPIKey     string

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
	config.FlagVectorStoreTgt:        {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "pgvector connection string (defaults to storage.postgres_dsn when unset)"},
	config.FlagEmbeddingProv:         {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Deprecated here; embeddings are written by the derive worker (--embed-spans)"},
	config.FlagEmbeddingTgt:          {Name: "embedding-target", ViperKey: "embedding.target", Description: "Deprecated here; embeddings are written by the derive worker (--embed-spans)"},
	config.FlagEmbeddingModel:        {Name: "embedding-model", ViperKey: "embedding.model", Description: "Deprecated here; embeddings are written by the derive worker (--embed-spans)"},
	config.FlagEmbeddingDims:         {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Deprecated here; embeddings are written by the derive worker (--embed-spans)"},
}

const proxyLongDesc string = `Run the proxy server.

The proxy intercepts all requests and transparently forwards them to the
configured upstream URL, recording request/response conversation turns.

Supported provider types: anthropic, openai, ollama

Embeddings are no longer written at capture time: the derive worker family is
the single writer (tapes serve derive-worker --embed-spans). The embedding
flags remain accepted for deployment compatibility but have no effect here.`

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
				config.FlagVectorStoreTgt,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
			})

			cmder.listen = v.GetString("proxy.listen")
			cmder.upstream = v.GetString("proxy.upstream")
			cmder.providerType = v.GetString("proxy.provider")
			cmder.vectorStoreTarget = v.GetString("vector_store.target")
			embedding := config.ResolveEmbeddingConfigWithOptions(
				v.GetString("embedding.provider"),
				v.GetString("embedding.target"),
				v.GetString("embedding.model"),
				v.GetUint("embedding.dimensions"),
				config.ResolveEmbeddingConfigOptions{
					DimensionsSet: config.IsRegisteredFlagExplicitlySet(v, cmd, cmder.flags, config.FlagEmbeddingDims),
				},
			)
			cmder.embeddingProvider = embedding.Provider
			cmder.embeddingTarget = embedding.Target
			cmder.embeddingModel = embedding.Model
			cmder.embeddingDimensions = embedding.Dimensions
			cmder.embeddingAPIKey, err = credentials.APIKeyForProvider(embedding.Provider, configDir)
			if err != nil {
				return fmt.Errorf("could not load embedding credentials: %w", err)
			}
			cmder.project = v.GetString("proxy.project")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			if cmder.vectorStoreTarget == "" && cmder.postgresDSN != "" {
				cmder.vectorStoreTarget = cmder.postgresDSN
			}

			if cmder.project == "" {
				cmder.project = git.RepoName(cmd.Context())
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			telemetry.FromContext(cmd.Context()).CaptureServerStarted("proxy")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagProxyListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagUpstream, &cmder.upstream)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProvider, &cmder.providerType)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)

	return cmd
}

func (c *proxyCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

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

	// Capture-time embedding is retired: the derive worker family is
	// the single writer of embeddings (tapes serve derive-worker
	// --embed-spans). The embedding flags remain accepted so existing
	// deployments keep booting, but they no longer have any effect here.
	if c.embeddingTarget != "" || c.embeddingModel != "" {
		c.logger.Info("capture-time embedding is retired; embeddings are written by the derive worker",
			"see", "tapes serve derive-worker --embed-spans",
		)
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

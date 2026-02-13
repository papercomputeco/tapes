// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/config"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	vectorutils "github.com/papercomputeco/tapes/pkg/vector/utils"
	"github.com/papercomputeco/tapes/proxy"
)

type proxyCommander struct {
	flags config.FlagSet

	listen       string
	upstream     string
	providerType string
	debug        bool
	sqlitePath   string

	vectorStoreProvider string
	vectorStoreTarget   string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint

	logger *zap.Logger
}

// proxyFlags defines the flags for the standalone proxy subcommand.
// Uses FlagProxyListenStandalone (--listen/-l) instead of the parent's
// --proxy-listen/-p, and omits --api-listen since this is proxy-only.
var proxyFlags = config.FlagSet{
	config.FlagProxyListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagUpstream:              {Name: "upstream", Shorthand: "u", ViperKey: "proxy.upstream", Description: "Upstream LLM provider URL"},
	config.FlagProvider:              {Name: "provider", ViperKey: "proxy.provider", Description: "LLM provider type (anthropic, openai, ollama)"},
	config.FlagSQLite:                {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagVectorStoreProv:       {Name: "vector-store-provider", ViperKey: "vector_store.provider", Description: "Vector store provider type (e.g., chroma, sqlite)"},
	config.FlagVectorStoreTgt:        {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "Vector store target: filepath for sqlite or URL for remote service"},
	config.FlagEmbeddingProv:         {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama)"},
	config.FlagEmbeddingTgt:          {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel:        {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., nomic-embed-text)"},
	config.FlagEmbeddingDims:         {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
}

const proxyLongDesc string = `Run the proxy server.

The proxy intercepts all requests and transparently forwards them to the
configured upstream URL, recording request/response conversation turns.

Supported provider types: anthropic, openai, ollama

Optionally configure vector storage and embeddings of text content for "tapes search"
agentic functionality.`

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
				config.FlagSQLite,
				config.FlagVectorStoreProv,
				config.FlagVectorStoreTgt,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
			})

			cmder.listen = v.GetString("proxy.listen")
			cmder.upstream = v.GetString("proxy.upstream")
			cmder.providerType = v.GetString("proxy.provider")
			cmder.sqlitePath = v.GetString("storage.sqlite_path")
			cmder.vectorStoreProvider = v.GetString("vector_store.provider")
			cmder.vectorStoreTarget = v.GetString("vector_store.target")
			cmder.embeddingProvider = v.GetString("embedding.provider")
			cmder.embeddingTarget = v.GetString("embedding.target")
			cmder.embeddingModel = v.GetString("embedding.model")
			cmder.embeddingDimensions = v.GetUint("embedding.dimensions")

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagProxyListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagUpstream, &cmder.upstream)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProvider, &cmder.providerType)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreProv, &cmder.vectorStoreProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)

	return cmd
}

func (c *proxyCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer func() { _ = c.logger.Sync() }()

	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	config := proxy.Config{
		ListenAddr:   c.listen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
	}

	if c.vectorStoreTarget != "" {
		config.Embedder, err = embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
			ProviderType: c.embeddingProvider,
			TargetURL:    c.embeddingTarget,
			Model:        c.embeddingModel,
		})
		if err != nil {
			return fmt.Errorf("creating embedder: %w", err)
		}
		defer config.Embedder.Close()

		config.VectorDriver, err = vectorutils.NewVectorDriver(&vectorutils.NewVectorDriverOpts{
			ProviderType: c.vectorStoreProvider,
			Target:       c.vectorStoreTarget,
			Logger:       c.logger,
			Dimensions:   c.embeddingDimensions,
		})
		if err != nil {
			return fmt.Errorf("creating vector driver: %w", err)
		}
		defer config.VectorDriver.Close()

		c.logger.Info("vector storage enabled",
			zap.String("vector_store_provider", c.vectorStoreProvider),
			zap.String("vector_store_target", c.vectorStoreTarget),
			zap.String("embedding_provider", c.embeddingProvider),
			zap.String("embedding_target", c.embeddingTarget),
			zap.String("embedding_model", c.embeddingModel),
		)
	}

	p, err := proxy.New(config, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer p.Close()

	c.logger.Info("starting proxy server",
		zap.String("listen", c.listen),
		zap.String("upstream", c.upstream),
		zap.String("provider", c.providerType),
	)

	return p.Run()
}

func (c *proxyCommander) newStorageDriver() (storage.Driver, error) {
	if c.sqlitePath != "" {
		driver, err := sqlite.NewDriver(context.Background(), c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		c.logger.Info("using SQLite storage", zap.String("path", c.sqlitePath))
		return driver, nil
	}

	c.logger.Info("using in-memory storage")
	return inmemory.NewDriver(), nil
}

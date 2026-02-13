// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/config"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	vectorutils "github.com/papercomputeco/tapes/pkg/vector/utils"
	"github.com/papercomputeco/tapes/proxy"
)

type proxyCommander struct {
	listen       string
	upstream     string
	providerType string
	debug        bool
	sqlitePath   string
	project      string

	vectorStoreProvider string
	vectorStoreTarget   string

	embeddingProvider string
	embeddingTarget   string
	embeddingModel    string

	logger *zap.Logger
}

const proxyLongDesc string = `Run the proxy server.

The proxy intercepts all requests and transparently forwards them to the
configured upstream URL, recording request/response conversation turns.

Supported provider types: anthropic, openai, ollama

Optionally configure vector storage and embeddings of text content for "tapes search"
agentic functionality.`

const proxyShortDesc string = "Run the Tapes proxy server"

func NewProxyCmd() *cobra.Command {
	cmder := &proxyCommander{}

	cmd := &cobra.Command{
		Use:   "proxy",
		Short: proxyShortDesc,
		Long:  proxyLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			cfger, err := config.NewConfiger(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			cfg, err := cfger.LoadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			if !cmd.Flags().Changed("listen") {
				cmder.listen = cfg.Proxy.Listen
			}
			if !cmd.Flags().Changed("upstream") {
				cmder.upstream = cfg.Proxy.Upstream
			}
			if !cmd.Flags().Changed("provider") {
				cmder.providerType = cfg.Proxy.Provider
			}
			if !cmd.Flags().Changed("sqlite") {
				cmder.sqlitePath = cfg.Storage.SQLitePath
			}
			if !cmd.Flags().Changed("vector-store-provider") {
				cmder.vectorStoreProvider = cfg.VectorStore.Provider
			}
			if !cmd.Flags().Changed("vector-store-target") {
				cmder.vectorStoreTarget = cfg.VectorStore.Target
			}
			if !cmd.Flags().Changed("embedding-provider") {
				cmder.embeddingProvider = cfg.Embedding.Provider
			}
			if !cmd.Flags().Changed("embedding-target") {
				cmder.embeddingTarget = cfg.Embedding.Target
			}
			if !cmd.Flags().Changed("embedding-model") {
				cmder.embeddingModel = cfg.Embedding.Model
			}
			if !cmd.Flags().Changed("project") {
				cmder.project = cfg.Proxy.Project
			}
			if cmder.project == "" {
				cmder.project = git.RepoName()
			}
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

	defaults := config.NewDefaultConfig()
	cmd.Flags().StringVarP(&cmder.listen, "listen", "l", defaults.Proxy.Listen, "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", defaults.Proxy.Upstream, "Upstream LLM provider URL")
	cmd.Flags().StringVarP(&cmder.providerType, "provider", "p", defaults.Proxy.Provider, "LLM provider type (anthropic, openai, ollama)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (default: in-memory)")
	cmd.Flags().StringVar(&cmder.vectorStoreProvider, "vector-store-provider", defaults.VectorStore.Provider, "Vector store provider type (e.g., chroma, sqlite)")
	cmd.Flags().StringVar(&cmder.vectorStoreTarget, "vector-store-target", defaults.VectorStore.Target, "Vector store URL (e.g., http://localhost:8000)")
	cmd.Flags().StringVar(&cmder.embeddingProvider, "embedding-provider", defaults.Embedding.Provider, "Embedding provider type (e.g., ollama)")
	cmd.Flags().StringVar(&cmder.embeddingTarget, "embedding-target", defaults.Embedding.Target, "Embedding provider URL")
	cmd.Flags().StringVar(&cmder.embeddingModel, "embedding-model", defaults.Embedding.Model, "Embedding model name (e.g., nomic-embed-text)")
	cmd.Flags().StringVar(&cmder.project, "project", "", "Project name to tag sessions (default: auto-detect from git)")

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
		Project:      c.project,
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

			// TODO - need to make this actually configurable
			Dimensions: 1024,
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

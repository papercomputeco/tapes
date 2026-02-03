// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			return cmder.run()
		},
	}

	cmd.Flags().StringVarP(&cmder.listen, "listen", "l", ":8080", "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", "http://localhost:11434", "Upstream LLM provider URL")
	cmd.Flags().StringVarP(&cmder.providerType, "provider", "p", "ollama", "LLM provider type (anthropic, openai, ollama)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (default: in-memory)")
	cmd.Flags().StringVar(&cmder.vectorStoreProvider, "vector-store-provider", "sqlite", "Vector store provider type (e.g., chroma, sqlite)")
	cmd.Flags().StringVar(&cmder.vectorStoreTarget, "vector-store-target", "", "Vector store URL (e.g., http://localhost:8000)")
	cmd.Flags().StringVar(&cmder.embeddingProvider, "embedding-provider", "", "Embedding provider type (e.g., ollama)")
	cmd.Flags().StringVar(&cmder.embeddingTarget, "embedding-target", "", "Embedding provider URL")
	cmd.Flags().StringVar(&cmder.embeddingModel, "embedding-model", "", "Embedding model name (e.g., nomic-embed-text)")

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

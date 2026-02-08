// Package servecmder provides the serve command with subcommands for running services.
package servecmder

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/api"
	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/dotdir"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	vectorutils "github.com/papercomputeco/tapes/pkg/vector/utils"
	"github.com/papercomputeco/tapes/proxy"
)

type ServeCommander struct {
	proxyListen string
	apiListen   string
	upstream    string
	debug       bool
	sqlitePath  string

	providerType string

	vectorStoreProvider string
	vectorStoreTarget   string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint

	logger *zap.Logger
}

const serveLongDesc string = `Run Tapes services.

Use subcommands to run individual services or all services together:
  tapes serve          Run both proxy and API server together
  tapes serve api      Run just the API server
  tapes serve proxy    Run just the proxy server

Optionally configure vector storage and embeddings of text content for "tapes search"
agentic functionality.`

const serveShortDesc string = "Run Tapes services"

func NewServeCmd() *cobra.Command {
	cmder := &ServeCommander{}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: serveShortDesc,
		Long:  serveLongDesc,
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

			// Resolve default sqlite path from dotdir target.
			dotdirManager := dotdir.NewManager()
			defaultTargetDir, err := dotdirManager.Target(configDir)
			if err != nil {
				return fmt.Errorf("resolving target dir: %w", err)
			}
			defaultTargetSqliteFile := filepath.Join(defaultTargetDir, "tapes.sqlite")

			if !cmd.Flags().Changed("proxy-listen") {
				cmder.proxyListen = cfg.Proxy.Listen
			}
			if !cmd.Flags().Changed("api-listen") {
				cmder.apiListen = cfg.API.Listen
			}
			if !cmd.Flags().Changed("upstream") {
				cmder.upstream = cfg.Proxy.Upstream
			}
			if !cmd.Flags().Changed("provider") {
				cmder.providerType = cfg.Proxy.Provider
			}
			if !cmd.Flags().Changed("sqlite") {
				if cfg.Storage.SQLitePath != "" {
					cmder.sqlitePath = cfg.Storage.SQLitePath
				} else {
					cmder.sqlitePath = defaultTargetSqliteFile
				}
			}
			if !cmd.Flags().Changed("vector-store-provider") {
				cmder.vectorStoreProvider = cfg.VectorStore.Provider
			}
			if !cmd.Flags().Changed("vector-store-target") {
				if cfg.VectorStore.Target != "" {
					cmder.vectorStoreTarget = cfg.VectorStore.Target
				} else {
					cmder.vectorStoreTarget = defaultTargetSqliteFile
				}
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
			if !cmd.Flags().Changed("embedding-dimensions") {
				cmder.embeddingDimensions = cfg.Embedding.Dimensions
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
	cmd.Flags().StringVarP(&cmder.proxyListen, "proxy-listen", "p", defaults.Proxy.Listen, "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.apiListen, "api-listen", "a", defaults.API.Listen, "Address for API server to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", defaults.Proxy.Upstream, "Upstream LLM provider URL")
	cmd.Flags().StringVar(&cmder.providerType, "provider", defaults.Proxy.Provider, "LLM provider type (anthropic, openai, ollama, vertex)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (e.g., ./tapes.sqlite, in-memory)")
	cmd.Flags().StringVar(&cmder.vectorStoreProvider, "vector-store-provider", defaults.VectorStore.Provider, "Vector store provider type (e.g., chroma, sqlite)")
	cmd.Flags().StringVar(&cmder.vectorStoreTarget, "vector-store-target", defaults.VectorStore.Target, "Vector store target filepath for sqlite or URL for vector store service (e.g., http://localhost:8000, ./db.sqlite)")
	cmd.Flags().StringVar(&cmder.embeddingProvider, "embedding-provider", defaults.Embedding.Provider, "Embedding provider type (e.g., ollama)")
	cmd.Flags().StringVar(&cmder.embeddingTarget, "embedding-target", defaults.Embedding.Target, "Embedding provider URL")
	cmd.Flags().StringVar(&cmder.embeddingModel, "embedding-model", defaults.Embedding.Model, "Embedding model name (e.g., nomic-embed-text)")
	cmd.Flags().UintVar(&cmder.embeddingDimensions, "embedding-dimensions", defaults.Embedding.Dimensions, "Embedding dimensionality.")

	cmd.AddCommand(apicmder.NewAPICmd())
	cmd.AddCommand(proxycmder.NewProxyCmd())

	return cmd
}

func (c *ServeCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer func() { _ = c.logger.Sync() }()

	// Create shared driver
	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	dagLoader, err := c.newDagLoader()
	if err != nil {
		return err
	}
	defer driver.Close()

	proxyConfig := proxy.Config{
		ListenAddr:   c.proxyListen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
	}

	proxyConfig.VectorDriver, err = vectorutils.NewVectorDriver(&vectorutils.NewVectorDriverOpts{
		ProviderType: c.vectorStoreProvider,
		Target:       c.vectorStoreTarget,
		Dimensions:   c.embeddingDimensions,
		Logger:       c.logger,
	})
	if err != nil {
		return fmt.Errorf("creating vector driver: %w", err)
	}
	defer proxyConfig.VectorDriver.Close()

	proxyConfig.Embedder, err = embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: c.embeddingProvider,
		TargetURL:    c.embeddingTarget,
		Model:        c.embeddingModel,
	})
	if err != nil {
		return fmt.Errorf("creating embedder: %w", err)
	}
	defer proxyConfig.Embedder.Close()

	c.logger.Info("vector storage enabled",
		zap.String("vector_store_provider", c.vectorStoreProvider),
		zap.String("vector_store_target", c.vectorStoreTarget),
		zap.String("embedding_provider", c.embeddingProvider),
		zap.String("embedding_target", c.embeddingTarget),
		zap.String("embedding_model", c.embeddingModel),
	)

	// Create proxy
	p, err := proxy.New(proxyConfig, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer p.Close()

	c.logger.Info("starting proxy",
		zap.String("proxy_addr", c.proxyListen),
		zap.String("upstream", c.upstream),
		zap.String("provider", c.providerType),
	)

	// Create API server
	apiConfig := api.Config{
		ListenAddr:   c.apiListen,
		VectorDriver: proxyConfig.VectorDriver,
		Embedder:     proxyConfig.Embedder,
	}
	apiServer, err := api.NewServer(apiConfig, driver, dagLoader, c.logger)
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}

	c.logger.Info("starting api server",
		zap.String("api_addr", c.apiListen),
	)

	// Channel to capture errors from goroutines
	errChan := make(chan error, 2)

	// Start proxy in goroutine
	go func() {
		if err := p.Run(); err != nil {
			errChan <- fmt.Errorf("proxy error: %w", err)
		}
	}()

	// Start API server in goroutine
	go func() {
		if err := apiServer.Run(); err != nil {
			errChan <- fmt.Errorf("API server error: %w", err)
		}
	}()

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		return err
	case sig := <-sigChan:
		c.logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		return nil
	}
}

func (c *ServeCommander) newStorageDriver() (storage.Driver, error) {
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

func (c *ServeCommander) newDagLoader() (merkle.DagLoader, error) {
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

// Package servecmder provides the serve command with subcommands for running services.
package servecmder

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/api"
	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
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

	embeddingProvider string
	embeddingTarget   string
	embeddingModel    string

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
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}
			return cmder.run()
		},
	}

	cmd.Flags().StringVarP(&cmder.proxyListen, "proxy-listen", "p", ":8080", "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.apiListen, "api-listen", "a", ":8081", "Address for API server to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", "http://localhost:11434", "Upstream LLM provider URL")
	cmd.Flags().StringVar(&cmder.providerType, "provider", "ollama", "LLM provider type (anthropic, openai, ollama, besteffort)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (default: in-memory)")
	cmd.Flags().StringVar(&cmder.vectorStoreProvider, "vector-store-provider", "", "Vector store provider type (e.g., chroma)")
	cmd.Flags().StringVar(&cmder.vectorStoreTarget, "vector-store-target", "", "Vector store URL (e.g., http://localhost:8000)")
	cmd.Flags().StringVar(&cmder.embeddingProvider, "embedding-provider", "", "Embedding provider type (e.g., ollama)")
	cmd.Flags().StringVar(&cmder.embeddingTarget, "embedding-target", "", "Embedding provider URL")
	cmd.Flags().StringVar(&cmder.embeddingModel, "embedding-model", "", "Embedding model name (e.g., nomic-embed-text)")

	cmd.AddCommand(apicmder.NewAPICmd())
	cmd.AddCommand(proxycmder.NewProxyCmd())

	return cmd
}

func (c *ServeCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer c.logger.Sync()

	// Create shared driver
	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	proxyConfig := proxy.Config{
		ListenAddr:   c.proxyListen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
	}

	if c.vectorStoreTarget != "" {
		proxyConfig.Embedder, err = embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
			ProviderType: c.embeddingProvider,
			TargetURL:    c.embeddingTarget,
			Model:        c.embeddingModel,
		})
		if err != nil {
			return fmt.Errorf("creating embedder: %w", err)
		}
		defer proxyConfig.Embedder.Close()

		proxyConfig.VectorDriver, err = vectorutils.NewVectorDriver(&vectorutils.NewVectorDriverOpts{
			ProviderType: c.vectorStoreProvider,
			TargetURL:    c.vectorStoreTarget,
			Logger:       c.logger,
		})
		if err != nil {
			return fmt.Errorf("creating vector driver: %w", err)
		}
		defer proxyConfig.VectorDriver.Close()

		c.logger.Info("vector storage enabled",
			zap.String("vector_store_provider", c.vectorStoreProvider),
			zap.String("vector_store_target", c.vectorStoreTarget),
			zap.String("embedding_provider", c.embeddingProvider),
			zap.String("embedding_target", c.embeddingTarget),
			zap.String("embedding_model", c.embeddingModel),
		)
	}

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
	apiServer, err := api.NewServer(apiConfig, driver, c.logger)
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
		driver, err := sqlite.NewSQLiteDriver(c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		c.logger.Info("using SQLite storage", zap.String("path", c.sqlitePath))
		return driver, nil
	}

	c.logger.Info("using in-memory storage")
	return inmemory.NewInMemoryDriver(), nil
}

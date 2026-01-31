// Package servecmder provides the serve command with subcommands for running services.
package servecmder

import (
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}
			return cmder.run()
		},
	}

	dotdirManger := dotdir.NewManager()
	defaultTargetDir, err := dotdirManger.Target("")
	if err != nil {
		panic(err)
	}
	defaultTargetSqliteFile := filepath.Join(defaultTargetDir, "tapes.sqlite")

	cmd.Flags().StringVarP(&cmder.proxyListen, "proxy-listen", "p", ":8080", "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.apiListen, "api-listen", "a", ":8081", "Address for API server to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", "http://localhost:11434", "Upstream LLM provider URL")
	cmd.Flags().StringVar(&cmder.providerType, "provider", "ollama", "LLM provider type (anthropic, openai, ollama, besteffort)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", defaultTargetSqliteFile, "Path to SQLite database (e.g., ./tapes.sqlite, in-memory)")
	cmd.Flags().StringVar(&cmder.vectorStoreProvider, "vector-store-provider", "sqlite", "Vector store provider type (e.g., chroma, sqlite)")
	cmd.Flags().StringVar(&cmder.vectorStoreTarget, "vector-store-target", defaultTargetSqliteFile, "Vector store target fielpath for sqlite or URL for vector store service (e.g., http://localhost:8000, ./db.sqlite)")
	cmd.Flags().StringVar(&cmder.embeddingProvider, "embedding-provider", "ollama", "Embedding provider type (e.g., ollama)")
	cmd.Flags().StringVar(&cmder.embeddingTarget, "embedding-target", "http://localhost:11434", "Embedding provider URL")
	cmd.Flags().StringVar(&cmder.embeddingModel, "embedding-model", "embeddinggemma", "Embedding model name (e.g., embeddinggemma, nomic-embed-text)")
	cmd.Flags().UintVar(&cmder.embeddingDimensions, "embedding-dimensions", 768, "Embedding dimensionality.")

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

func (c *ServeCommander) newDagLoader() (merkle.DagLoader, error) {
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

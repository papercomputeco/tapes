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
	flags config.FlagSet

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

// ServeFlags is the shared FlagSet for all serve-family commands.
// The parent "tapes serve" and the "tapes serve proxy" / "tapes serve api"
// subcommands each pick the subset they need from this set.
var ServeFlags = config.FlagSet{
	config.FlagProxyListen:           {Name: "proxy-listen", Shorthand: "p", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagAPIListen:             {Name: "api-listen", Shorthand: "a", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagUpstream:              {Name: "upstream", Shorthand: "u", ViperKey: "proxy.upstream", Description: "Upstream LLM provider URL"},
	config.FlagProvider:              {Name: "provider", ViperKey: "proxy.provider", Description: "LLM provider type (anthropic, openai, ollama)"},
	config.FlagSQLite:                {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagVectorStoreProv:       {Name: "vector-store-provider", ViperKey: "vector_store.provider", Description: "Vector store provider type (e.g., chroma, sqlite)"},
	config.FlagVectorStoreTgt:        {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "Vector store target: filepath for sqlite or URL for remote service"},
	config.FlagEmbeddingProv:         {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama)"},
	config.FlagEmbeddingTgt:          {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel:        {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., nomic-embed-text)"},
	config.FlagEmbeddingDims:         {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
	config.FlagProxyListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagAPIListenStandalone:   {Name: "listen", Shorthand: "l", ViperKey: "api.listen", Description: "Address for API server to listen on"},
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
	cmder := &ServeCommander{
		flags: ServeFlags,
	}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: serveShortDesc,
		Long:  serveLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagProxyListen,
				config.FlagAPIListen,
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

			// Resolve default sqlite path from dotdir target when not set
			// via flag, env, or config file.
			if v.GetString("storage.sqlite_path") == "" {
				dotdirManager := dotdir.NewManager()
				defaultTargetDir, err := dotdirManager.Target(configDir)
				if err != nil {
					return fmt.Errorf("resolving target dir: %w", err)
				}
				if defaultTargetDir != "" {
					v.Set("storage.sqlite_path", filepath.Join(defaultTargetDir, "tapes.sqlite"))
				}
			}

			// Same fallback for vector store target.
			if v.GetString("vector_store.target") == "" {
				dotdirManager := dotdir.NewManager()
				defaultTargetDir, err := dotdirManager.Target(configDir)
				if err != nil {
					return fmt.Errorf("resolving target dir: %w", err)
				}
				if defaultTargetDir != "" {
					v.Set("vector_store.target", filepath.Join(defaultTargetDir, "tapes.sqlite"))
				}
			}

			cmder.proxyListen = v.GetString("proxy.listen")
			cmder.apiListen = v.GetString("api.listen")
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

	config.AddStringFlag(cmd, cmder.flags, config.FlagProxyListen, &cmder.proxyListen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPIListen, &cmder.apiListen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagUpstream, &cmder.upstream)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProvider, &cmder.providerType)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreProv, &cmder.vectorStoreProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)

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

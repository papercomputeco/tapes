// Package servecmder provides the serve command with subcommands for running services.
package servecmder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
	ingestcmder "github.com/papercomputeco/tapes/cmd/tapes/serve/ingest"
	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/publisher"
	kafkapublisher "github.com/papercomputeco/tapes/pkg/publisher/kafka"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
	"github.com/papercomputeco/tapes/pkg/vector/pgvector"
	"github.com/papercomputeco/tapes/proxy"
)

type ServeCommander struct {
	flags config.FlagSet

	proxyListen  string
	apiListen    string
	apiWebUI     bool
	ingestListen string
	upstream     string
	debug        bool
	postgresDSN  string
	project      string

	providerType string

	kafkaBrokers  string
	kafkaTopic    string
	kafkaClientID string

	vectorStoreTarget string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint
	embeddingAPIKey     string

	logger *slog.Logger
}

// ServeFlags defines the flags for the parent "tapes serve" command.
var ServeFlags = config.FlagSet{
	config.FlagProxyListen:    {Name: "proxy-listen", Shorthand: "p", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagAPIListen:      {Name: "api-listen", Shorthand: "a", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagAPIWebUI:       {Name: "api-web-ui", ViperKey: "api.web_ui", Description: "Enable the minimal browser UI at /"},
	config.FlagIngestListen:   {Name: "ingest-listen", Shorthand: "i", ViperKey: "ingest.listen", Description: "Address for ingest server to listen on (sidecar mode)"},
	config.FlagUpstream:       {Name: "upstream", Shorthand: "u", ViperKey: "proxy.upstream", Description: "Upstream LLM provider URL"},
	config.FlagProvider:       {Name: "provider", ViperKey: "proxy.provider", Description: "LLM provider type (anthropic, openai, ollama)"},
	config.FlagPostgres:       {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:        {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
	config.FlagVectorStoreTgt: {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "pgvector connection string (defaults to storage.postgres_dsn when unset)"},
	config.FlagEmbeddingProv:  {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama, openai)"},
	config.FlagEmbeddingTgt:   {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel: {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., embeddinggemma, text-embedding-3-large)"},
	config.FlagEmbeddingDims:  {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
	config.FlagKafkaBrokers:   {Name: "kafka-brokers", ViperKey: "publisher.kafka.brokers", Description: "Comma separated list of broker ip:port pairs"},
	config.FlagKafkaClientID:  {Name: "kafka-client-id", ViperKey: "publisher.kafka.client_id", Description: "Optional Kafka client.id"},
	config.FlagKafkaTopic:     {Name: "kafka-topic", ViperKey: "publisher.kafka.topic", Description: "Name of topic to publish session events (e.g. tapes.nodes.v1)"},
}

const serveLongDesc string = `Run Tapes services.

Use subcommands to run individual services or all services together:
  tapes serve            Run proxy and API server together
  tapes serve api        Run just the API server
  tapes serve proxy      Run just the proxy server
  tapes serve ingest     Run just the ingest server (sidecar mode)

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
				config.FlagAPIWebUI,
				config.FlagIngestListen,
				config.FlagUpstream,
				config.FlagProvider,
				config.FlagPostgres,
				config.FlagProject,
				config.FlagVectorStoreTgt,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
				config.FlagKafkaBrokers,
				config.FlagKafkaClientID,
				config.FlagKafkaTopic,
			})

			// Default pgvector to the primary Postgres DSN.
			if v.GetString("vector_store.target") == "" && v.GetString("storage.postgres_dsn") != "" {
				v.Set("vector_store.target", v.GetString("storage.postgres_dsn"))
			}

			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.proxyListen = v.GetString("proxy.listen")
			cmder.apiListen = v.GetString("api.listen")
			cmder.apiWebUI = v.GetBool("api.web_ui")
			cmder.ingestListen = v.GetString("ingest.listen")
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
			cmder.kafkaBrokers = v.GetString("publisher.kafka.brokers")
			cmder.kafkaClientID = v.GetString("publisher.kafka.client_id")
			cmder.kafkaTopic = v.GetString("publisher.kafka.topic")
			cmder.project = v.GetString("proxy.project")

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
			telemetry.FromContext(cmd.Context()).CaptureServerStarted("both")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagProxyListen, &cmder.proxyListen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPIListen, &cmder.apiListen)
	config.AddBoolFlag(cmd, cmder.flags, config.FlagAPIWebUI, &cmder.apiWebUI)
	config.AddStringFlag(cmd, cmder.flags, config.FlagIngestListen, &cmder.ingestListen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagUpstream, &cmder.upstream)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProvider, &cmder.providerType)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaBrokers, &cmder.kafkaBrokers)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaClientID, &cmder.kafkaClientID)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaTopic, &cmder.kafkaTopic)

	cmd.AddCommand(apicmder.NewAPICmd())
	cmd.AddCommand(ingestcmder.NewIngestCmd())
	cmd.AddCommand(proxycmder.NewProxyCmd())

	return cmd
}

func (c *ServeCommander) validatePublisherConfig() error {
	kafkaBrokers := splitKafkaBrokers(c.kafkaBrokers)
	kafkaTopic := strings.TrimSpace(c.kafkaTopic)

	if len(kafkaBrokers) == 0 && kafkaTopic == "" {
		return nil
	}

	if len(kafkaBrokers) == 0 {
		return errors.New("kafka brokers are required when kafka topic is set")
	}

	if kafkaTopic == "" {
		return errors.New("kafka topic is required when kafka brokers are set")
	}

	return nil
}

func splitKafkaBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}

func (c *ServeCommander) newPublisher() (publisher.Publisher, error) {
	kafkaBrokers := splitKafkaBrokers(c.kafkaBrokers)
	kafkaTopic := strings.TrimSpace(c.kafkaTopic)
	if len(kafkaBrokers) == 0 && kafkaTopic == "" {
		return nil, nil
	}

	return kafkapublisher.NewPublisher(kafkapublisher.Config{
		Brokers:  kafkaBrokers,
		Topic:    kafkaTopic,
		ClientID: strings.TrimSpace(c.kafkaClientID),
	})
}

func (c *ServeCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	if err := c.validatePublisherConfig(); err != nil {
		return err
	}

	proxyPub, err := c.newPublisher()
	if err != nil {
		return fmt.Errorf("creating proxy publisher: %w", err)
	}
	defer func() {
		if proxyPub != nil {
			_ = proxyPub.Close()
		}
	}()

	ingestPub, err := c.newPublisher()
	if err != nil {
		return fmt.Errorf("creating ingest publisher: %w", err)
	}
	defer func() {
		if ingestPub != nil {
			_ = ingestPub.Close()
		}
	}()

	driver, err := postgres.NewDriver(context.TODO(), c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	proxyConfig := proxy.Config{
		ListenAddr:   c.proxyListen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
		Publisher:    proxyPub,
		Project:      c.project,
	}

	proxyConfig.VectorDriver, err = pgvector.NewDriver(context.TODO(), &pgvector.Config{
		ConnString: c.vectorStoreTarget,
		Dimensions: c.embeddingDimensions,
	}, c.logger)
	if err != nil {
		return fmt.Errorf("could not create new vector driver: %w", err)
	}
	defer proxyConfig.VectorDriver.Close()

	proxyConfig.Embedder, err = embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: c.embeddingProvider,
		TargetURL:    c.embeddingTarget,
		Model:        c.embeddingModel,
		Dimensions:   c.embeddingDimensions,
		APIKey:       c.embeddingAPIKey,
	})
	if err != nil {
		return fmt.Errorf("creating embedder: %w", err)
	}
	defer proxyConfig.Embedder.Close()

	c.logger.Info("vector storage enabled",
		"vector_store_target", config.RedactDSN(c.vectorStoreTarget),
		"embedding_provider", c.embeddingProvider,
		"embedding_target", c.embeddingTarget,
		"embedding_model", c.embeddingModel,
	)

	// Create proxy
	p, err := proxy.New(proxyConfig, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer p.Close()

	c.logger.Info("starting proxy",
		"proxy_addr", c.proxyListen,
		"upstream", c.upstream,
		"provider", c.providerType,
	)

	// Create API server
	apiConfig := api.Config{
		ListenAddr:   c.apiListen,
		VectorDriver: proxyConfig.VectorDriver,
		Embedder:     proxyConfig.Embedder,
		EnableWebUI:  c.apiWebUI,
	}
	apiServer, err := api.NewServer(apiConfig, driver, c.logger)
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}

	c.logger.Info("starting api server",
		"api_addr", c.apiListen,
	)

	// Optionally create ingest server for sidecar mode
	ingestConfig := ingest.Config{
		ListenAddr:   c.ingestListen,
		VectorDriver: proxyConfig.VectorDriver,
		Embedder:     proxyConfig.Embedder,
		Publisher:    ingestPub,
		Project:      c.project,
	}
	ingestServer, err := ingest.New(ingestConfig, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating ingest server: %w", err)
	}
	defer ingestServer.Close()

	c.logger.Info("starting ingest server",
		"ingest_addr", c.ingestListen,
	)

	// Channel to capture errors from goroutines
	errChan := make(chan error, 3)

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

	go func() {
		if err := ingestServer.Run(); err != nil {
			errChan <- fmt.Errorf("ingest server error: %w", err)
		}
	}()

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		return err
	case sig := <-sigChan:
		c.logger.Info("received signal, shutting down", "signal", sig.String())
		return nil
	}
}

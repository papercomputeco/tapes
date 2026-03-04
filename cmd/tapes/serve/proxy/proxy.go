// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/publisher"
	kafkapublisher "github.com/papercomputeco/tapes/pkg/publisher/kafka"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
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
	postgresDSN  string
	project      string

	kafkaBrokers  string
	kafkaTopic    string
	kafkaClientID string

	vectorStoreProvider string
	vectorStoreTarget   string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint

	logger *slog.Logger
}

// proxyFlags defines the flags for the standalone proxy subcommand.
// Uses FlagProxyListenStandalone (--listen/-l) instead of the parent's
// --proxy-listen/-p, and omits --api-listen since this is proxy-only.
var proxyFlags = config.FlagSet{
	config.FlagProxyListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagUpstream:              {Name: "upstream", Shorthand: "u", ViperKey: "proxy.upstream", Description: "Upstream LLM provider URL"},
	config.FlagProvider:              {Name: "provider", ViperKey: "proxy.provider", Description: "LLM provider type (anthropic, openai, ollama)"},
	config.FlagSQLite:                {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagPostgres:              {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:               {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
	config.FlagVectorStoreProv:       {Name: "vector-store-provider", ViperKey: "vector_store.provider", Description: "Vector store provider type (e.g., chroma, sqlite)"},
	config.FlagVectorStoreTgt:        {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "Vector store target: filepath for sqlite or URL for remote service"},
	config.FlagEmbeddingProv:         {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama)"},
	config.FlagEmbeddingTgt:          {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel:        {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., nomic-embed-text)"},
	config.FlagEmbeddingDims:         {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
	config.FlagKafkaBrokers:          {Name: "kafka-brokers", ViperKey: "publisher.kafka.brokers", Description: "Comma separated list of broker ip:port pairs"},
	config.FlagKafkaClientID:         {Name: "kafka-client-id", ViperKey: "publisher.kafka.client_id", Description: "Optional Kafka client.id"},
	config.FlagKafkaTopic:            {Name: "kafka-topic", ViperKey: "publisher.kafka.topic", Description: "Name of topic to publish session events (e.g. tapes.nodes.v1)"},
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
				config.FlagPostgres,
				config.FlagProject,
				config.FlagVectorStoreProv,
				config.FlagVectorStoreTgt,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
				config.FlagKafkaBrokers,
				config.FlagKafkaClientID,
				config.FlagKafkaTopic,
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
			cmder.project = v.GetString("proxy.project")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.kafkaBrokers = v.GetString("publisher.kafka.brokers")
			cmder.kafkaClientID = v.GetString("publisher.kafka.client_id")
			cmder.kafkaTopic = v.GetString("publisher.kafka.topic")

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

			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagProxyListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagUpstream, &cmder.upstream)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProvider, &cmder.providerType)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreProv, &cmder.vectorStoreProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaBrokers, &cmder.kafkaBrokers)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaClientID, &cmder.kafkaClientID)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaTopic, &cmder.kafkaTopic)

	return cmd
}

func (c *proxyCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	if err := c.validatePublisherConfig(); err != nil {
		return err
	}

	pub, err := c.newPublisher()
	if err != nil {
		return fmt.Errorf("creating publisher: %w", err)
	}
	defer func() {
		if pub != nil {
			_ = pub.Close()
		}
	}()

	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	if err := driver.Migrate(context.Background()); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	config := proxy.Config{
		ListenAddr:   c.listen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
		Publisher:    pub,
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
			Dimensions:   c.embeddingDimensions,
		})
		if err != nil {
			return fmt.Errorf("creating vector driver: %w", err)
		}
		defer config.VectorDriver.Close()

		c.logger.Info("vector storage enabled",
			"vector_store_provider", c.vectorStoreProvider,
			"vector_store_target", c.vectorStoreTarget,
			"embedding_provider", c.embeddingProvider,
			"embedding_target", c.embeddingTarget,
			"embedding_model", c.embeddingModel,
		)
	}

	p, err := proxy.New(config, driver, c.logger)
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

func (c *proxyCommander) validatePublisherConfig() error {
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

func (c *proxyCommander) newPublisher() (publisher.Publisher, error) {
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

func (c *proxyCommander) newStorageDriver() (storage.Driver, error) {
	if c.postgresDSN != "" {
		driver, err := postgres.NewDriver(context.Background(), c.postgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL storer: %w", err)
		}
		c.logger.Info("using PostgreSQL storage")
		return driver, nil
	}

	if c.sqlitePath != "" {
		driver, err := sqlite.NewDriver(context.Background(), c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		c.logger.Info("using SQLite storage", "path", c.sqlitePath)
		return driver, nil
	}

	c.logger.Info("using in-memory storage")
	return inmemory.NewDriver(), nil
}

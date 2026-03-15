// Package ingestcmder provides the ingest server cobra command.
package ingestcmder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/ingest"
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
	"github.com/papercomputeco/tapes/pkg/telemetry"
	vectorutils "github.com/papercomputeco/tapes/pkg/vector/utils"
)

type ingestCommander struct {
	flags config.FlagSet

	listen      string
	debug       bool
	sqlitePath  string
	postgresDSN string
	project     string

	vectorStoreProvider string
	vectorStoreTarget   string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint

	kafkaBrokers  string
	kafkaTopic    string
	kafkaClientID string

	logger *slog.Logger
}

// ingestFlags defines the flags for the standalone ingest subcommand.
// Uses FlagIngestListenStandalone (--listen/-l) instead of the parent's
// --ingest-listen/-i, and omits proxy/api-specific flags.
var ingestFlags = config.FlagSet{
	config.FlagIngestListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "ingest.listen", Description: "Address for ingest server to listen on"},
	config.FlagSQLite:                 {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagPostgres:               {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:                {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
	config.FlagVectorStoreProv:        {Name: "vector-store-provider", ViperKey: "vector_store.provider", Description: "Vector store provider type (e.g., chroma, sqlite, qdrant)"},
	config.FlagVectorStoreTgt:         {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "Vector store target: filepath for sqlite or URL for remote service"},
	config.FlagEmbeddingProv:          {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama)"},
	config.FlagEmbeddingTgt:           {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel:         {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., nomic-embed-text)"},
	config.FlagEmbeddingDims:          {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
	config.FlagKafkaBrokers:           {Name: "kafka-brokers", ViperKey: "publisher.kafka.brokers", Description: "Comma separated list of broker ip:port pairs"},
	config.FlagKafkaClientID:          {Name: "kafka-client-id", ViperKey: "publisher.kafka.client_id", Description: "Optional Kafka client.id"},
	config.FlagKafkaTopic:             {Name: "kafka-topic", ViperKey: "publisher.kafka.topic", Description: "Name of topic to publish session events (e.g. tapes.nodes.v1)"},
}

const ingestLongDesc string = `Run the ingest server (sidecar mode).

The ingest server accepts completed LLM conversation turns via HTTP and stores
them in the Merkle DAG. Use this when an external gateway (e.g., Envoy AI Gateway)
handles upstream LLM traffic and tapes only needs to store, embed, and publish data.

Endpoints:
  POST /v1/ingest        Accept a single conversation turn
  POST /v1/ingest/batch  Accept multiple conversation turns

Optionally configure vector storage and embeddings for "tapes search" functionality.`

const ingestShortDesc string = "Run the Tapes ingest server (sidecar mode)"

// NewIngestCmd creates the cobra command for the standalone ingest server.
func NewIngestCmd() *cobra.Command {
	cmder := &ingestCommander{
		flags: ingestFlags,
	}

	cmd := &cobra.Command{
		Use:   "ingest",
		Short: ingestShortDesc,
		Long:  ingestLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagIngestListenStandalone,
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

			cmder.listen = v.GetString("ingest.listen")
			cmder.sqlitePath = v.GetString("storage.sqlite_path")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.project = v.GetString("proxy.project")
			cmder.vectorStoreProvider = v.GetString("vector_store.provider")
			cmder.vectorStoreTarget = v.GetString("vector_store.target")
			cmder.embeddingProvider = v.GetString("embedding.provider")
			cmder.embeddingTarget = v.GetString("embedding.target")
			cmder.embeddingModel = v.GetString("embedding.model")
			cmder.embeddingDimensions = v.GetUint("embedding.dimensions")
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

			telemetry.FromContext(cmd.Context()).CaptureServerStarted("ingest")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagIngestListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreProv, &cmder.vectorStoreProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaBrokers, &cmder.kafkaBrokers)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaClientID, &cmder.kafkaClientID)
	config.AddStringFlag(cmd, cmder.flags, config.FlagKafkaTopic, &cmder.kafkaTopic)

	return cmd
}

func (c *ingestCommander) run() error {
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

	cfg := ingest.Config{
		ListenAddr: c.listen,
		Publisher:  pub,
		Project:    c.project,
	}

	if c.vectorStoreTarget != "" {
		cfg.Embedder, err = embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
			ProviderType: c.embeddingProvider,
			TargetURL:    c.embeddingTarget,
			Model:        c.embeddingModel,
		})
		if err != nil {
			return fmt.Errorf("creating embedder: %w", err)
		}
		defer cfg.Embedder.Close()

		cfg.VectorDriver, err = vectorutils.NewVectorDriver(&vectorutils.NewVectorDriverOpts{
			ProviderType: c.vectorStoreProvider,
			Target:       c.vectorStoreTarget,
			Logger:       c.logger,
			Dimensions:   c.embeddingDimensions,
		})
		if err != nil {
			return fmt.Errorf("creating vector driver: %w", err)
		}
		defer cfg.VectorDriver.Close()

		c.logger.Info("vector storage enabled",
			"vector_store_provider", c.vectorStoreProvider,
			"vector_store_target", c.vectorStoreTarget,
			"embedding_provider", c.embeddingProvider,
			"embedding_target", c.embeddingTarget,
			"embedding_model", c.embeddingModel,
		)
	}

	s, err := ingest.New(cfg, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating ingest server: %w", err)
	}
	defer s.Close()

	c.logger.Info("starting ingest server",
		"listen", c.listen,
	)

	return s.Run()
}

func (c *ingestCommander) validatePublisherConfig() error {
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

func (c *ingestCommander) newPublisher() (publisher.Publisher, error) {
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

func (c *ingestCommander) newStorageDriver() (storage.Driver, error) {
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

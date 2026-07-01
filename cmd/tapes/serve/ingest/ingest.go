// Package ingestcmder provides the ingest server cobra command.
package ingestcmder

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type ingestCommander struct {
	flags config.FlagSet

	listen      string
	debug       bool
	postgresDSN string
	project     string

	vectorStoreTarget string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint
	embeddingAPIKey     string

	logger *slog.Logger
}

// ingestFlags defines the flags for the standalone ingest subcommand.
// Uses FlagIngestListenStandalone (--listen/-l) instead of the parent's
// --ingest-listen/-i, and omits proxy/api-specific flags.
var ingestFlags = config.FlagSet{
	config.FlagIngestListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "ingest.listen", Description: "Address for ingest server to listen on"},
	config.FlagPostgres:               {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:                {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
	config.FlagVectorStoreTgt:         {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "pgvector connection string (defaults to storage.postgres_dsn when unset)"},
	config.FlagEmbeddingProv:          {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Deprecated here; embeddings are written by the embed worker (tapes serve embed-worker)"},
	config.FlagEmbeddingTgt:           {Name: "embedding-target", ViperKey: "embedding.target", Description: "Deprecated here; embeddings are written by the embed worker (tapes serve embed-worker)"},
	config.FlagEmbeddingModel:         {Name: "embedding-model", ViperKey: "embedding.model", Description: "Deprecated here; embeddings are written by the embed worker (tapes serve embed-worker)"},
	config.FlagEmbeddingDims:          {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Deprecated here; embeddings are written by the embed worker (tapes serve embed-worker)"},
}

const ingestLongDesc string = `Run the ingest server (sidecar mode).

The ingest server accepts completed LLM conversation turns via HTTP and appends
them to the immutable raw-turn capture log. Use this when an external gateway
(e.g., Envoy AI Gateway) handles upstream LLM traffic and tapes only needs to
capture the turns for the deriver.

Endpoints:
  POST /v1/ingest        Accept a single conversation turn
  POST /v1/ingest/batch  Accept multiple conversation turns

Embeddings are no longer written at ingest time: the embed worker family is
the single writer (tapes serve embed-worker). The embedding flags remain
accepted for deployment compatibility but have no effect here.`

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
				config.FlagPostgres,
				config.FlagProject,
				config.FlagVectorStoreTgt,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
			})

			cmder.listen = v.GetString("ingest.listen")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.project = v.GetString("proxy.project")
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
			if cmder.vectorStoreTarget == "" && cmder.postgresDSN != "" {
				cmder.vectorStoreTarget = cmder.postgresDSN
			}

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
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)

	return cmd
}

func (c *ingestCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	driver, err := postgres.NewDriver(context.TODO(), c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	cfg := ingest.Config{
		ListenAddr: c.listen,
		Project:    c.project,
	}

	// Ingest-time embedding is retired: the embed worker family is the
	// single writer of embeddings (tapes serve embed-worker, or the
	// tapes dev embed-spans backfill). The embedding flags remain
	// accepted so existing deployments keep booting, but they no longer
	// have any effect here.
	if c.embeddingTarget != "" || c.embeddingModel != "" {
		c.logger.Info("ingest-time embedding is retired; embeddings are written by the embed worker",
			"see", "tapes serve embed-worker",
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

// Package apicmder provides the API tapes server cobra command.
package apicmder

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
	"github.com/papercomputeco/tapes/pkg/vector/pgvector"
)

type apiCommander struct {
	flags config.FlagSet

	listen      string
	debug       bool
	postgresDSN string
	webUI       bool

	vectorStoreTarget string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint
	embeddingAPIKey     string

	// skillModel overrides the chat model POST /v1/skills/generate uses.
	// Empty keeps the generator's per-provider default (the embedding
	// model is not a chat model, so the search config can't supply it).
	skillModel string

	logger *slog.Logger
}

// apiFlags defines the flags for the standalone API subcommand.
var apiFlags = config.FlagSet{
	config.FlagAPIListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagAPIWebUI:            {Name: "web-ui", ViperKey: "api.web_ui", Description: "Enable the minimal browser UI at /"},
	config.FlagPostgres:            {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagVectorStoreTgt:      {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "pgvector connection string (defaults to storage.postgres_dsn when unset)"},
	config.FlagEmbeddingProv:       {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama, openai)"},
	config.FlagEmbeddingTgt:        {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel:      {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., text-embedding-3-large)"},
	config.FlagEmbeddingDims:       {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
	config.FlagSkillModel:          {Name: "skill-model", ViperKey: "skill.model", Description: "Chat model for skill generation (defaults to the provider's chat model)"},
}

const apiLongDesc string = `Run the Tapes API server for inspecting, managing, and query agent sessions.`

const apiShortDesc string = "Run the Tapes API server"

func NewAPICmd() *cobra.Command {
	cmder := &apiCommander{
		flags: apiFlags,
	}

	cmd := &cobra.Command{
		Use:   "api",
		Short: apiShortDesc,
		Long:  apiLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagAPIListenStandalone,
				config.FlagAPIWebUI,
				config.FlagPostgres,
				config.FlagVectorStoreTgt,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
				config.FlagSkillModel,
			})

			cmder.listen = v.GetString("api.listen")
			cmder.webUI = v.GetBool("api.web_ui")
			cmder.skillModel = v.GetString("skill.model")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.vectorStoreTarget = v.GetString("vector_store.target")
			if cmder.vectorStoreTarget == "" && cmder.postgresDSN != "" {
				cmder.vectorStoreTarget = cmder.postgresDSN
			}
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
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			telemetry.FromContext(cmd.Context()).CaptureServerStarted("api")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagAPIListenStandalone, &cmder.listen)
	config.AddBoolFlag(cmd, cmder.flags, config.FlagAPIWebUI, &cmder.webUI)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagVectorStoreTgt, &cmder.vectorStoreTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSkillModel, &cmder.skillModel)

	return cmd
}

func (c *apiCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	driver, err := postgres.NewDriver(context.Background(), c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	apiConfig := api.Config{
		ListenAddr:  c.listen,
		EnableWebUI: c.webUI,
		// Skill generation reuses the search/embedding credential — the same
		// shared key resolved above for the embedder. The model is a separate
		// knob (--skill-model / TAPES_SKILL_MODEL): the embedding model is not
		// a chat model, so when unset the generator picks a chat-capable
		// per-provider default. Empty values fall back to the generator's
		// env/credentials resolution.
		SkillLLMProvider: c.embeddingProvider,
		SkillLLMModel:    c.skillModel,
		SkillLLMAPIKey:   c.embeddingAPIKey,
		SkillLLMBaseURL:  c.embeddingTarget,
	}

	if c.vectorStoreTarget != "" {
		apiConfig.Embedder, err = embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
			ProviderType: c.embeddingProvider,
			TargetURL:    c.embeddingTarget,
			Model:        c.embeddingModel,
			Dimensions:   c.embeddingDimensions,
			APIKey:       c.embeddingAPIKey,
		})
		if err != nil {
			return fmt.Errorf("could not create new embedder: %w", err)
		}
		defer apiConfig.Embedder.Close()

		apiConfig.VectorDriver, err = pgvector.NewDriver(context.Background(), &pgvector.Config{
			ConnString: c.vectorStoreTarget,
			Dimensions: c.embeddingDimensions,
		}, c.logger)
		if err != nil {
			return fmt.Errorf("could not create new vector driver: %w", err)
		}
		defer apiConfig.VectorDriver.Close()

		// Span search reads the span-embedding projection written by
		// the derive worker / embed-spans backfill. The store performs
		// no schema work here: until a writer has run, the endpoint
		// answers 503 with ErrNotInitialized instead of failing boot.
		apiConfig.SpanSearcher, err = spanembed.NewStore(driver.DB(), spanembed.StoreConfig{
			Dimensions: c.embeddingDimensions,
		}, c.logger)
		if err != nil {
			return fmt.Errorf("could not create span embedding store: %w", err)
		}

		c.logger.Info("vector search enabled",
			"vector_store_target", config.RedactDSN(c.vectorStoreTarget),
			"embedding_provider", c.embeddingProvider,
			"embedding_target", c.embeddingTarget,
			"embedding_model", c.embeddingModel,
		)
	}

	server, err := api.NewServer(apiConfig, driver, c.logger)
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}

	c.logger.Info("starting API server",
		"listen", c.listen,
	)

	return server.Run()
}

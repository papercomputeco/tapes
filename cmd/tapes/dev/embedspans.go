package devcmder

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

type embedSpansCommander struct {
	flags config.FlagSet

	postgresDSN string
	orgID       string
	batchSize   int
	debug       bool

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint
	embeddingAPIKey     string

	logger *slog.Logger
}

var embedSpansFlags = config.FlagSet{
	config.FlagPostgres:       {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagEmbeddingProv:  {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama, openai)"},
	config.FlagEmbeddingTgt:   {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel: {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., embeddinggemma, text-embedding-3-large)"},
	config.FlagEmbeddingDims:  {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality (must match the model's output)"},
}

const embedSpansLongDesc string = `One-shot span embedding backfill.

Embeds every eligible un-embedded span: main-conversation llm spans
(call_kind=main), using each span's delta-only content — the fresh
input plus the response rendered to text, never the re-sent history.
Shadow calls (permission checks, title generation) are excluded by
design; they poison search relevance.

Idempotent: embeddings are keyed by deterministic span identity and
gated by a content hash, so re-running embeds nothing new unless raw
content was re-derived differently or the model changed. Orphaned
embeddings (spans pruned by a re-derive) are removed first.

The model and dimensions are an explicit pair: the vector table is
created with exactly --embedding-dimensions and the run fails fast if
an existing table or the model's actual output disagrees.

Examples:
  # Against a clearing DB (port-forwarded), embedding via Ollama on the host:
  tapes dev embed-spans \
    --postgres "postgres://user:pass@127.0.0.1:15432/tapes" \
    --embedding-provider ollama \
    --embedding-target http://localhost:11434 \
    --embedding-model embeddinggemma \
    --embedding-dimensions 768

  # Scope to one org:
  tapes dev embed-spans --postgres "$DSN" --org 00000000-0000-0000-0000-000000000000`

func newEmbedSpansCmd() *cobra.Command {
	cmder := &embedSpansCommander{
		flags: embedSpansFlags,
	}

	cmd := &cobra.Command{
		Use:   "embed-spans",
		Short: "Embed all eligible un-embedded spans (one-shot backfill)",
		Long:  embedSpansLongDesc,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagPostgres,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
			})

			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
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
			return cmder.run(cmd)
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	cmd.Flags().StringVar(&cmder.orgID, "org", "", "Only embed spans belonging to this org UUID (default: all orgs)")
	cmd.Flags().IntVar(&cmder.batchSize, "batch-size", spanembed.DefaultBatchSize, "Candidate page size")

	return cmd
}

func (c *embedSpansCommander) run(cmd *cobra.Command) error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))
	ctx := cmd.Context()

	if c.postgresDSN == "" {
		return errors.New("embed-spans requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}

	driver, err := postgres.NewDriver(ctx, c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	embedder, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: c.embeddingProvider,
		TargetURL:    c.embeddingTarget,
		Model:        c.embeddingModel,
		Dimensions:   c.embeddingDimensions,
		APIKey:       c.embeddingAPIKey,
	})
	if err != nil {
		return fmt.Errorf("could not create embedder: %w", err)
	}
	defer embedder.Close()

	store, err := spanembed.NewStore(driver.DB(), spanembed.StoreConfig{
		Dimensions: c.embeddingDimensions,
		OrgID:      c.orgID,
	}, c.logger)
	if err != nil {
		return fmt.Errorf("could not create span embedding store: %w", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("span embedding schema: %w", err)
	}

	pass, err := spanembed.NewPass(store, store, embedder, spanembed.PassConfig{
		Model:      c.embeddingModel,
		Dimensions: c.embeddingDimensions,
		BatchSize:  c.batchSize,
	}, c.logger)
	if err != nil {
		return fmt.Errorf("could not create embed pass: %w", err)
	}

	c.logger.Info("embedding spans",
		"embedding_provider", c.embeddingProvider,
		"embedding_target", c.embeddingTarget,
		"embedding_model", c.embeddingModel,
		"embedding_dimensions", c.embeddingDimensions,
	)

	report, err := pass.Run(ctx)
	if err != nil {
		return err
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))

	// Per-span failures are skip-and-continue inside the pass (the
	// worker-integrated pass retries next round), but a one-shot
	// backfill that embedded NOTHING while failing on every candidate
	// is a configuration error — an unreachable target or a model that
	// cannot embed — and must say so with its exit code.
	if report.Failed > 0 && report.Embedded == 0 {
		return fmt.Errorf("every eligible span failed to embed (%d failures): the embedding target/model is likely misconfigured", report.Failed)
	}
	return nil
}

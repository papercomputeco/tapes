package servecmder

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	deriveworker "github.com/papercomputeco/tapes/pkg/derive/worker"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/embedworker"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/proxy"
)

// Stack is the set of long-running services a tapes install is made of: the
// capture proxy, the API server, the ingest sidecar, and the derive and embed
// workers.
//
// It is a type rather than the body of one command so cassette registry setup
// stays separate from service construction and lifecycle.
type Stack struct {
	// Listen addresses for the three servers.
	ProxyListen  string
	APIListen    string
	IngestListen string

	// APIWebUI serves the minimal browser UI at the API server's root.
	APIWebUI bool

	// Upstream, ProviderType, and Project configure capture.
	Upstream     string
	ProviderType string
	Project      string

	// PostgresDSN is the capture and derived store. VectorStoreTarget is the
	// pgvector connection, which defaults to the same database.
	PostgresDSN       string
	VectorStoreTarget string

	// Embedding* configure the embedder shared by the API's search read path
	// and the embed worker. EmbedSpans turns the in-process worker on.
	EmbeddingProvider   string
	EmbeddingTarget     string
	EmbeddingModel      string
	EmbeddingDimensions uint
	EmbeddingAPIKey     string
	EmbedSpans          bool

	// CassetteSources are exact full OpenAPI document URLs transferred to the
	// API server's lifetime-owned runtime.
	CassetteSources []string

	// ContractVersions overrides the tapes contracts the API server serves.
	// Empty leaves the decision to the API server, which is the thing that
	// actually serves the contract and therefore owns the answer.
	ContractVersions []cassette.ContractVersion

	// Started, when set, is called once every service has been launched. Cassette
	// serving uses it to begin aggregation after the API server starts.
	//
	// It is called synchronously and must return — the stack does not begin
	// waiting on a service failure until it does. Anything long-running belongs
	// in a goroutine the hook launches.
	Started func(context.Context, *api.Server)

	// Logger is required.
	Logger *slog.Logger
}

// AddFlags registers the stack's flags on a command.
func (stack *Stack) AddFlags(cmd *cobra.Command, flags config.FlagSet) {
	config.AddStringFlag(cmd, flags, config.FlagProxyListen, &stack.ProxyListen)
	config.AddStringFlag(cmd, flags, config.FlagAPIListen, &stack.APIListen)
	config.AddBoolFlag(cmd, flags, config.FlagAPIWebUI, &stack.APIWebUI)
	config.AddStringFlag(cmd, flags, config.FlagIngestListen, &stack.IngestListen)
	config.AddStringFlag(cmd, flags, config.FlagUpstream, &stack.Upstream)
	config.AddStringFlag(cmd, flags, config.FlagProvider, &stack.ProviderType)
	config.AddStringFlag(cmd, flags, config.FlagProject, &stack.Project)
	config.AddStringFlag(cmd, flags, config.FlagPostgres, &stack.PostgresDSN)
	config.AddStringFlag(cmd, flags, config.FlagVectorStoreTgt, &stack.VectorStoreTarget)
	config.AddStringFlag(cmd, flags, config.FlagEmbeddingProv, &stack.EmbeddingProvider)
	config.AddStringFlag(cmd, flags, config.FlagEmbeddingTgt, &stack.EmbeddingTarget)
	config.AddStringFlag(cmd, flags, config.FlagEmbeddingModel, &stack.EmbeddingModel)
	config.AddUintFlag(cmd, flags, config.FlagEmbeddingDims, &stack.EmbeddingDimensions)
	config.AddStringSliceFlag(cmd, flags, config.FlagCassettes, &stack.CassetteSources)

	cmd.Flags().BoolVar(&stack.EmbedSpans, "embed-spans", true,
		"Embed main llm spans in the background so semantic search (tapes search) works; on by default, disable with --embed-spans=false")
}

// stackFlagKeys are the registered flags Resolve binds into viper's precedence
// chain, in the order they are declared above.
var stackFlagKeys = []string{
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
	config.FlagCassettes,
}

// Resolve fills the stack from flags, environment, and config file. Call it
// from a command's PreRunE, after AddFlags has registered the flags it binds.
func (stack *Stack) Resolve(cmd *cobra.Command, flags config.FlagSet) error {
	configDir, _ := cmd.Flags().GetString("config-dir")
	v, err := config.InitViper(configDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	config.BindRegisteredFlags(v, cmd, flags, stackFlagKeys)

	stack.CassetteSources, err = config.GetRegisteredStringSlice(v, cmd, flags, config.FlagCassettes)
	if err != nil {
		return fmt.Errorf("loading cassette sources: %w", err)
	}
	if err := config.ValidateCassetteSources(stack.CassetteSources); err != nil {
		return err
	}

	// Default pgvector to the primary Postgres DSN.
	if v.GetString("vector_store.target") == "" && v.GetString("storage.postgres_dsn") != "" {
		v.Set("vector_store.target", v.GetString("storage.postgres_dsn"))
	}

	stack.PostgresDSN = v.GetString("storage.postgres_dsn")
	stack.ProxyListen = v.GetString("proxy.listen")
	stack.APIListen = v.GetString("api.listen")
	stack.APIWebUI = v.GetBool("api.web_ui")
	stack.IngestListen = v.GetString("ingest.listen")
	stack.Upstream = v.GetString("proxy.upstream")
	stack.ProviderType = v.GetString("proxy.provider")
	stack.VectorStoreTarget = v.GetString("vector_store.target")

	embedding := config.ResolveEmbeddingConfigWithOptions(
		v.GetString("embedding.provider"),
		v.GetString("embedding.target"),
		v.GetString("embedding.model"),
		v.GetUint("embedding.dimensions"),
		config.ResolveEmbeddingConfigOptions{
			DimensionsSet: config.IsRegisteredFlagExplicitlySet(v, cmd, flags, config.FlagEmbeddingDims),
		},
	)
	stack.EmbeddingProvider = embedding.Provider
	stack.EmbeddingTarget = embedding.Target
	stack.EmbeddingModel = embedding.Model
	stack.EmbeddingDimensions = embedding.Dimensions
	stack.EmbeddingAPIKey, err = credentials.APIKeyForProvider(embedding.Provider, configDir)
	if err != nil {
		return fmt.Errorf("could not load embedding credentials: %w", err)
	}

	stack.Project = v.GetString("proxy.project")
	if stack.Project == "" {
		stack.Project = git.RepoName(cmd.Context())
	}

	return nil
}

// Run starts every service and blocks until one fails or ctx is cancelled.
//
// The caller owns the signal handling: cancelling ctx stops the workers, and
// the servers are torn down by the deferred closes on the way out.
func (stack *Stack) Run(ctx context.Context) error {
	driver, err := postgres.NewDriver(ctx, stack.PostgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	// The embedder serves the API's search read path and the in-process
	// embed worker below. Capture-time embedding is retired: the embed
	// worker family is the single writer of embeddings.
	embedder, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: stack.EmbeddingProvider,
		TargetURL:    stack.EmbeddingTarget,
		Model:        stack.EmbeddingModel,
		Dimensions:   stack.EmbeddingDimensions,
		APIKey:       stack.EmbeddingAPIKey,
	})
	if err != nil {
		return fmt.Errorf("creating embedder: %w", err)
	}
	defer embedder.Close()

	spanSearcher, err := spanembed.NewStore(driver.DB(), spanembed.StoreConfig{
		Dimensions: stack.EmbeddingDimensions,
	}, stack.Logger)
	if err != nil {
		return fmt.Errorf("could not create span embedding store: %w", err)
	}

	stack.Logger.Info("vector search enabled",
		"vector_store_target", config.RedactDSN(stack.VectorStoreTarget),
		"embedding_provider", stack.EmbeddingProvider,
		"embedding_target", stack.EmbeddingTarget,
		"embedding_model", stack.EmbeddingModel,
	)

	// These constructors own worker pools whose lifecycle is closed explicitly
	// below; neither constructor accepts an inherited context.
	p, err := proxy.New(proxy.Config{ //nolint:contextcheck
		ListenAddr:   stack.ProxyListen,
		UpstreamURL:  stack.Upstream,
		ProviderType: stack.ProviderType,
		Project:      stack.Project,
	}, driver, stack.Logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer p.Close()

	stack.Logger.Info("starting proxy",
		"proxy_addr", stack.ProxyListen,
		"upstream", stack.Upstream,
		"provider", stack.ProviderType,
	)

	apiServer, err := api.NewServer(api.Config{ //nolint:contextcheck // Fiber owns request contexts.
		ListenAddr:       stack.APIListen,
		Embedder:         embedder,
		SpanSearcher:     spanSearcher,
		EnableWebUI:      stack.APIWebUI,
		ContractVersions: stack.ContractVersions,
	}, driver, stack.Logger)
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}
	apiServer.SetCassetteSources(stack.CassetteSources)

	stack.Logger.Info("starting api server", "api_addr", stack.APIListen)

	// contextcheck is right that this chain drops `ctx`: the ingest server's
	// capture workers persist each turn under context.Background()
	// (proxy/worker/pool.go, processJob), deliberately, so that a shutdown or
	// a cancelled request cannot abandon a captured turn mid-persist. Capture
	// durability outranks prompt cancellation here.
	//
	// nolintlint rides along because contextcheck's cross-package analysis
	// resolves this chain only some of the time: on the runs where it does
	// not, the directive reads as unused and reds the lint gate on a tree
	// that has not changed. Suppressing both keeps the gate deterministic.
	ingestServer, err := ingest.New(ingest.Config{ //nolint:contextcheck,nolintlint
		ListenAddr: stack.IngestListen,
		Project:    stack.Project,
	}, driver, stack.Logger)
	if err != nil {
		return fmt.Errorf("creating ingest server: %w", err)
	}
	defer ingestServer.Close()

	stack.Logger.Info("starting ingest server", "ingest_addr", stack.IngestListen)

	// Local single-process convenience: run the derive worker in-process
	// so a captured turn projects into the sessions/traces/spans surface
	// the deck and API read — no separate `tapes serve derive-worker`
	// dance for local use. The short debounce keeps the local
	// capture→deck loop snappy; production runs the worker as its own
	// process (`tapes serve derive-worker`) with the full-size default
	// debounce and its own memory budget.
	deriveCfg := deriveworker.Config{
		Project:  stack.Project,
		Debounce: 2 * time.Second,
	}
	// By default the stack also embeds main llm spans, so `tapes search` works
	// out of the box (disable with --embed-spans=false). Embedding is never a
	// step of the derive loop — it runs as its own embed-worker loop (see
	// pkg/embedworker) so a slow or down embedding backend cannot stall
	// derivation. Locally that loop runs in-process on a short interval to
	// keep the capture→search loop snappy; production runs it as its own
	// process (`tapes serve embed-worker`) with the full-size default
	// interval. It reuses the embedder and span store already built for the
	// API's search read path. Embedding degrades gracefully: setup or backend
	// failures disable search but never fail the stack, and a failing pass
	// backs off between retries instead of hammering a dead backend.
	var embedW *embedworker.Worker
	if stack.EmbedSpans {
		if err := spanSearcher.EnsureSchema(ctx); err != nil {
			stack.Logger.Warn("span embedding disabled: could not prepare the embedding schema — tapes search will be unavailable", "error", err)
		} else if pass, perr := spanembed.NewPass(spanSearcher, spanSearcher, embedder, spanembed.PassConfig{
			Model:      stack.EmbeddingModel,
			Dimensions: stack.EmbeddingDimensions,
		}, stack.Logger); perr != nil {
			stack.Logger.Warn("span embedding disabled: could not create the embed pass — tapes search will be unavailable", "error", perr)
		} else {
			embedW = embedworker.NewWorker(embedworker.Config{
				Interval: 10 * time.Second,
			}, pass, stack.Logger)
			stack.Logger.Info("span embedding enabled (in-process)", "model", stack.EmbeddingModel)
		}
	}
	deriveW := deriveworker.NewWorker(deriveCfg, driver, stack.Logger)
	stack.Logger.Info("starting derive worker (in-process)", "debounce", deriveCfg.Debounce)

	errChan := make(chan error, 5)

	go func() {
		if err := p.Run(); err != nil {
			errChan <- fmt.Errorf("proxy error: %w", err)
		}
	}()

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

	go func() {
		if err := deriveW.Run(ctx); err != nil {
			errChan <- fmt.Errorf("derive worker error: %w", err)
		}
	}()

	if embedW != nil {
		go func() {
			if err := embedW.Run(ctx); err != nil {
				errChan <- fmt.Errorf("embed worker error: %w", err)
			}
		}()
	}

	if stack.Started != nil {
		stack.Started(ctx, apiServer)
	}

	// Wait for interrupt signal or a fatal error from any service.
	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		stack.Logger.Info("received signal, shutting down")

		return nil
	}
}

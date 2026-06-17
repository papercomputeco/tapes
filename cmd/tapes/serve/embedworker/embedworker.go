// Package embedworkercmder provides the embed-worker cobra command.
package embedworkercmder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/embedworker"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/spanembed"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type embedWorkerCommander struct {
	flags config.FlagSet

	debug       bool
	postgresDSN string

	interval      string
	metricsListen string
	waitForDB     bool
	batchSize     int
	orgID         string

	embeddingProvider   string
	embeddingTarget     string
	embeddingModel      string
	embeddingDimensions uint
	embeddingAPIKey     string

	logger *slog.Logger
}

// embedWorkerFlags defines the flags for the embed-worker subcommand.
var embedWorkerFlags = config.FlagSet{
	config.FlagPostgres:                 {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagEmbedWorkerInterval:      {Name: "interval", ViperKey: "embed_worker.interval", Description: "How often to run a full span embed pass (Go duration, default 1m)"},
	config.FlagEmbedWorkerMetricsListen: {Name: "metrics-listen", ViperKey: "embed_worker.metrics_listen", Description: "Address to serve /metrics, /healthz (liveness), /readyz (readiness), and /ping on (empty disables)"},
	config.FlagEmbedWorkerWaitForDB:     {Name: "wait-for-db", ViperKey: "embed_worker.wait_for_db", Description: "Retry an unreachable Postgres at startup with backoff instead of exiting (for orchestrated environments; default: fail fast)"},
	config.FlagEmbedWorkerBatchSize:     {Name: "batch-size", ViperKey: "embed_worker.batch_size", Description: "Candidate page size; bounds peak memory per pass (0 uses the built-in default)"},
	config.FlagEmbedWorkerOrg:           {Name: "org", ViperKey: "embed_worker.org", Description: "Only embed spans belonging to this org UUID (default: all orgs)"},
	config.FlagEmbeddingProv:            {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama, openai)"},
	config.FlagEmbeddingTgt:             {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel:           {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., embeddinggemma, text-embedding-3-large)"},
	config.FlagEmbeddingDims:            {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality (must match the model's output)"},
}

const embedWorkerLongDesc string = `Run the embed worker.

The embed worker turns the derived main-conversation llm spans into
vector embeddings for semantic search ("tapes search"). It runs the
bounded span embed pass on its own interval (default 1m) and once at
startup to clear the standing backlog.

It runs as its OWN process, split out of the derive worker on purpose:
embedding calls an external provider whose latency and availability are
outside Tapes' control, so it must never share a memory budget or a
failure domain with derivation. Derivation keeps projecting raw turns
into the read model no matter what the embedding backend does. Run extra
replicas to scale; the pass is idempotent (keyed by span identity and
gated by a content hash), so concurrent or repeated runs only cost
redundant reads.

A per-span embed failure (the provider rejected the input or was
unreachable for that span) is counted and logged; the span stays
un-embedded and the next pass retries it — a single bad span never
aborts the pass.

Configure the backend with the --embedding-* flags; the model and
dimensions must be an explicit, matching pair — the vector table is
created with exactly the configured dimensions and startup fails fast
when an existing table disagrees.

Operations: an unreachable database fails startup fast unless
--wait-for-db is set; pass failures back off exponentially (capped at
5m) and recover on their own. --metrics-listen serves Prometheus
/metrics plus /healthz (liveness) and /readyz (readiness) for
orchestrators. SIGTERM/SIGINT drains the in-flight pass (bounded at 30s)
before exiting; a second signal kills immediately.

--batch-size bounds how many candidate spans are held in memory at once;
lower it to cap the worker's peak memory, raise it to cut round trips.`

const embedWorkerShortDesc string = "Run the Tapes embed worker"

// NewEmbedWorkerCmd creates the cobra command for the embed worker.
func NewEmbedWorkerCmd() *cobra.Command {
	cmder := &embedWorkerCommander{
		flags: embedWorkerFlags,
	}

	cmd := &cobra.Command{
		Use:   "embed-worker",
		Short: embedWorkerShortDesc,
		Long:  embedWorkerLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagPostgres,
				config.FlagEmbedWorkerInterval,
				config.FlagEmbedWorkerMetricsListen,
				config.FlagEmbedWorkerWaitForDB,
				config.FlagEmbedWorkerBatchSize,
				config.FlagEmbedWorkerOrg,
				config.FlagEmbeddingProv,
				config.FlagEmbeddingTgt,
				config.FlagEmbeddingModel,
				config.FlagEmbeddingDims,
			})

			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.interval = v.GetString("embed_worker.interval")
			cmder.metricsListen = v.GetString("embed_worker.metrics_listen")
			cmder.waitForDB = v.GetBool("embed_worker.wait_for_db")
			cmder.batchSize = v.GetInt("embed_worker.batch_size")
			cmder.orgID = v.GetString("embed_worker.org")

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

			telemetry.FromContext(cmd.Context()).CaptureServerStarted("embed-worker")
			return cmder.run(cmd.Context())
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbedWorkerInterval, &cmder.interval)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbedWorkerMetricsListen, &cmder.metricsListen)
	config.AddBoolFlag(cmd, cmder.flags, config.FlagEmbedWorkerWaitForDB, &cmder.waitForDB)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingProv, &cmder.embeddingProvider)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingTgt, &cmder.embeddingTarget)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbeddingModel, &cmder.embeddingModel)
	config.AddUintFlag(cmd, cmder.flags, config.FlagEmbeddingDims, &cmder.embeddingDimensions)
	config.AddIntFlag(cmd, cmder.flags, config.FlagEmbedWorkerBatchSize, &cmder.batchSize)
	config.AddStringFlag(cmd, cmder.flags, config.FlagEmbedWorkerOrg, &cmder.orgID)

	return cmd
}

func (c *embedWorkerCommander) run(ctx context.Context) error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	if c.postgresDSN == "" {
		return errors.New("embed worker requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}

	interval, err := parseDurationFlag("interval", c.interval)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		// The first signal starts the graceful drain; restoring default
		// handling makes a second signal kill immediately.
		<-ctx.Done()
		stop()
	}()

	// The metrics/health listener starts BEFORE the database connect so
	// /healthz answers while --wait-for-db retries. /readyz flips to 200
	// only once the worker exists and its database is reachable.
	metrics := embedworker.NewMetrics()
	var readyWorker atomic.Pointer[embedworker.Worker]

	if c.metricsListen != "" {
		srv := c.metricsServer(&readyWorker, metrics)
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				c.logger.Error("metrics listener", "error", err)
			}
		}()
		defer func() { _ = srv.Close() }()
		c.logger.Info("metrics listener started", "listen", c.metricsListen)
	}

	driver, err := c.connect(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return err
	}
	defer driver.Close()

	pass, cleanup, err := c.buildPass(ctx, driver)
	if err != nil {
		return err
	}
	defer cleanup()

	w := embedworker.NewWorker(embedworker.Config{
		Interval: interval,
		Metrics:  metrics,
		Ready: func(ctx context.Context) error {
			return driver.DB().Ping(ctx)
		},
	}, pass, c.logger)
	readyWorker.Store(w)

	return w.Run(ctx)
}

// metricsServer builds the /metrics + health-probe listener.
func (c *embedWorkerCommander) metricsServer(readyWorker *atomic.Pointer[embedworker.Worker], metrics *embedworker.Metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/ping", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("pong"))
	})
	// Liveness: the process is up. Never depends on the database — a DB
	// outage must not get the pod killed.
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})
	// Readiness: the database is reachable. 503 until the first connect.
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, r *http.Request) {
		w := readyWorker.Load()
		if w == nil {
			http.Error(rw, "initializing: database not connected", http.StatusServiceUnavailable)
			return
		}
		probeCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := w.Ready(probeCtx); err != nil {
			http.Error(rw, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})
	return &http.Server{
		Addr:              c.metricsListen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// buildPass wires the span embed pass onto the worker's Postgres pool.
// Schema is ensured at startup so a model/dimensions misconfiguration
// fails the process immediately and visibly.
func (c *embedWorkerCommander) buildPass(ctx context.Context, driver *postgres.Driver) (*spanembed.Pass, func(), error) {
	embedder, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: c.embeddingProvider,
		TargetURL:    c.embeddingTarget,
		Model:        c.embeddingModel,
		Dimensions:   c.embeddingDimensions,
		APIKey:       c.embeddingAPIKey,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("could not create embedder: %w", err)
	}

	store, err := spanembed.NewStore(driver.DB(), spanembed.StoreConfig{
		Dimensions: c.embeddingDimensions,
		OrgID:      c.orgID,
	}, c.logger)
	if err != nil {
		_ = embedder.Close()
		return nil, nil, fmt.Errorf("could not create span embedding store: %w", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		_ = embedder.Close()
		return nil, nil, fmt.Errorf("span embedding schema: %w", err)
	}

	pass, err := spanembed.NewPass(store, store, embedder, spanembed.PassConfig{
		Model:      c.embeddingModel,
		Dimensions: c.embeddingDimensions,
		BatchSize:  c.batchSize,
	}, c.logger)
	if err != nil {
		_ = embedder.Close()
		return nil, nil, fmt.Errorf("could not create span embed pass: %w", err)
	}

	c.logger.Info("span embedding enabled",
		"embedding_provider", c.embeddingProvider,
		"embedding_target", c.embeddingTarget,
		"embedding_model", c.embeddingModel,
		"embedding_dimensions", c.embeddingDimensions,
		"batch_size", c.batchSize,
	)
	return pass, func() { _ = embedder.Close() }, nil
}

// Startup connection bounds mirror the derive worker: a small pool with
// a bounded connect timeout beats pgx's NumCPU-based default for a
// single-loop worker.
const (
	connectTimeout    = 10 * time.Second
	maxPoolConns      = 4
	maxConnectBackoff = 30 * time.Second
)

// connect opens the Postgres driver. By default an unreachable database
// is a startup error; with --wait-for-db the worker retries with
// exponential backoff until the database appears or ctx is canceled.
func (c *embedWorkerCommander) connect(ctx context.Context) (*postgres.Driver, error) {
	opts := []postgres.PoolOption{
		postgres.WithConnectTimeout(connectTimeout),
		postgres.WithMaxConns(maxPoolConns),
	}

	driver, err := postgres.NewDriver(ctx, c.postgresDSN, opts...)
	if err == nil {
		return driver, nil
	}
	if !c.waitForDB {
		return nil, fmt.Errorf("postgres unreachable at startup (pass --wait-for-db to retry instead): %w", err)
	}

	backoff := time.Second
	for attempt := 1; ; attempt++ {
		c.logger.Warn("postgres unreachable, retrying",
			"attempt", attempt,
			"retry_in", backoff,
			"error", err.Error(),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		driver, err = postgres.NewDriver(ctx, c.postgresDSN, opts...)
		if err == nil {
			c.logger.Info("postgres reachable", "attempts", attempt+1)
			return driver, nil
		}
		backoff = min(backoff*2, maxConnectBackoff)
	}
}

// parseDurationFlag parses an optional Go-duration flag value; empty
// means "use the worker default".
func parseDurationFlag(name, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s %q: %w", name, value, err)
	}
	return d, nil
}

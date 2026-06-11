// Package deriveworkercmder provides the derive-worker cobra command.
package deriveworkercmder

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
	"github.com/papercomputeco/tapes/pkg/derive/worker"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type deriveWorkerCommander struct {
	flags config.FlagSet

	debug       bool
	postgresDSN string
	project     string

	pollInterval  string
	debounce      string
	sweepInterval string
	sweepWindow   string
	metricsListen string
	waitForDB     bool

	logger *slog.Logger
}

// deriveWorkerFlags defines the flags for the derive-worker subcommand.
var deriveWorkerFlags = config.FlagSet{
	config.FlagPostgres:                  {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:                   {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
	config.FlagDeriveWorkerPoll:          {Name: "poll-interval", ViperKey: "derive_worker.poll_interval", Description: "How often to poll the dirty-session queue (Go duration, default 5s)"},
	config.FlagDeriveWorkerDebounce:      {Name: "debounce", ViperKey: "derive_worker.debounce", Description: "How long a session's dirty mark must be quiet before deriving (Go duration, default 20s)"},
	config.FlagDeriveWorkerSweep:         {Name: "sweep-interval", ViperKey: "derive_worker.sweep_interval", Description: "Backstop sweep cadence re-enqueuing recently-active raw-layer sessions (Go duration, default 1h)"},
	config.FlagDeriveWorkerSweepWindow:   {Name: "sweep-window", ViperKey: "derive_worker.sweep_window", Description: "Only sweep sessions with raw activity in this window (Go duration, default 24h; negative sweeps all history)"},
	config.FlagDeriveWorkerMetricsListen: {Name: "metrics-listen", ViperKey: "derive_worker.metrics_listen", Description: "Address to serve /metrics, /healthz (liveness), /readyz (readiness), and /ping on (empty disables)"},
	config.FlagDeriveWorkerWaitForDB:     {Name: "wait-for-db", ViperKey: "derive_worker.wait_for_db", Description: "Retry an unreachable Postgres at startup with backoff instead of exiting (for orchestrated environments; default: fail fast)"},
}

const deriveWorkerLongDesc string = `Run the derive worker.

The derive worker turns the immutable raw-turn layer into the derived node
layer continuously: ingest marks a session dirty whenever a raw turn (wire or
transcript) lands for it, and the worker polls that queue, waits for the burst
to settle (debounce), then re-derives ONE session at a time under a per-session
Postgres advisory lock — so concurrent workers never double-derive. Run extra
replicas to scale; the lock makes them safe.

A slow backstop sweep (default hourly, plus once at startup) re-enqueues
sessions with raw activity inside --sweep-window (default 24h), catching any
lost dirty mark without stampeding the queue on restart. Pass a negative
window to sweep all of history — the full re-derive escape hatch after a
deriver fix.

Derivation is idempotent (re-running an unchanged session prunes 0 nodes), so
everything here is safely at-least-once. The admin endpoints
POST /v1/admin/derive/run and /verify remain available as escape hatches.

Operations: an unreachable database fails startup fast unless --wait-for-db
is set; poll failures back off exponentially (capped at 30s) and recover on
their own. --metrics-listen serves Prometheus /metrics plus /healthz
(liveness) and /readyz (readiness) for orchestrators. SIGTERM/SIGINT drains
the in-flight derive (bounded at 30s) before exiting; a second signal kills
immediately.

Run this as its own process with its own memory budget — never inside the API
server.`

const deriveWorkerShortDesc string = "Run the Tapes derive worker"

// NewDeriveWorkerCmd creates the cobra command for the derive worker.
func NewDeriveWorkerCmd() *cobra.Command {
	cmder := &deriveWorkerCommander{
		flags: deriveWorkerFlags,
	}

	cmd := &cobra.Command{
		Use:   "derive-worker",
		Short: deriveWorkerShortDesc,
		Long:  deriveWorkerLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagPostgres,
				config.FlagProject,
				config.FlagDeriveWorkerPoll,
				config.FlagDeriveWorkerDebounce,
				config.FlagDeriveWorkerSweep,
				config.FlagDeriveWorkerSweepWindow,
				config.FlagDeriveWorkerMetricsListen,
				config.FlagDeriveWorkerWaitForDB,
			})

			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.project = v.GetString("proxy.project")
			cmder.pollInterval = v.GetString("derive_worker.poll_interval")
			cmder.debounce = v.GetString("derive_worker.debounce")
			cmder.sweepInterval = v.GetString("derive_worker.sweep_interval")
			cmder.sweepWindow = v.GetString("derive_worker.sweep_window")
			cmder.metricsListen = v.GetString("derive_worker.metrics_listen")
			cmder.waitForDB = v.GetBool("derive_worker.wait_for_db")

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

			telemetry.FromContext(cmd.Context()).CaptureServerStarted("derive-worker")
			return cmder.run(cmd.Context())
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)
	config.AddStringFlag(cmd, cmder.flags, config.FlagDeriveWorkerPoll, &cmder.pollInterval)
	config.AddStringFlag(cmd, cmder.flags, config.FlagDeriveWorkerDebounce, &cmder.debounce)
	config.AddStringFlag(cmd, cmder.flags, config.FlagDeriveWorkerSweep, &cmder.sweepInterval)
	config.AddStringFlag(cmd, cmder.flags, config.FlagDeriveWorkerSweepWindow, &cmder.sweepWindow)
	config.AddStringFlag(cmd, cmder.flags, config.FlagDeriveWorkerMetricsListen, &cmder.metricsListen)
	config.AddBoolFlag(cmd, cmder.flags, config.FlagDeriveWorkerWaitForDB, &cmder.waitForDB)

	return cmd
}

func (c *deriveWorkerCommander) run(ctx context.Context) error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	if c.postgresDSN == "" {
		return errors.New("derive worker requires a postgres DSN (--postgres or storage.postgres_dsn)")
	}

	cfg := worker.Config{Project: c.project}
	var err error
	if cfg.PollInterval, err = parseDurationFlag("poll-interval", c.pollInterval); err != nil {
		return err
	}
	if cfg.Debounce, err = parseDurationFlag("debounce", c.debounce); err != nil {
		return err
	}
	if cfg.SweepInterval, err = parseDurationFlag("sweep-interval", c.sweepInterval); err != nil {
		return err
	}
	if cfg.SweepWindow, err = parseDurationFlag("sweep-window", c.sweepWindow); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		// The first signal starts the graceful drain (the worker
		// finishes its in-flight derive, bounded by its drain timeout).
		// Restoring default signal handling here makes a second signal
		// kill the process immediately — the operator's escape hatch.
		<-ctx.Done()
		stop()
	}()

	// The metrics/health listener starts BEFORE the database connect so
	// /healthz answers (and /metrics scrapes don't 404) while
	// --wait-for-db retries an unreachable store. /readyz flips to 200
	// only once the worker exists and can poll its queue.
	metrics := worker.NewMetrics()
	cfg.Metrics = metrics
	var readyWorker atomic.Pointer[worker.Worker]

	if c.metricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/ping", func(rw http.ResponseWriter, _ *http.Request) {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("pong"))
		})
		// Liveness: the process is up and serving. Never depends on
		// the database — a DB outage must not get the pod killed.
		mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("ok"))
		})
		// Readiness: the store answers and the dirty queue is
		// pollable. 503 until the first successful connect.
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
		srv := &http.Server{
			Addr:              c.metricsListen,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
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
		// A shutdown signal during the startup wait is a clean exit,
		// same as a signal during the run loop — not an error.
		return nil
	}
	if err != nil {
		return err
	}
	defer driver.Close()

	w := worker.NewWorker(cfg, driver, c.logger)
	readyWorker.Store(w)

	return w.Run(ctx)
}

// Startup connection bounds. The worker derives one session at a time,
// so a small pool (poll conn + pinned advisory-lock conn + derive conn
// + headroom) beats pgx's NumCPU-based default; the connect timeout
// keeps an unreachable host from hanging startup for the OS TCP
// timeout.
const (
	connectTimeout    = 10 * time.Second
	maxPoolConns      = 4
	maxConnectBackoff = 30 * time.Second
)

// connect opens the Postgres driver. By default an unreachable
// database is a startup error — fail fast and clearly so a bad DSN
// surfaces immediately. With --wait-for-db the worker instead retries
// with exponential backoff until the database appears or the context
// is canceled (the right behavior under an orchestrator that starts
// the worker and the database concurrently).
func (c *deriveWorkerCommander) connect(ctx context.Context) (*postgres.Driver, error) {
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

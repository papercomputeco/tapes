package deckcmder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/local"
	"github.com/papercomputeco/tapes/pkg/start"
)

// bootstrapConfig captures everything bootstrapAPI needs to resolve an API
// target without reading flags directly. Tests override these.
type bootstrapConfig struct {
	apiTarget         string // resolved value of --api-target / client.api_target
	apiTargetIsCustom bool   // true if user explicitly set api-target
	postgresDSN       string // resolved value of --postgres / storage.postgres_dsn
	configDir         string // forwarded to start.SpawnOptions
	debug             bool
	out               io.Writer
	hasDocker         func() bool // defaults to local.HasDocker; overridden in tests
}

// bootstrapAPI resolves the URL of a tapes API server, bringing up local
// Postgres + a `tapes start` daemon when nothing is already serving.
//
// The returned cleanup function unregisters the deck session from the daemon's
// agent list and is safe to call multiple times. It is non-nil even on the
// happy paths that don't require cleanup; callers should always defer it.
func bootstrapAPI(ctx context.Context, cfg bootstrapConfig) (string, func(), error) {
	if cfg.out == nil {
		cfg.out = os.Stdout
	}
	if cfg.hasDocker == nil {
		cfg.hasDocker = local.HasDocker
	}

	if cfg.apiTargetIsCustom {
		return normalizeAPITarget(cfg.apiTarget), func() {}, nil
	}

	manager, err := start.NewManager(cfg.configDir)
	if err != nil {
		return "", nil, fmt.Errorf("locating tapes start manager: %w", err)
	}

	state, err := start.LoadHealthyMatching(ctx, manager, cfg.postgresDSN, os.Stderr)
	if err != nil {
		var mismatch *start.DSNMismatchError
		if errors.As(err, &mismatch) {
			return "", nil, errors.New(strings.Join([]string{
				cliui.FailMark + " Running tapes daemon is bound to a different Postgres",
				"",
				"  running:   " + mismatch.Running,
				"  requested: " + mismatch.Requested,
				"",
				"To switch, stop the running daemon first:",
				"  pkill -f 'tapes start'",
				"",
				"Or omit --postgres / unset storage.postgres_dsn to attach to the running daemon.",
			}, "\n"))
		}
		return "", nil, err
	}
	if state != nil {
		return finalizeWithDaemon(ctx, manager, state.APIURL)
	}

	dsn, err := ensurePostgres(ctx, cfg)
	if err != nil {
		return "", nil, err
	}
	cfg.postgresDSN = dsn

	state, err = spawnAndWait(ctx, manager, cfg)
	if err != nil {
		return "", nil, err
	}

	return finalizeWithDaemon(ctx, manager, state.APIURL)
}

// ensurePostgres checks the configured DSN and runs `pkg/local.Up` (Postgres
// only) if nothing answers. Returns the DSN that was confirmed working so the
// caller can forward it to the daemon spawn. Returns a styled actionable
// error when Docker is unavailable.
func ensurePostgres(ctx context.Context, cfg bootstrapConfig) (string, error) {
	dsn := cfg.postgresDSN
	if dsn == "" {
		dsn = local.PostgresDSN(local.DefaultPostgresPort)
	}

	if postgresReachable(ctx, dsn) {
		return dsn, nil
	}

	if !cfg.hasDocker() {
		return "", errors.New(strings.Join([]string{
			cliui.FailMark + " Postgres is not reachable and Docker is not installed",
			"",
			"Install Docker (https://docs.docker.com/get-docker/) and re-run:",
			"  tapes deck",
			"",
			"Or point at an existing Postgres:",
			"  tapes config set storage.postgres_dsn <dsn>",
		}, "\n"))
	}

	fmt.Fprintf(cfg.out, "%s\n", cliui.HeaderStyle.Render("Bootstrapping local Postgres"))
	if err := local.Up(ctx, local.Options{
		ConfigDir:  cfg.configDir,
		SkipOllama: true,
		Out:        cfg.out,
	}); err != nil {
		return "", err
	}
	return dsn, nil
}

// postgresReachable opens a small pgxpool against dsn and pings it. Returns
// false on any error (unreachable, auth failure, missing db, ...). A 1s
// timeout keeps cold starts snappy when the host port is closed.
func postgresReachable(ctx context.Context, dsn string) bool {
	if strings.TrimSpace(dsn) == "" {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	pool, err := pgxpool.New(probeCtx, dsn)
	if err != nil {
		return false
	}
	defer pool.Close()
	return pool.Ping(probeCtx) == nil
}

// spawnAndWait launches `tapes start --daemon` and polls until /ping responds
// or the spawn fails.
func spawnAndWait(ctx context.Context, manager *start.Manager, cfg bootstrapConfig) (*start.State, error) {
	fmt.Fprintf(cfg.out, "%s\n", cliui.HeaderStyle.Render("Starting tapes API daemon"))
	state, _, err := start.SpawnAndWait(ctx, manager,
		start.SpawnOptions{
			ConfigDir:   cfg.configDir,
			Debug:       cfg.debug,
			PostgresDSN: cfg.postgresDSN,
		},
		start.WaitOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("spawning tapes daemon: %w", err)
	}
	return state, nil
}

// finalizeWithDaemon registers the current process as a deck session and
// returns the apiURL plus an unregister cleanup. Registration failures don't
// block deck from opening — the daemon still works fine without the session
// record, the only consequence is the idle-monitor may shut down underneath
// if no other agents are running.
func finalizeWithDaemon(_ context.Context, manager *start.Manager, apiURL string) (string, func(), error) {
	pid := os.Getpid()
	if err := start.RegisterAgent(manager, "deck", pid); err != nil {
		// Non-fatal: log to stderr and continue.
		fmt.Fprintf(os.Stderr, "warning: could not register deck session: %v\n", err)
		return apiURL, func() {}, nil
	}
	cleanup := func() {
		_ = start.UnregisterAgent(manager, pid)
	}
	return apiURL, cleanup, nil
}

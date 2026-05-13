package deckcmder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/local"
	"github.com/papercomputeco/tapes/pkg/start"
)

// bootstrapConfig captures everything bootstrapAPI needs to resolve an API
// target without reading flags directly. Tests override the function fields.
type bootstrapConfig struct {
	apiTarget         string // resolved value of --api-target / client.api_target
	apiTargetIsCustom bool   // true if user explicitly set api-target
	postgresDSN       string // resolved value of --postgres / storage.postgres_dsn
	configDir         string // forwarded to start.SpawnOptions
	debug             bool
	out               io.Writer
	hasDocker         func() bool                              // defaults to local.HasDocker
	localUp           func(ctx context.Context, configDir string, out io.Writer) error
	spawn             func(ctx context.Context, manager *start.Manager, opts start.SpawnOptions) (*start.State, error)
}

// bootstrapAPI resolves the URL of a tapes API server.
//
// The deck client never opens a Postgres connection itself. Instead it:
//
//  1. honours an explicit --api-target / TAPES_CLIENT_API_TARGET,
//  2. attaches to a healthy `tapes start` daemon if one is already running,
//  3. asks the daemon to start; if the daemon fails because Postgres is
//     unreachable, falls back to `tapes local up` (Docker container) and
//     retries the spawn once.
//
// The returned cleanup function unregisters the deck session from the
// daemon's agent list and is safe to call multiple times. It is non-nil even
// on the happy paths that don't require cleanup; callers should always defer
// it.
func bootstrapAPI(ctx context.Context, cfg bootstrapConfig) (string, func(), error) {
	if cfg.out == nil {
		cfg.out = os.Stdout
	}
	if cfg.hasDocker == nil {
		cfg.hasDocker = local.HasDocker
	}
	if cfg.localUp == nil {
		cfg.localUp = defaultLocalUp
	}
	if cfg.spawn == nil {
		cfg.spawn = defaultSpawn
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

	// No existing daemon. Clear any sentinel from a prior failed spawn so we
	// don't react to stale information from a previous run.
	_ = manager.ClearSpawnError()

	state, err = trySpawn(ctx, manager, cfg)
	if err == nil {
		return finalizeWithDaemon(ctx, manager, state.APIURL)
	}

	spawnErr, _ := manager.LoadSpawnError()
	if spawnErr == nil || spawnErr.Reason != start.ReasonPostgresUnreachable {
		// Generic spawn failure — surface the underlying error.
		return "", nil, err
	}

	dsn := spawnErr.DSN
	if dsn == "" {
		dsn = cfg.postgresDSN
	}
	if dsn == "" {
		dsn = local.PostgresDSN(local.DefaultPostgresPort)
	}

	if !local.IsLocalDefaultHost(dsn) {
		return "", nil, errors.New(strings.Join([]string{
			cliui.FailMark + " Configured Postgres is unreachable",
			"",
			"  storage.postgres_dsn: " + dsn,
			"",
			"Bring it up, or clear the configured DSN to use a local container:",
			"  tapes config set storage.postgres_dsn \"\"",
		}, "\n"))
	}

	if !cfg.hasDocker() {
		return "", nil, errors.New(strings.Join([]string{
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
	if err := cfg.localUp(ctx, cfg.configDir, cfg.out); err != nil {
		return "", nil, err
	}

	_ = manager.ClearSpawnError()
	cfg.postgresDSN = dsn
	state, err = trySpawn(ctx, manager, cfg)
	if err != nil {
		return "", nil, err
	}
	return finalizeWithDaemon(ctx, manager, state.APIURL)
}

// trySpawn launches `tapes start --daemon` and waits for it. The daemon
// records any Postgres failure to a SpawnError sentinel which the caller
// inspects.
func trySpawn(ctx context.Context, manager *start.Manager, cfg bootstrapConfig) (*start.State, error) {
	fmt.Fprintf(cfg.out, "%s\n", cliui.HeaderStyle.Render("Starting tapes API daemon"))
	state, err := cfg.spawn(ctx, manager, start.SpawnOptions{
		ConfigDir:   cfg.configDir,
		Debug:       cfg.debug,
		PostgresDSN: cfg.postgresDSN,
	})
	if err != nil {
		return nil, fmt.Errorf("spawning tapes daemon: %w", err)
	}
	return state, nil
}

func defaultSpawn(ctx context.Context, manager *start.Manager, opts start.SpawnOptions) (*start.State, error) {
	state, _, err := start.SpawnAndWait(ctx, manager, opts, start.WaitOptions{})
	return state, err
}

func defaultLocalUp(ctx context.Context, configDir string, out io.Writer) error {
	return local.Up(ctx, local.Options{
		ConfigDir:  configDir,
		SkipOllama: true,
		Out:        out,
	})
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

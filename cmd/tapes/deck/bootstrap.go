package deckcmder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/start"
)

// bootstrapConfig captures everything bootstrapAPI needs to resolve an API
// target without reading flags directly. Tests override the function fields.
type bootstrapConfig struct {
	apiTarget         string // resolved value of --api-target / client.api_target
	apiTargetIsCustom bool   // true if user explicitly set api-target
	postgresDSN       string // forwarded to LoadHealthyMatching to detect DSN mismatches
	configDir         string
	out               io.Writer
	apiReachable      func(ctx context.Context, apiURL string) bool // overridable for tests
}

// bootstrapAPI resolves the URL of a tapes API server.
//
// Deck is a pure API client. It never opens a Postgres connection and never
// orchestrates Docker. Resolution order:
//
//  1. Explicit --api-target / TAPES_CLIENT_API_TARGET.
//  2. A host-side `tapes start` daemon (e.g. left running by `tapes start
//     claude`). When present, deck attaches and registers a session so the
//     daemon's idle-monitor doesn't tear it down underneath us.
//  3. The configured `client.api_target`, if a tapes API answers /ping
//     there. This is the URL `tapes local up` writes when it brings up the
//     bundled tapes serve container.
//  4. Nothing reachable → styled error pointing the user at `tapes local
//     up` or --api-target.
//
// The returned cleanup function is always non-nil; callers should defer it.
func bootstrapAPI(ctx context.Context, cfg bootstrapConfig) (string, func(), error) {
	if cfg.out == nil {
		cfg.out = os.Stdout
	}
	if cfg.apiReachable == nil {
		cfg.apiReachable = start.APIReachable
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

	target := normalizeAPITarget(cfg.apiTarget)
	if target != "" && cfg.apiReachable(ctx, target) {
		return target, func() {}, nil
	}

	return "", nil, errors.New(strings.Join([]string{
		cliui.FailMark + " No tapes API is reachable",
		"",
		"  api-target: " + target,
		"",
		"Bring up a local stack and try again:",
		"  tapes local up",
		"",
		"Or point at an existing API:",
		"  tapes deck --api-target <url>",
	}, "\n"))
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

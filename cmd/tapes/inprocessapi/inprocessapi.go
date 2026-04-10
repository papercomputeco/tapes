// Package inprocessapi provides a shared helper for CLI subcommands that
// want to talk to the tapes API server over HTTP without requiring an
// external server. It opens the local SQLite database, runs migrations,
// and starts an api.Server bound to a random localhost port. Callers
// receive the loopback URL and a stop function to invoke at shutdown.
package inprocessapi

import (
	"context"
	"fmt"
	"net"

	"github.com/papercomputeco/tapes/api"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

// Start spins up an in-process tapes API server backed by the SQLite
// database at sqlitePath. Returns the loopback URL the caller should use
// to construct an HTTP client, plus a stop function that must be invoked
// at shutdown to release the listener and close the storage driver.
//
// pricing is passed through to the API server's /v1/sessions/summary
// handler. nil is acceptable; the handler falls back to
// sessions.DefaultPricing in that case.
func Start(ctx context.Context, sqlitePath string, pricing sessions.PricingTable) (string, func(), error) {
	logger := tapeslogger.NewNoop()

	driver, err := sqlite.NewDriver(ctx, sqlitePath)
	if err != nil {
		return "", nil, fmt.Errorf("opening sqlite driver: %w", err)
	}
	if err := driver.Migrate(ctx); err != nil {
		_ = driver.Close()
		return "", nil, fmt.Errorf("running migrations: %w", err)
	}

	server, err := api.NewServer(api.Config{
		ListenAddr: ":0",
		Pricing:    pricing,
	}, driver, logger)
	if err != nil {
		_ = driver.Close()
		return "", nil, fmt.Errorf("creating in-process api server: %w", err)
	}

	// Bind a random localhost port up front so the address is known
	// before Fiber starts serving. Connection attempts that arrive
	// before the goroutine below schedules will queue at the OS level.
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		_ = driver.Close()
		return "", nil, fmt.Errorf("binding in-process listener: %w", err)
	}

	go func() {
		_ = server.RunWithListener(listener)
	}()

	target := "http://" + listener.Addr().String()
	stop := func() {
		_ = server.Shutdown()
		_ = driver.Close()
	}
	return target, stop, nil
}

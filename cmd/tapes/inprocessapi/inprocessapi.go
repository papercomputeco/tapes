// Package inprocessapi provides a shared helper for CLI subcommands that
// want to talk to the tapes API server over HTTP without requiring an
// external server. It opens a local PostgreSQL-backed storage driver, runs migrations,
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
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// Start spins up an in-process tapes API server backed by PostgreSQL
// using postgresDSN. Returns the loopback URL the caller should use
// to construct an HTTP client, plus a stop function that must be invoked
// at shutdown to release the listener and close the storage driver.
//
// pricing is passed through to the API server's /v1/stems
// handler. nil is acceptable; the handler falls back to
// sessions.DefaultPricing in that case.
func Start(ctx context.Context, postgresDSN string, pricing sessions.PricingTable) (string, func(), error) {
	logger := tapeslogger.NewNoop()

	driver, err := postgres.NewDriver(ctx, postgresDSN)
	if err != nil {
		return "", nil, fmt.Errorf("opening postgres driver: %w", err)
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

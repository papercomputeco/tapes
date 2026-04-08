package deckcmder

import (
	"context"
	"fmt"
	"net"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/deck"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

// startInProcessAPI spins up a tapes API server backed by the local SQLite
// database at sqlitePath, listens on a random localhost port, and returns
// the loopback URL plus a stop function the caller must invoke at shutdown.
//
// Used by `tapes deck` when --api-target is unset, so the user experience of
// running the deck against a local file is unchanged even though the data
// path now goes through HTTP for consistency with remote use.
func startInProcessAPI(ctx context.Context, sqlitePath string, pricing deck.PricingTable) (string, func(), error) {
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
	}, driver, driver, logger)
	if err != nil {
		_ = driver.Close()
		return "", nil, fmt.Errorf("creating in-process api server: %w", err)
	}

	// Bind a random localhost port up front so the address is known before
	// Fiber starts serving. Connection attempts that arrive before the
	// goroutine below schedules will queue at the OS level.
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		_ = driver.Close()
		return "", nil, fmt.Errorf("binding in-process listener: %w", err)
	}

	// Run the server in a background goroutine. Errors after Shutdown are
	// expected and ignored; the user-visible failure path is the HTTP
	// requests the deck makes against the loopback URL.
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

package api

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/papercomputeco/tapes/api/mcp"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Server is the API server for managing and querying the Tapes system
type Server struct {
	config    Config
	driver    storage.Driver
	logger    *slog.Logger
	app       *fiber.App
	metrics   *Metrics
	mcpServer *mcp.Server
}

// NewServer creates a new API server.
// The storer is injected to allow sharing with other components
// (e.g., the proxy when not run as a singleton).
func NewServer(config Config, driver storage.Driver, log *slog.Logger) (*Server, error) {
	var err error
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	s := &Server{
		config:  config,
		driver:  driver,
		logger:  log,
		app:     app,
		metrics: NewMetrics(),
	}

	// RED metrics is registered first so it sits as the outermost wrapper.
	// Order matters: the request-count and duration increments run AFTER
	// c.Next() returns, not in a defer, so a panic unwinding through them
	// would skip those updates. Putting recover.New() inside the metrics
	// middleware means recover catches the panic and translates it into
	// an error returned to the metrics middleware — which then derives
	// the right status via the err (see Middleware in metrics.go).
	app.Use(s.metrics.Middleware())
	app.Use(recover.New())

	// Tenant context: canonicalise the client-asserted org_id header onto
	// Locals so the read handlers can scope lookups to a single tenant.
	// Registered after recover (a malformed header can't escape panic
	// translation) and before the routes; it only sets a Local, so it is a
	// no-op for /metrics and /ping.
	app.Use(s.withOrgContext)

	// /metrics is intentionally outside any auth group — Alloy scrapes
	// in-cluster and there is no caller identity to verify.
	app.Get("/metrics", s.metrics.Handler())

	app.Get("/ping", s.handlePing)

	if config.EnableWebUI {
		// Minimal same-origin web UI. Like Prometheus's built-in UI, this is
		// served directly by the API binary and has no frontend build step.
		app.Get("/", s.handleWebUI)
	}

	// v1 surface. Two models share the namespace:
	//   /v1/sessions  — product sessions (sessions-table, UUID-keyed)
	//   /v1/stems     — Merkle leaf chains (hash-keyed, forensic view)
	// Static paths are registered before parameterised ones.
	app.Get("/v1/stats", s.handleStats)
	app.Get("/v1/sessions", s.handleListSessions)
	app.Get("/v1/sessions/:id", s.handleGetSession)
	app.Get("/v1/stems", s.handleListStems)
	app.Get("/v1/stems/:hash/graph", s.handleGetStemGraph)
	app.Get("/v1/stems/:hash", s.handleGetStem)
	app.Get("/v1/search", s.handleSearchEndpoint)

	app.Post("/v1/admin/seed/demo", s.handleSeedDemo)
	app.Post("/v1/admin/backfill/usage", s.handleBackfillUsage)
	app.Post("/v1/admin/backfill/session-status", s.handleBackfillSessionStatus)
	app.Post("/v1/admin/derive/verify", s.handleDeriveVerify)
	app.Post("/v1/admin/derive/run", s.handleDeriveRun)

	// API reference UI. Always mounted — the viewer JS comes from a CDN
	// at view time, so the binary cost is negligible.
	s.mountSwagger(app)

	// Register MCP server if vector driver and embedder are configured
	var mcpServer *mcp.Server
	if config.VectorDriver != nil && config.Embedder != nil {
		s.logger.Debug("creating mcp server")
		mcpServer, err = mcp.NewServer(mcp.Config{
			Driver:       driver,
			VectorDriver: config.VectorDriver,
			Embedder:     config.Embedder,
			Logger:       log,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create MCP server: %w", err)
		}
	} else {
		s.logger.Debug("creating noop mcp server")
		mcpServer, err = mcp.NewServer(mcp.Config{
			Noop: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create noop MCP server: %w", err)
		}
	}

	s.mcpServer = mcpServer

	// Mount MCP handler using the fiber adaptor for net/http Handlers
	// which is what the modelcontextprotocol/go-sdk uses under the hood
	app.All("/v1/mcp", adaptor.HTTPHandler(s.mcpServer.Handler()))

	return s, nil
}

// Run starts the API server on the configured address.
func (s *Server) Run() error {
	s.logger.Info("starting API server",
		"listen", s.config.ListenAddr,
	)
	return s.app.Listen(s.config.ListenAddr)
}

// RunWithListener starts the API server using the provided listener.
func (s *Server) RunWithListener(listener net.Listener) error {
	s.logger.Info("starting API server",
		"listen", listener.Addr().String(),
	)
	return s.app.Listener(listener)
}

// Shutdown gracefully shuts down the API server.
func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

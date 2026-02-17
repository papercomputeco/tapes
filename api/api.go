package api

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/api/mcp"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// Server is the API server for managing and querying the Tapes system
type Server struct {
	config    Config
	driver    storage.Driver
	dagLoader merkle.DagLoader
	logger    *slog.Logger
	app       *fiber.App
	mcpServer *mcp.Server
}

// NewServer creates a new API server.
// The storer is injected to allow sharing with other components
// (e.g., the proxy when not run as a singleton).
func NewServer(config Config, driver storage.Driver, dagLoader merkle.DagLoader, log *slog.Logger) (*Server, error) {
	var err error
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	s := &Server{
		config:    config,
		driver:    driver,
		dagLoader: dagLoader,
		logger:    log,
		app:       app,
	}

	app.Get("/ping", s.handlePing)
	app.Get("/dag/stats", s.handleDAGStats)
	app.Get("/dag/node/:hash", s.handleGetNode)
	app.Get("/dag/history", s.handleListHistories)
	app.Get("/dag/history/:hash", s.handleGetHistory)
	app.Get("/v1/search", s.handleSearchEndpoint)

	// Register MCP server if vector driver and embedder are configured
	var mcpServer *mcp.Server
	if config.VectorDriver != nil && config.Embedder != nil {
		s.logger.Debug("creating mcp server")
		mcpServer, err = mcp.NewServer(mcp.Config{
			DagLoader:    dagLoader,
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

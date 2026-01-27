package api

import (
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// Server is the API server for managing and querying the Tapes system
type Server struct {
	config Config
	storer storage.Driver
	logger *zap.Logger
	app    *fiber.App
}

// NewServer creates a new API server.
// The storer is injected to allow sharing with other components
// (e.g., the proxy when not run as a singleton).
func NewServer(config Config, storer storage.Driver, logger *zap.Logger) *Server {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	s := &Server{
		config: config,
		storer: storer,
		logger: logger,
		app:    app,
	}

	app.Get("/ping", s.handlePing)
	app.Get("/dag/stats", s.handleDAGStats)
	app.Get("/dag/node/:hash", s.handleGetNode)
	app.Get("/dag/history", s.handleListHistories)
	app.Get("/dag/history/:hash", s.handleGetHistory)

	return s
}

// Run starts the API server on the configured address.
func (s *Server) Run() error {
	s.logger.Info("starting API server",
		zap.String("listen", s.config.ListenAddr),
	)
	return s.app.Listen(s.config.ListenAddr)
}

// Shutdown gracefully shuts down the API server.
func (s *Server) Shutdown() error {
	return s.app.Shutdown()
}

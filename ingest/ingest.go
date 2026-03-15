package ingest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/proxy/worker"
)

// TurnPayload is the ingest request body for a single completed conversation turn.
// It carries the raw provider request and response so tapes can parse, store,
// and embed them exactly as the transparent proxy would.
type TurnPayload struct {
	// Provider type: "openai", "anthropic", "ollama"
	Provider string `json:"provider"`

	// AgentName optionally tags the turn (same as X-Tapes-Agent-Name header)
	AgentName string `json:"agent_name,omitempty"`

	// RawRequest is the original request body sent to the LLM provider
	RawRequest json.RawMessage `json:"request"`

	// RawResponse is the complete response body from the LLM provider
	RawResponse json.RawMessage `json:"response"`
}

// BatchPayload is the ingest request body for multiple conversation turns.
type BatchPayload struct {
	Turns []TurnPayload `json:"turns"`
}

// BatchResult reports the outcome of a batch ingest.
type BatchResult struct {
	Accepted int      `json:"accepted"`
	Rejected int      `json:"rejected"`
	Errors   []string `json:"errors,omitempty"`
}

// Server is an HTTP server that accepts completed LLM conversation turns
// for async storage in the Merkle DAG.
type Server struct {
	config     Config
	driver     storage.Driver
	workerPool *worker.Pool
	logger     *slog.Logger
	server     *fiber.App
	providers  map[string]provider.Provider
}

// New creates a new ingest Server.
func New(config Config, driver storage.Driver, log *slog.Logger) (*Server, error) {
	providers := make(map[string]provider.Provider)
	for _, name := range provider.SupportedProviders() {
		prov, err := provider.New(name)
		if err != nil {
			return nil, fmt.Errorf("could not create provider %s: %w", name, err)
		}
		providers[name] = prov
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	wp, err := worker.NewPool(&worker.Config{
		Driver:       driver,
		Publisher:    config.Publisher,
		VectorDriver: config.VectorDriver,
		Embedder:     config.Embedder,
		Project:      config.Project,
		Logger:       log,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create worker pool: %w", err)
	}

	s := &Server{
		config:     config,
		driver:     driver,
		workerPool: wp,
		logger:     log,
		server:     app,
		providers:  providers,
	}

	app.Get("/ping", s.handlePing)
	app.Post("/v1/ingest", s.handleIngest)
	app.Post("/v1/ingest/batch", s.handleBatchIngest)

	return s, nil
}

// Run starts the ingest server on the configured address.
func (s *Server) Run() error {
	s.logger.Info("starting ingest server",
		"listen", s.config.ListenAddr,
	)
	return s.server.Listen(s.config.ListenAddr)
}

// RunWithListener starts the ingest server using the provided listener.
func (s *Server) RunWithListener(listener net.Listener) error {
	s.logger.Info("starting ingest server",
		"listen", listener.Addr().String(),
	)
	return s.server.Listener(listener)
}

// Close gracefully shuts down the server and waits for the worker pool to drain.
func (s *Server) Close() error {
	s.workerPool.Close()
	return s.server.Shutdown()
}

func (s *Server) handlePing(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

func (s *Server) handleIngest(c *fiber.Ctx) error {
	var payload TurnPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
	}

	if err := s.processTurn(&payload); err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "accepted"})
}

func (s *Server) handleBatchIngest(c *fiber.Ctx) error {
	var payload BatchPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid payload: " + err.Error()})
	}

	if len(payload.Turns) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "empty batch"})
	}

	result := BatchResult{}
	for i := range payload.Turns {
		if err := s.processTurn(&payload.Turns[i]); err != nil {
			result.Rejected++
			result.Errors = append(result.Errors, fmt.Sprintf("turn[%d]: %s", i, err.Error()))
		} else {
			result.Accepted++
		}
	}

	return c.Status(fiber.StatusAccepted).JSON(result)
}

// processTurn parses a raw turn payload and enqueues it for async DAG storage.
func (s *Server) processTurn(turn *TurnPayload) error {
	prov, ok := s.providers[turn.Provider]
	if !ok {
		return fmt.Errorf("unsupported provider: %q (supported: %v)", turn.Provider, provider.SupportedProviders())
	}

	parsedReq, err := prov.ParseRequest(turn.RawRequest)
	if err != nil {
		return fmt.Errorf("cannot parse request: %w", err)
	}

	parsedResp, err := prov.ParseResponse(turn.RawResponse)
	if err != nil {
		return fmt.Errorf("cannot parse response: %w", err)
	}

	s.logger.Debug("ingesting turn",
		"provider", prov.Name(),
		"agent", turn.AgentName,
		"model", parsedReq.Model,
	)

	s.workerPool.Enqueue(worker.Job{
		Provider:  prov.Name(),
		AgentName: turn.AgentName,
		Req:       parsedReq,
		Resp:      parsedResp,
	})

	return nil
}

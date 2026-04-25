package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/proxy/worker"
)

// Ingest error classes. Each maps to a distinct HTTP status so operators can
// tell malformed envelopes from unknown providers from downstream outages
// without tailing logs.
var (
	// ErrEnvelope means the POST body could not be decoded as a TurnPayload.
	// Returned as 400 Bad Request.
	ErrEnvelope = errors.New("invalid envelope")

	// ErrUnprocessable covers validation / parse failures inside a well-formed
	// envelope: unknown provider, unparseable provider-specific request /
	// response body, etc. Returned as 422 Unprocessable Entity.
	ErrUnprocessable = errors.New("unprocessable turn")

	// ErrDownstream covers failures that originate below the handler: worker
	// pool saturation, DAG write errors, storage unavailability. Returned as
	// 502 Bad Gateway.
	ErrDownstream = errors.New("downstream failure")
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
	metrics    *Metrics
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
		metrics:    NewMetrics(),
	}

	app.Get("/ping", s.handlePing)
	app.Get("/metrics", adaptor.HTTPHandler(s.metrics.Handler()))
	app.Post("/v1/ingest", s.handleIngest)
	app.Post("/v1/ingest/batch", s.handleBatchIngest)
	app.Get("/v1/ingest/nodes", s.handleListNodesByAgent)

	return s, nil
}

// Metrics exposes the ingest metrics so tests and health checks can scrape
// the registry programmatically.
func (s *Server) Metrics() *Metrics { return s.metrics }

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

// NodeSummary is the per-item shape returned by GET /v1/ingest/nodes.
// Intentionally minimal — operators and canaries only need to confirm a row
// landed for a given agent; rich querying is served by the tapes-api.
type NodeSummary struct {
	Hash      string `json:"hash"`
	Role      string `json:"role"`
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	AgentName string `json:"agent_name,omitempty"`
}

// NodeListResponse wraps a slice of NodeSummary in a count+items envelope so
// the shape is consistent with the rest of the tapes query surface.
type NodeListResponse struct {
	Count int           `json:"count"`
	Nodes []NodeSummary `json:"nodes"`
}

// handleListNodesByAgent returns nodes whose Bucket.AgentName matches the
// ?agent= query parameter. Intended for e2e verification and the staging
// canary — not a general-purpose query surface. Without a filter, returns
// the full list to aid debugging.
//
// The implementation walks the full node list and filters in memory; this is
// O(N) and appropriate for e2e verification (handfuls of nodes) but should
// not be relied on for production queries against a real store.
func (s *Server) handleListNodesByAgent(c *fiber.Ctx) error {
	agent := c.Query("agent")

	nodes, err := s.driver.List(c.Context())
	if err != nil {
		s.logger.Error("ingest list nodes failed", "error", err)
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %v", ErrDownstream, err),
		})
	}

	out := NodeListResponse{Nodes: []NodeSummary{}}
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if agent != "" && n.Bucket.AgentName != agent {
			continue
		}
		out.Nodes = append(out.Nodes, NodeSummary{
			Hash:      n.Hash,
			Role:      n.Bucket.Role,
			Provider:  n.Bucket.Provider,
			Model:     n.Bucket.Model,
			AgentName: n.Bucket.AgentName,
		})
	}
	out.Count = len(out.Nodes)
	return c.JSON(out)
}

func (s *Server) handleIngest(c *fiber.Ctx) error {
	bodySize := len(c.Body())

	var payload TurnPayload
	if err := c.BodyParser(&payload); err != nil {
		s.logger.Warn("ingest envelope rejected",
			"reason", "envelope",
			"error", err,
			"bytes", bodySize,
		)
		s.metrics.ObserveWrite("", ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %s", ErrEnvelope, err),
		})
	}

	start := time.Now()
	if err := s.processTurn(&payload); err != nil {
		s.recordProcessTurnError(payload.Provider, err, bodySize)
		return s.writeProcessTurnError(c, err)
	}
	s.metrics.ObserveDAGLatency(payload.Provider, time.Since(start).Seconds())
	s.metrics.ObserveWrite(payload.Provider, ResultAccepted, bodySize)

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "accepted"})
}

// recordProcessTurnError maps an internal error to the matching metric label
// without affecting the HTTP response flow. Kept separate from
// writeProcessTurnError so batch-ingest can reuse the metric call site.
func (s *Server) recordProcessTurnError(provider string, err error, bodyBytes int) {
	result := ResultRejectParse
	switch {
	case errors.Is(err, ErrEnvelope):
		result = ResultRejectEnv
	case errors.Is(err, ErrDownstream):
		result = ResultDownstreamErr
	case errors.Is(err, ErrUnprocessable):
		// Unknown provider is the most common and worth a distinct label so
		// operators can see it separately from generic parse failures.
		if _, ok := s.providers[provider]; !ok {
			result = ResultUnknownProv
		}
	}
	s.metrics.ObserveWrite(provider, result, bodyBytes)
}

func (s *Server) handleBatchIngest(c *fiber.Ctx) error {
	bodySize := len(c.Body())

	var payload BatchPayload
	if err := c.BodyParser(&payload); err != nil {
		s.logger.Warn("ingest batch envelope rejected",
			"reason", "envelope",
			"error", err,
			"bytes", bodySize,
		)
		s.metrics.ObserveWrite("", ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %s", ErrEnvelope, err),
		})
	}

	if len(payload.Turns) == 0 {
		s.metrics.ObserveWrite("", ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: empty batch", ErrEnvelope),
		})
	}

	result := BatchResult{}
	for i := range payload.Turns {
		t := &payload.Turns[i]
		// Per-turn bytes approximate the cost of a single turn within the batch
		// envelope. Sum of raw request + response is a close lower bound; the
		// JSON envelope overhead (provider, agent_name) is small and omitted to
		// avoid re-marshaling.
		turnBytes := len(t.RawRequest) + len(t.RawResponse)

		start := time.Now()
		if err := s.processTurn(t); err != nil {
			s.recordProcessTurnError(t.Provider, err, turnBytes)
			result.Rejected++
			result.Errors = append(result.Errors, fmt.Sprintf("turn[%d]: %s", i, err.Error()))
			continue
		}
		s.metrics.ObserveDAGLatency(t.Provider, time.Since(start).Seconds())
		s.metrics.ObserveWrite(t.Provider, ResultAccepted, turnBytes)
		result.Accepted++
	}

	return c.Status(fiber.StatusAccepted).JSON(result)
}

// writeProcessTurnError maps an error returned by processTurn to the matching
// HTTP status code. This is the mechanism that splits 400 / 422 / 502 so
// operators can distinguish failure classes at a glance.
func (s *Server) writeProcessTurnError(c *fiber.Ctx, err error) error {
	status := fiber.StatusUnprocessableEntity
	reason := "unprocessable"
	switch {
	case errors.Is(err, ErrEnvelope):
		status = fiber.StatusBadRequest
		reason = "envelope"
	case errors.Is(err, ErrDownstream):
		status = fiber.StatusBadGateway
		reason = "downstream"
	}

	s.logger.Warn("ingest rejected",
		"reason", reason,
		"status", status,
		"error", err,
	)
	return c.Status(status).JSON(llm.ErrorResponse{Error: err.Error()})
}

// processTurn parses a raw turn payload and enqueues it for async DAG storage.
// Returned errors wrap one of ErrEnvelope / ErrUnprocessable / ErrDownstream so
// the caller can map to an HTTP status without re-parsing the message.
func (s *Server) processTurn(turn *TurnPayload) error {
	prov, ok := s.providers[turn.Provider]
	if !ok {
		return fmt.Errorf("%w: unsupported provider %q (supported: %v)",
			ErrUnprocessable, turn.Provider, provider.SupportedProviders())
	}

	parsedReq, err := prov.ParseRequest(turn.RawRequest)
	if err != nil {
		return fmt.Errorf("%w: cannot parse request: %w", ErrUnprocessable, err)
	}

	parsedResp, err := prov.ParseResponse(turn.RawResponse)
	if err != nil {
		return fmt.Errorf("%w: cannot parse response: %w", ErrUnprocessable, err)
	}

	s.logger.Debug("ingesting turn",
		"provider", prov.Name(),
		"agent", turn.AgentName,
		"model", parsedReq.Model,
	)

	if ok := s.workerPool.Enqueue(worker.Job{
		Provider:  prov.Name(),
		AgentName: turn.AgentName,
		Req:       parsedReq,
		Resp:      parsedResp,
	}); !ok {
		s.logger.Error("ingest enqueue failed: worker queue full",
			"provider", prov.Name(),
			"agent", turn.AgentName,
			"model", parsedReq.Model,
		)
		return fmt.Errorf("%w: worker queue full", ErrDownstream)
	}

	return nil
}

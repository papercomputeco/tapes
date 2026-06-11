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
	"github.com/papercomputeco/tapes/pkg/sessions"
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
// It carries the raw provider request plus an already-reduced response.
// Capture adapters such as tapes-extproc own protocol-specific stream reduction;
// ingest owns request parsing, validation, and durable storage.
type TurnPayload struct {
	// Provider type: "openai", "anthropic", "ollama"
	Provider string `json:"provider"`

	// AgentName optionally tags the turn (same as X-Tapes-Agent-Name header)
	AgentName string `json:"agent_name,omitempty"`

	// RawRequest is the original request body sent to the LLM provider.
	RawRequest json.RawMessage `json:"request"`

	// Response is the already reduced, provider-agnostic response for the turn.
	Response llm.ChatResponse `json:"response"`

	// SpanContext is optional harness-supplied trace/span identity. It is
	// normally populated by a harness extension or proxy headers, not by
	// provider payload content.
	SpanContext *storage.SpanContext `json:"span_context,omitempty"`

	// Session is the optional session-tracking envelope. When present,
	// ingest UPSERTs a `sessions` row keyed by
	// (org_id, harness_id, harness_session_id), resolves the
	// parent_session_id FK (placeholder-inserting when needed), and
	// rolls up turn counters — all in the same transaction as the
	// nodes insert. When absent, ingest treats the turn as
	// harness_id="unknown" and derives a synthetic harness_session_id
	// from the captured turn's Merkle root prefix.
	//
	// The type lives in pkg/sessions to avoid an import cycle
	// (proxy/worker depends on it too).
	Session *sessions.IngestEnvelope `json:"session,omitempty"`
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

	if err := payload.Session.Validate(); err != nil {
		s.logger.Warn("ingest envelope rejected",
			"reason", "session",
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
		turnBytes := len(t.RawRequest) + reducedResponseSize(t.Response)

		if err := t.Session.Validate(); err != nil {
			s.logger.Warn("ingest batch turn rejected",
				"reason", "session",
				"error", err,
				"turn", i,
				"bytes", turnBytes,
			)
			s.metrics.ObserveWrite(t.Provider, ResultRejectEnv, turnBytes)
			result.Rejected++
			result.Errors = append(result.Errors, fmt.Sprintf("turn[%d]: %s: %s", i, ErrEnvelope, err))
			continue
		}

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

// reducedResponseSize provides the json marshaled size of the chat response
func reducedResponseSize(resp llm.ChatResponse) int {
	b, err := json.Marshal(resp)
	if err != nil {
		return 0
	}
	return len(b)
}

// validateReducedResponse is a sanity check ontop of a provided llm.ChatResponse
// that ensures the payload to the ingest server is valid.
func validateReducedResponse(resp *llm.ChatResponse) error {
	if resp.Message.Role == "" {
		return errors.New("missing response.message.role")
	}
	if len(resp.Message.Content) == 0 {
		return errors.New("missing response.message.content")
	}
	for i, block := range resp.Message.Content {
		if block.Type == "" {
			return fmt.Errorf("missing response.message.content[%d].type", i)
		}
	}
	return nil
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

	parsedResp := turn.Response
	if err := validateReducedResponse(&parsedResp); err != nil {
		return fmt.Errorf("%w: invalid reduced response: %w", ErrUnprocessable, err)
	}

	// Log session attribution at debug. The full envelope isn't logged
	// (auth_subject, harness_metadata are sensitive); we surface just
	// the natural-key tuple so operators can correlate a turn with
	// the eventual sessions row when triaging ingestion issues.
	var (
		sessHarnessID, sessHarnessSessionID, sessOrgID string
	)
	if turn.Session != nil {
		sessHarnessID = turn.Session.HarnessIDOrUnknown()
		sessHarnessSessionID = turn.Session.HarnessSessionID
		sessOrgID = turn.Session.OrgID
	}
	s.logger.Debug("ingesting turn",
		"provider", prov.Name(),
		"agent", turn.AgentName,
		"model", parsedReq.Model,
		"session_org_id", sessOrgID,
		"session_harness_id", sessHarnessID,
		"session_harness_session_id", sessHarnessSessionID,
	)

	if ok := s.workerPool.Enqueue(worker.Job{
		Provider:    prov.Name(),
		AgentName:   turn.AgentName,
		Req:         parsedReq,
		Resp:        &parsedResp,
		Session:     turn.Session,
		SpanContext: turn.SpanContext,
	}); !ok {
		s.logger.Error("ingest enqueue failed: worker queue full",
			"provider", prov.Name(),
			"agent", turn.AgentName,
			"model", parsedReq.Model,
		)
		// Snapshot depth even on a drop so the gauge reflects saturation.
		s.metrics.SetQueueDepth(s.workerPool.Len())
		return fmt.Errorf("%w: worker queue full", ErrDownstream)
	}

	// Best-effort snapshot of post-enqueue depth. Workers may have already
	// drained the slot we just wrote, so the value can lag actual depth — but
	// over many turns this tracks back-pressure well enough to alert on.
	s.metrics.SetQueueDepth(s.workerPool.Len())
	return nil
}

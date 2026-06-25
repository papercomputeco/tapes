package api

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
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
	// Trace payloads are large JSON (a full session detail measured
	// 2.65MB raw, ~80KB gzipped) and every read crosses a network leg
	// that honors Accept-Encoding — the console's server functions
	// included. Compression is the single highest-leverage byte saver
	// on the read surface.
	app.Use(compress.New())

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

	// v1 surface: product sessions (sessions-table, UUID-keyed) and the
	// span projection beneath them. Static paths are registered before
	// parameterised ones.
	app.Get("/v1/stats", s.handleStats)
	app.Get("/v1/sessions", s.handleListSessions)
	app.Get("/v1/sessions/:id/traces", s.handleGetSessionTraces)
	app.Get("/v1/sessions/:id/raw_turns", s.handleListSessionRawTurns)
	app.Get("/v1/traces", s.handleListTraceSummaries)
	app.Get("/v1/traces/:trace_id/spans/:span_id", s.handleGetSpan)
	app.Get("/v1/traces/:trace_id", s.handleGetTrace)
	app.Get("/v1/sessions/:id", s.handleGetSession)
	app.Get("/v1/sessions/:id/skills", s.handleListSessionSkills)
	app.Get("/v1/search/spans", s.handleSearchSpansEndpoint)

	// Skills: generate from sessions, persist, edit, version, duplicate, and
	// render a drop-in SKILL.md. Skills are keyed on an opaque id (the route
	// key, mirroring sessions); slug is a cosmetic label. Literal/sub-path routes
	// are registered before the bare /:id param routes so they aren't captured.
	app.Get("/v1/skills", s.handleListSkills)
	app.Post("/v1/skills", s.handleCreateSkill)
	app.Post("/v1/skills/generate", s.handleGenerateSkill)
	app.Get("/v1/skills/:id/skill.md", s.handleSkillMarkdown)
	app.Get("/v1/skills/:id/versions", s.handleListSkillVersions)
	app.Post("/v1/skills/:id/versions", s.handlePublishSkill)
	app.Post("/v1/skills/:id/duplicate", s.handleDuplicateSkill)
	app.Put("/v1/skills/:id", s.handleUpdateSkill)
	app.Delete("/v1/skills/:id", s.handleDeleteSkill)
	app.Get("/v1/skills/:id", s.handleGetSkill)

	app.Post("/v1/admin/seed/demo", s.handleSeedDemo)
	app.Post("/v1/admin/backfill/usage", s.handleBackfillUsage)
	app.Post("/v1/admin/backfill/session-status", s.handleBackfillSessionStatus)
	app.Post("/v1/admin/derive/verify", s.handleDeriveVerify)
	app.Post("/v1/admin/derive/run", s.handleDeriveRun)

	// API reference UI. Always mounted — the viewer JS comes from a CDN
	// at view time, so the binary cost is negligible.
	s.mountSwagger(app)

	// Register MCP server if span search and embedder are configured. The
	// MCP `search` tool runs the same span search as GET /v1/search/spans.
	var mcpServer *mcp.Server
	if config.SpanSearcher != nil && config.Embedder != nil {
		s.logger.Debug("creating mcp server")
		mcpServer, err = mcp.NewServer(mcp.Config{
			SpanSearcher: config.SpanSearcher,
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

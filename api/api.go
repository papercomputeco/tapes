package api

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/api/mcp"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/oasfiber"
)

// Server is the API server for managing and querying the Tapes system
type Server struct {
	config    Config
	driver    storage.Driver
	logger    *slog.Logger
	app       *fiber.App
	metrics   *Metrics
	mcpServer *mcp.Server

	// cassettes is the fleet this server publishes, and cassetteSpecs is the
	// cache of the documents it publishes for them. They are separate fields
	// because handler tests state a spec surface directly while still using a
	// real registry; in production both are the same *cassetterunner.Runner.
	cassettes     *cassetterunner.Registry
	cassetteSpecs cassetterunner.SpecCache

	// contracts is the resolved set of tapes contracts this server serves. It
	// is fixed at construction so cassette admission and the discovery
	// document are answering from the same set.
	contracts []cassette.ContractVersion

	// openapi is the live description of this server's own surface, populated
	// by the same calls that register the routes. GET /openapi compiles it —
	// there is no other source for the published contract — which is why a
	// route cannot be served here without being described.
	openapi *tapesoapi.Parser
}

// NewServer creates a new API server.
// The storer is injected to allow sharing with other components
// (e.g., the proxy when not run as a singleton).
func NewServer(config Config, driver storage.Driver, log *slog.Logger) (*Server, error) {
	return newServer(config, driver, log, nil)
}

// newServer builds the server with an explicit source of doc comments.
//
// docs is nil everywhere except under `tapes dev openapi --docs-root`: a
// deployed binary has no source tree to read prose out of, so the contract it
// serves carries route and operation prose but not per-field prose.
func newServer(config Config, driver storage.Driver, log *slog.Logger, docs tapesoapi.TypeDocs) (*Server, error) {
	var err error
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	contracts := resolveContractVersions(config.ContractVersions)
	runner := cassetterunner.NewRunner(cassetterunner.Config{
		Contracts: contracts,
		Title:     "tapes",
		// The aggregate document is versioned with the contract discovery
		// advertises, so /openapi and /v1/cassettes cannot disagree about
		// which surface a client is looking at.
		Version: string(currentContractVersion(contracts)),
	})
	s := &Server{
		config:        config,
		driver:        driver,
		logger:        log,
		app:           app,
		metrics:       NewMetrics(),
		cassettes:     runner.Registry(),
		cassetteSpecs: runner,
		contracts:     contracts,
		openapi:       NewOpenAPIParser(docs),
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

	// /metrics is intentionally outside any auth group — Alloy scrapes
	// in-cluster and there is no caller identity to verify. It is registered
	// straight on the app because Prometheus exposition is scraped by
	// convention, not called from a generated client.
	app.Get("/metrics", s.metrics.Handler())

	if config.EnableWebUI {
		// Minimal same-origin web UI. Like Prometheus's built-in UI, this is
		// served directly by the API binary and has no frontend build step.
		// HTML, not API surface, so it is not described.
		app.Get("/", s.handleWebUI)
	}

	// The MCP server is built before the routes because one of them mounts its
	// handler. The MCP `search` tool runs the same span search as
	// GET /v1/search/spans.
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

	// Every documented route registers through this wrapper, which puts it on
	// the app and into the parser in one call.
	//
	// Registration order is still load-bearing, and the wrapper does not change
	// it: Fiber matches in order, so a literal path must precede the
	// parameterised route that would otherwise swallow it — /v1/sessions/export
	// before /v1/sessions/:id, and the /v1/skills/:id sub-paths before the bare
	// /v1/skills/:id. The route table in openapi_routes.go preserves that order.
	router := oasfiber.Wrap(app, s.openapi, oasfiber.WithUndocumented(oasfiber.Fail))
	s.mountV1(router)

	// API reference UI. Always mounted — the viewer JS comes from a CDN
	// at view time, so the binary cost is negligible.
	s.mountReference(app)

	// The cassette surface is always present. An install with no cassettes
	// publishes an empty discovery document rather than changing the API shape.
	s.mountCassettes(router)

	// A route the wrapper could not describe is a defect in this file, not in a
	// request. Surfacing it at construction is the whole point of the Fail
	// policy above: the alternative is a contract that silently omits a served
	// endpoint, which is the failure this machinery exists to prevent.
	if err := router.Err(); err != nil {
		return nil, err
	}

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

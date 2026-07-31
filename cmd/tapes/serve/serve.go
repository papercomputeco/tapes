// Package servecmder provides the serve command with subcommands for running services.
package servecmder

import (
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
	deriveworkercmder "github.com/papercomputeco/tapes/cmd/tapes/serve/deriveworker"
	embedworkercmder "github.com/papercomputeco/tapes/cmd/tapes/serve/embedworker"
	ingestcmder "github.com/papercomputeco/tapes/cmd/tapes/serve/ingest"
	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

// ServeCommander runs core's process set and optionally publishes a registry
// of externally managed cassettes.
type ServeCommander struct {
	flags config.FlagSet
	stack Stack

	refresh time.Duration
}

// ServeFlags defines the flags for the parent "tapes serve" command.
var ServeFlags = config.FlagSet{
	config.FlagProxyListen:    {Name: "proxy-listen", Shorthand: "p", ViperKey: "proxy.listen", Description: "Address for proxy to listen on"},
	config.FlagAPIListen:      {Name: "api-listen", Shorthand: "a", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagAPIWebUI:       {Name: "api-web-ui", ViperKey: "api.web_ui", Description: "Enable the minimal browser UI at /"},
	config.FlagIngestListen:   {Name: "ingest-listen", Shorthand: "i", ViperKey: "ingest.listen", Description: "Address for ingest server to listen on (sidecar mode)"},
	config.FlagUpstream:       {Name: "upstream", Shorthand: "u", ViperKey: "proxy.upstream", Description: "Upstream LLM provider URL"},
	config.FlagProvider:       {Name: "provider", ViperKey: "proxy.provider", Description: "LLM provider type (anthropic, openai, ollama)"},
	config.FlagPostgres:       {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:        {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
	config.FlagVectorStoreTgt: {Name: "vector-store-target", ViperKey: "vector_store.target", Description: "pgvector connection string (defaults to storage.postgres_dsn when unset)"},
	config.FlagEmbeddingProv:  {Name: "embedding-provider", ViperKey: "embedding.provider", Description: "Embedding provider type (e.g., ollama, openai)"},
	config.FlagEmbeddingTgt:   {Name: "embedding-target", ViperKey: "embedding.target", Description: "Embedding provider URL"},
	config.FlagEmbeddingModel: {Name: "embedding-model", ViperKey: "embedding.model", Description: "Embedding model name (e.g., embeddinggemma, text-embedding-3-large)"},
	config.FlagEmbeddingDims:  {Name: "embedding-dimensions", ViperKey: "embedding.dimensions", Description: "Embedding dimensionality"},
	config.FlagCassettes:      {Name: "cassettes", ViperKey: "cassettes", Description: "Full cassette OpenAPI URLs (comma-separated or repeated)"},
}

const serveLongDesc string = `Run Tapes services.

Use subcommands to run individual services or all services together:
  tapes serve            Run proxy and API server together
  tapes serve api        Run just the API server
  tapes serve proxy      Run just the proxy server
  tapes serve ingest     Run just the ingest server (sidecar mode)
  tapes serve derive-worker  Run the derive worker (raw → derived layer)
  tapes serve embed-worker   Run the embed worker (derived spans → search vectors)

Configure cassettes = ["http://host/openapi"] in config.toml, set a CSV list
in TAPES_CASSETTES, or pass --cassettes to publish already-running cassette
services. Process lifecycle,
credentials, and grants remain the operator's responsibility.

Optionally configure vector storage and embeddings of text content for "tapes search"
agentic functionality.`

const serveShortDesc string = "Run Tapes services"

func NewServeCmd() *cobra.Command {
	cmder := &ServeCommander{
		flags: ServeFlags,
	}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: serveShortDesc,
		Long:  serveLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.stack.Resolve(cmd, cmder.flags)
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			telemetry.FromContext(cmd.Context()).CaptureServerStarted("both")

			return cmder.run(cmd)
		},
	}

	cmder.stack.AddFlags(cmd, cmder.flags)
	cmd.Flags().DurationVar(&cmder.refresh, "cassette-refresh", 30*time.Second, "How often to refresh cassette OpenAPI documents")

	cmd.AddCommand(apicmder.NewAPICmd())
	cmd.AddCommand(deriveworkercmder.NewDeriveWorkerCmd())
	cmd.AddCommand(embedworkercmder.NewEmbedWorkerCmd())
	cmd.AddCommand(ingestcmder.NewIngestCmd())
	cmd.AddCommand(proxycmder.NewProxyCmd())

	return cmd
}

func (c *ServeCommander) run(cmd *cobra.Command) error {
	c.stack.Logger = logger.FromContext(cmd.Context())
	c.configureCassettes(cmd)

	// Signal-aware context: a SIGINT/SIGTERM cancels it, which stops the
	// in-process workers' run loops (the HTTP servers are torn down via their
	// deferred Close()).
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return c.stack.Run(ctx)
}

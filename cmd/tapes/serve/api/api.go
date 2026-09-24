// Package apicmder provides the API tapes server cobra command.
package apicmder

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/cmd/tapes/serve/internallisten"
	"github.com/papercomputeco/tapes/internal/memlimit"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type apiCommander struct {
	flags config.FlagSet

	listen      string
	postgresDSN string
	webUI       bool

	cassetteSources []string
	cassetteRefresh time.Duration

	logger *slog.Logger
}

// apiFlags defines the flags for the standalone API subcommand.
var apiFlags = config.FlagSet{
	config.FlagAPIListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagAPIWebUI:            {Name: "web-ui", ViperKey: "api.web_ui", Description: "Enable the minimal browser UI at /"},
	config.FlagPostgres:            {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagCassettes:           {Name: "cassettes", ViperKey: "cassettes", Description: "Full cassette OpenAPI URLs (comma-separated or repeated)"},
}

const apiLongDesc string = `Run the Tapes API server for inspecting, managing, and query agent sessions.`

const apiShortDesc string = "Run the Tapes API server"

func NewAPICmd() *cobra.Command {
	return newAPICmd(&apiCommander{flags: apiFlags})
}

func newAPICmd(cmder *apiCommander) *cobra.Command {
	if cmder.flags == nil {
		cmder.flags = apiFlags
	}

	cmd := &cobra.Command{
		Use:   "api",
		Short: apiShortDesc,
		Long:  apiLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagAPIListenStandalone,
				config.FlagAPIWebUI,
				config.FlagPostgres,
				config.FlagCassettes,
			})
			cmder.cassetteSources, err = config.GetRegisteredStringSlice(v, cmd, cmder.flags, config.FlagCassettes)
			if err != nil {
				return fmt.Errorf("loading cassette sources: %w", err)
			}
			if err := config.ValidateCassetteSources(cmder.cassetteSources); err != nil {
				return err
			}

			cmder.listen = v.GetString("api.listen")
			cmder.webUI = v.GetBool("api.web_ui")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			telemetry.FromContext(cmd.Context()).CaptureServerStarted("api")
			return cmder.run(cmd.Context())
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagAPIListenStandalone, &cmder.listen)
	config.AddBoolFlag(cmd, cmder.flags, config.FlagAPIWebUI, &cmder.webUI)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringSliceFlag(cmd, cmder.flags, config.FlagCassettes, &cmder.cassetteSources)
	cmd.Flags().DurationVar(&cmder.cassetteRefresh, "cassette-refresh", 30*time.Second,
		"How often to refresh cassette OpenAPI documents")

	return cmd
}

func (c *apiCommander) run(ctx context.Context) error {
	c.logger = logger.FromContext(ctx)

	// Bound the transient allocation overshoot of a large session read to
	// the container budget: a session's traces or export bundle is built
	// in one response, and the per-request churn lets the heap roughly
	// double before GC runs, so serving a large session can spike past the
	// memory limit and get the server OOM-killed even though its live set
	// fits. A cgroup-derived soft limit GC-paces the peak back toward the
	// live set. Applied before the server exists so every allocation is
	// paced. No-op when GOMEMLIMIT is set or no cgroup limit exists.
	memlimit.ApplySoftMemoryLimit(c.logger)

	if len(c.cassetteSources) > 0 {
		c.logger.Info("configured cassette OpenAPI sources",
			"count", len(c.cassetteSources),
		)
	}

	driver, err := postgres.NewDriver(ctx, c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	apiConfig := api.Config{
		ListenAddr:  c.listen,
		EnableWebUI: c.webUI,
	}

	server, err := api.NewServer(apiConfig, driver, c.logger) //nolint:contextcheck // Fiber owns request contexts.
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}
	// contextcheck notices SetSources' withdrawal path dispatches
	// registry-change hooks under its own bounded context. Deliberate: source
	// configuration is lifecycle plumbing, not a request, and each hook POST
	// carries its own timeout. nolintlint rides along because contextcheck's
	// cross-package analysis resolves this chain only some of the time.
	server.SetCassetteSources(c.cassetteSources) //nolint:contextcheck,nolintlint
	if len(c.cassetteSources) > 0 {
		server.StartCassetteSpecRefresh(ctx, c.cassetteRefresh)
	}

	internalServer, err := internallisten.New(server, internallisten.Config(), c.logger)
	if err != nil {
		return err
	}

	c.logger.Info("starting API server",
		"listen", c.listen,
	)

	// The API server's result is forwarded whether or not it is an error, so a
	// graceful shutdown still ends the command the way it did when this was a
	// direct return. The internal listener only speaks up when it fails: its
	// own graceful stop is not a reason to stop serving the API.
	errChan := make(chan error, 2)
	go func() { errChan <- server.Run() }()
	if internalServer != nil {
		defer func() { _ = internalServer.Shutdown() }()
		go func() {
			if err := internalServer.Run(); err != nil {
				errChan <- fmt.Errorf("internal listener error: %w", err)
			}
		}()
	}

	return <-errChan
}

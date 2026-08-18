// Package ingestcmder provides the ingest server cobra command.
package ingestcmder

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/git"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type ingestCommander struct {
	flags config.FlagSet

	listen      string
	postgresDSN string
	project     string

	logger *slog.Logger
}

// ingestFlags defines the flags for the standalone ingest subcommand.
// Uses FlagIngestListenStandalone (--listen/-l) instead of the parent's
// --ingest-listen/-i, and omits proxy/api-specific flags.
var ingestFlags = config.FlagSet{
	config.FlagIngestListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "ingest.listen", Description: "Address for ingest server to listen on"},
	config.FlagPostgres:               {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagProject:                {Name: "project", ViperKey: "proxy.project", Description: "Project name to tag sessions (default: auto-detect from git)"},
}

const ingestLongDesc string = `Run the ingest server (sidecar mode).

The ingest server accepts completed LLM conversation turns via HTTP and appends
them to the immutable raw-turn capture log. Use this when an external gateway
(e.g., Envoy AI Gateway) handles upstream LLM traffic and tapes only needs to
capture the turns for the deriver.

Endpoints:
  POST /v1/ingest        Accept a single conversation turn
`

const ingestShortDesc string = "Run the Tapes ingest server (sidecar mode)"

// NewIngestCmd creates the cobra command for the standalone ingest server.
func NewIngestCmd() *cobra.Command {
	cmder := &ingestCommander{
		flags: ingestFlags,
	}

	cmd := &cobra.Command{
		Use:   "ingest",
		Short: ingestShortDesc,
		Long:  ingestLongDesc,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagIngestListenStandalone,
				config.FlagPostgres,
				config.FlagProject,
			})

			cmder.listen = v.GetString("ingest.listen")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.project = v.GetString("proxy.project")
			if cmder.project == "" {
				cmder.project = git.RepoName(cmd.Context())
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmder.logger = logger.FromContext(cmd.Context())
			telemetry.FromContext(cmd.Context()).CaptureServerStarted("ingest")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagIngestListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagProject, &cmder.project)

	return cmd
}

func (c *ingestCommander) run() error {
	driver, err := postgres.NewDriver(context.TODO(), c.postgresDSN)
	if err != nil {
		return err
	}
	defer driver.Close()

	cfg := ingest.Config{
		ListenAddr: c.listen,
		Project:    c.project,
	}

	s, err := ingest.New(cfg, driver, c.logger)
	if err != nil {
		return fmt.Errorf("creating ingest server: %w", err)
	}
	defer s.Close()

	c.logger.Info("starting ingest server",
		"listen", c.listen,
	)

	return s.Run()
}

// Package apicmder provides the API tapes server cobra command.
package apicmder

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type apiCommander struct {
	flags config.FlagSet

	listen      string
	debug       bool
	sqlitePath  string
	postgresDSN string

	logger *slog.Logger
}

// apiFlags defines the flags for the standalone API subcommand.
var apiFlags = config.FlagSet{
	config.FlagAPIListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagSQLite:              {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagPostgres:            {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
}

const apiLongDesc string = `Run the Tapes API server for inspecting, managing, and query agent sessions.`

const apiShortDesc string = "Run the Tapes API server"

func NewAPICmd() *cobra.Command {
	cmder := &apiCommander{
		flags: apiFlags,
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
				config.FlagSQLite,
				config.FlagPostgres,
			})

			cmder.listen = v.GetString("api.listen")
			cmder.sqlitePath = v.GetString("storage.sqlite_path")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			telemetry.FromContext(cmd.Context()).CaptureServerStarted("api")
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagAPIListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)

	return cmd
}

func (c *apiCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	if err := driver.Migrate(context.Background()); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}

	config := api.Config{
		ListenAddr: c.listen,
	}

	server, err := api.NewServer(config, driver, c.logger)
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}

	c.logger.Info("starting API server",
		"listen", c.listen,
	)

	return server.Run()
}

func (c *apiCommander) newStorageDriver() (storage.Driver, error) {
	if c.postgresDSN != "" {
		driver, err := postgres.NewDriver(context.Background(), c.postgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL storer: %w", err)
		}
		c.logger.Info("using PostgreSQL storage")
		return driver, nil
	}

	if c.sqlitePath != "" {
		driver, err := sqlite.NewDriver(context.Background(), c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		c.logger.Info("using SQLite storage", "path", c.sqlitePath)
		return driver, nil
	}

	c.logger.Info("using in-memory storage")
	return inmemory.NewDriver(), nil
}

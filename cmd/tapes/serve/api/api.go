// Package apicmder provides the API tapes server cobra command.
package apicmder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

type apiCommander struct {
	flags config.FlagSet

	listen      string
	debug       bool
	sqlitePath  string
	postgresDSN string

	tursoDSN          string
	tursoAuthToken    string
	tursoSyncInterval string
	tursoLocalPath    string

	logger *slog.Logger
}

// apiFlags defines the flags for the standalone API subcommand.
var apiFlags = config.FlagSet{
	config.FlagAPIListenStandalone: {Name: "listen", Shorthand: "l", ViperKey: "api.listen", Description: "Address for API server to listen on"},
	config.FlagSQLite:              {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagPostgres:            {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
	config.FlagTurso:               {Name: "turso", ViperKey: "storage.turso_dsn", Description: "Turso database URL (e.g., libsql://<name>.turso.io)"},
	config.FlagTursoAuthToken:      {Name: "turso-auth-token", ViperKey: "storage.turso_auth_token", Description: "Turso authentication token"},
	config.FlagTursoSyncInterval:   {Name: "turso-sync-interval", ViperKey: "storage.turso_sync_interval", Description: "Turso embedded replica sync interval (e.g., 5s)"},
	config.FlagTursoLocalPath:      {Name: "turso-local-path", ViperKey: "storage.turso_local_path", Description: "Local replica path (enables embedded replica mode)"},
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
				config.FlagTurso,
				config.FlagTursoAuthToken,
				config.FlagTursoSyncInterval,
				config.FlagTursoLocalPath,
			})

			cmder.listen = v.GetString("api.listen")
			cmder.sqlitePath = v.GetString("storage.sqlite_path")
			cmder.postgresDSN = v.GetString("storage.postgres_dsn")
			cmder.tursoDSN = v.GetString("storage.turso_dsn")
			cmder.tursoAuthToken = v.GetString("storage.turso_auth_token")
			cmder.tursoSyncInterval = v.GetString("storage.turso_sync_interval")
			cmder.tursoLocalPath = v.GetString("storage.turso_local_path")
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagAPIListenStandalone, &cmder.listen)
	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagTurso, &cmder.tursoDSN)
	config.AddStringFlag(cmd, cmder.flags, config.FlagTursoAuthToken, &cmder.tursoAuthToken)
	config.AddStringFlag(cmd, cmder.flags, config.FlagTursoSyncInterval, &cmder.tursoSyncInterval)
	config.AddStringFlag(cmd, cmder.flags, config.FlagTursoLocalPath, &cmder.tursoLocalPath)

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

	// cast the driver as a DagLoader
	dagLoader, ok := driver.(merkle.DagLoader)
	if !ok {
		return errors.New("storage driver does not implement merkle.DagLoader")
	}

	config := api.Config{
		ListenAddr: c.listen,
	}

	server, err := api.NewServer(config, driver, dagLoader, c.logger)
	if err != nil {
		return fmt.Errorf("could not build new api server: %w", err)
	}

	c.logger.Info("starting API server",
		"listen", c.listen,
	)

	return server.Run()
}

// newStorageDriver is defined in storage.go (default) or storage_turso.go (build tag: turso)

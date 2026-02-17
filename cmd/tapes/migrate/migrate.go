// Package migratecmder provides the hidden tapes migrate command for running
// schema migrations independently of starting services.
package migratecmder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

type migrateCommander struct {
	flags config.FlagSet

	sqlitePath  string
	postgresDSN string
	debug       bool

	logger *slog.Logger
}

var migrateFlags = config.FlagSet{
	config.FlagSQLite:   {Name: "sqlite", Shorthand: "s", ViperKey: "storage.sqlite_path", Description: "Path to SQLite database"},
	config.FlagPostgres: {Name: "postgres", ViperKey: "storage.postgres_dsn", Description: "PostgreSQL connection string (e.g., postgres://user:pass@host:5432/db)"},
}

// NewMigrateCmd creates the hidden "tapes migrate" command.
func NewMigrateCmd() *cobra.Command {
	cmder := &migrateCommander{
		flags: migrateFlags,
	}

	cmd := &cobra.Command{
		Use:    "migrate",
		Short:  "Run schema migrations",
		Long:   "Apply any pending schema migrations to the configured storage backend.",
		Hidden: true,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagSQLite,
				config.FlagPostgres,
			})

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
			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagSQLite, &cmder.sqlitePath)
	config.AddStringFlag(cmd, cmder.flags, config.FlagPostgres, &cmder.postgresDSN)

	return cmd
}

func (c *migrateCommander) run() error {
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	c.logger.Info("running schema migrations")

	if err := driver.Migrate(context.Background()); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	c.logger.Info("schema migrations complete")
	return nil
}

func (c *migrateCommander) newStorageDriver() (storage.Driver, error) {
	if c.postgresDSN != "" {
		driver, err := postgres.NewDriver(context.Background(), c.postgresDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to create PostgreSQL driver: %w", err)
		}
		c.logger.Info("using PostgreSQL storage")
		return driver, nil
	}

	if c.sqlitePath != "" {
		driver, err := sqlite.NewDriver(context.Background(), c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite driver: %w", err)
		}
		c.logger.Info("using SQLite storage", "path", c.sqlitePath)
		return driver, nil
	}

	return nil, errors.New("no persistent storage configured: set --sqlite or --postgres (or configure storage.sqlite_path / storage.postgres_dsn)")
}

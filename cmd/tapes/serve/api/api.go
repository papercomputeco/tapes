// Package apicmder provides the API tapes server cobra command.
package apicmder

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
)

type apiCommander struct {
	listen     string
	debug      bool
	sqlitePath string
	logger     *zap.Logger
}

const apiLongDesc string = `Run the Tapes API server for inspecting, managing, and query agent sessions.`

const apiShortDesc string = "Run the Tapes API server"

func NewAPICmd() *cobra.Command {
	cmder := &apiCommander{}

	cmd := &cobra.Command{
		Use:   "api",
		Short: apiShortDesc,
		Long:  apiLongDesc,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}

			return cmder.run()
		},
	}

	cmd.Flags().StringVarP(&cmder.listen, "listen", "l", ":8081", "Address for API server to listen on")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (default: in-memory)")

	return cmd
}

func (c *apiCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer c.logger.Sync()

	driver, err := c.newStorageDriver()
	if err != nil {
		return err
	}
	defer driver.Close()

	config := api.Config{
		ListenAddr: c.listen,
	}

	server := api.NewServer(config, driver, c.logger)

	c.logger.Info("starting API server",
		zap.String("listen", c.listen),
	)

	return server.Run()
}

func (c *apiCommander) newStorageDriver() (storage.Driver, error) {
	if c.sqlitePath != "" {
		driver, err := sqlite.NewSQLiteDriver(c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		c.logger.Info("using SQLite storage", zap.String("path", c.sqlitePath))
		return driver, nil
	}

	c.logger.Info("using in-memory storage")
	return inmemory.NewInMemoryDriver(), nil
}

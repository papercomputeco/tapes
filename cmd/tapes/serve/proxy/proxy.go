// Package proxycmder provides the proxy server command.
package proxycmder

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/proxy"
)

type proxyCommander struct {
	listen     string
	upstream   string
	debug      bool
	sqlitePath string
	logger     *zap.Logger
}

const proxyLongDesc string = `Run the proxy server.

The proxy intercepts all requests and transparently forwards them to the
configured upstream URL, recording request/response conversation turns.`

const proxyShortDesc string = "Run the Tapes proxy server"

func NewProxyCmd() *cobra.Command {
	cmder := &proxyCommander{}

	cmd := &cobra.Command{
		Use:   "proxy",
		Short: proxyShortDesc,
		Long:  proxyLongDesc,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}

			return cmder.run()
		},
	}

	cmd.Flags().StringVarP(&cmder.listen, "listen", "l", ":8080", "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", "http://localhost:11434", "Upstream LLM provider URL")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (default: in-memory)")

	return cmd
}

func (c *proxyCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer c.logger.Sync()

	storer, err := c.createStorer()
	if err != nil {
		return err
	}
	defer storer.Close()

	config := proxy.Config{
		ListenAddr:  c.listen,
		UpstreamURL: c.upstream,
	}

	p := proxy.New(config, storer, c.logger)
	defer p.Close()

	c.logger.Info("starting proxy server",
		zap.String("listen", c.listen),
		zap.String("upstream", c.upstream),
	)

	return p.Run()
}

func (c *proxyCommander) createStorer() (merkle.Storer, error) {
	if c.sqlitePath != "" {
		storer, err := merkle.NewSQLiteStorer(c.sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		c.logger.Info("using SQLite storage", zap.String("path", c.sqlitePath))
		return storer, nil
	}

	c.logger.Info("using in-memory storage")
	return merkle.NewMemoryStorer(), nil
}

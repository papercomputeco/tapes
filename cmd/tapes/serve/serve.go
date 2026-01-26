// Package servecmder provides the serve command with subcommands for running services.
package servecmder

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/api"
	apicmder "github.com/papercomputeco/tapes/cmd/tapes/serve/api"
	proxycmder "github.com/papercomputeco/tapes/cmd/tapes/serve/proxy"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/proxy"
)

type ServeCommander struct {
	proxyListen  string
	apiListen    string
	upstream     string
	providerType string
	debug        bool
	sqlitePath   string
	logger       *zap.Logger
}

const serveLongDesc string = `Run Tapes services.

Use subcommands to run individual services or all services together:
  tapes serve          Run both proxy and API server together
  tapes serve api      Run just the API server
  tapes serve proxy    Run just the proxy server`

const serveShortDesc string = "Run Tapes services"

func NewServeCmd() *cobra.Command {
	cmder := &ServeCommander{}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: serveShortDesc,
		Long:  serveLongDesc,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}
			return cmder.run()
		},
	}

	cmd.Flags().StringVarP(&cmder.proxyListen, "proxy-listen", "p", ":8080", "Address for proxy to listen on")
	cmd.Flags().StringVarP(&cmder.apiListen, "api-listen", "a", ":8081", "Address for API server to listen on")
	cmd.Flags().StringVarP(&cmder.upstream, "upstream", "u", "http://localhost:11434", "Upstream LLM provider URL")
	cmd.Flags().StringVar(&cmder.providerType, "provider", "ollama", "LLM provider type (anthropic, openai, ollama, besteffort)")
	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database (default: in-memory)")

	cmd.AddCommand(apicmder.NewAPICmd())
	cmd.AddCommand(proxycmder.NewProxyCmd())

	return cmd
}

func (c *ServeCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer c.logger.Sync()

	// Create shared storer
	storer, err := c.createStorer()
	if err != nil {
		return err
	}
	defer storer.Close()

	// Create proxy
	proxyConfig := proxy.Config{
		ListenAddr:   c.proxyListen,
		UpstreamURL:  c.upstream,
		ProviderType: c.providerType,
	}
	p, err := proxy.New(proxyConfig, storer, c.logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer p.Close()

	c.logger.Info("starting proxy",
		zap.String("proxy_addr", c.proxyListen),
		zap.String("upstream", c.upstream),
		zap.String("provider", c.providerType),
	)

	// Create API server
	apiConfig := api.Config{
		ListenAddr: c.apiListen,
	}
	apiServer := api.NewServer(apiConfig, storer, c.logger)

	c.logger.Info("starting api server",
		zap.String("api_addr", c.apiListen),
	)

	// Channel to capture errors from goroutines
	errChan := make(chan error, 2)

	// Start proxy in goroutine
	go func() {
		if err := p.Run(); err != nil {
			errChan <- fmt.Errorf("proxy error: %w", err)
		}
	}()

	// Start API server in goroutine
	go func() {
		if err := apiServer.Run(); err != nil {
			errChan <- fmt.Errorf("API server error: %w", err)
		}
	}()

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		return err
	case sig := <-sigChan:
		c.logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		return nil
	}
}

func (c *ServeCommander) createStorer() (merkle.Storer, error) {
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

// Package startcmder provides the start command for launching tapes and agents.
package startcmder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/embeddings"
	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/start"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	"github.com/papercomputeco/tapes/pkg/vector"
	vectorutils "github.com/papercomputeco/tapes/pkg/vector/utils"
	"github.com/papercomputeco/tapes/proxy"
)

const (
	startLongDesc = `Start tapes and optionally launch an agent.

Examples:
  tapes start
  tapes start claude
  tapes start opencode
  tapes start codex
  tapes start --logs
`
	startShortDesc = "Start tapes services and agents"

	agentClaude   = "claude"
	agentOpenCode = "opencode"
	agentCodex    = "codex"
)

type startCommander struct {
	debug     bool
	configDir string
	logs      bool
	daemon    bool
}

type startConfig struct {
	SQLitePath          string
	VectorStoreProvider string
	VectorStoreTarget   string
	EmbeddingProvider   string
	EmbeddingTarget     string
	EmbeddingModel      string
	EmbeddingDimensions uint
	DefaultProvider     string
	DefaultUpstream     string
	OllamaUpstream      string
}

func NewStartCmd() *cobra.Command {
	cmder := &startCommander{}

	cmd := &cobra.Command{
		Use:   "start [agent]",
		Short: startShortDesc,
		Long:  startLongDesc,
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}
			cmder.configDir, err = cmd.Flags().GetString("config-dir")
			if err != nil {
				return fmt.Errorf("could not get config-dir flag: %w", err)
			}
			cmder.logs, err = cmd.Flags().GetBool("logs")
			if err != nil {
				return fmt.Errorf("could not get logs flag: %w", err)
			}
			cmder.daemon, err = cmd.Flags().GetBool("daemon")
			if err != nil {
				return fmt.Errorf("could not get daemon flag: %w", err)
			}

			agent := ""
			if len(args) == 1 {
				agent = strings.ToLower(strings.TrimSpace(args[0]))
			}

			switch {
			case cmder.logs:
				return cmder.runLogs(cmd.Context(), cmd.OutOrStdout())
			case cmder.daemon:
				return cmder.runDaemon(cmd.Context())
			case agent == "":
				return cmder.runForeground(cmd.Context())
			default:
				return cmder.runAgent(cmd.Context(), agent)
			}
		},
	}

	cmd.Flags().Bool("logs", false, "Stream logs from the running tapes start daemon")
	cmd.Flags().Bool("daemon", false, "Run start daemon (internal)")
	_ = cmd.Flags().MarkHidden("daemon")

	return cmd
}

func (c *startCommander) runLogs(ctx context.Context, out io.Writer) error {
	manager, err := start.NewManager(c.configDir)
	if err != nil {
		return err
	}

	lock, err := manager.Lock()
	if err != nil {
		return err
	}
	state, err := manager.LoadState()
	if releaseErr := lock.Release(); releaseErr != nil {
		return releaseErr
	}
	if err != nil {
		return err
	}
	if !stateHealthy(ctx, state) {
		return errors.New("daemon is not running")
	}

	logPath := manager.LogPath
	if state != nil && state.LogPath != "" {
		logPath = state.LogPath
	}

	if _, err := os.Stat(logPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("no start logs found")
		}
		return fmt.Errorf("checking log file: %w", err)
	}

	return followLog(ctx, logPath, out)
}

func (c *startCommander) runAgent(ctx context.Context, agent string) error {
	if !isSupportedAgent(agent) {
		return fmt.Errorf("unsupported agent: %s", agent)
	}

	manager, err := start.NewManager(c.configDir)
	if err != nil {
		return err
	}

	state, err := c.ensureDaemon(ctx, manager)
	if err != nil {
		return err
	}

	proxyURL := strings.TrimRight(state.ProxyURL, "/")
	agentBaseURL := fmt.Sprintf("%s/agents/%s", proxyURL, agent)

	// #nosec G204 -- agent commands are restricted to known binaries.
	cmd := exec.CommandContext(ctx, agentCommand(agent))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = os.Environ()

	cleanup := func() error { return nil }

	switch agent {
	case agentClaude:
		cmd.Env = append(cmd.Env, "ANTHROPIC_BASE_URL="+agentBaseURL)
	case agentCodex:
		cmd.Env = append(cmd.Env,
			"OPENAI_BASE_URL="+agentBaseURL,
			"OPENAI_API_BASE="+agentBaseURL,
		)
	case agentOpenCode:
		var configRoot string
		cleanup, configRoot, err = configureOpenCode(agentBaseURL)
		if err != nil {
			return err
		}
		cmd.Env = append(cmd.Env, "XDG_CONFIG_HOME="+configRoot)
	}

	if err := cmd.Start(); err != nil {
		_ = cleanup()
		return fmt.Errorf("starting %s: %w", agent, err)
	}

	agentPID := cmd.Process.Pid
	if err := c.registerAgent(manager, agent, agentPID); err != nil {
		_ = cleanup()
		return err
	}

	err = cmd.Wait()
	cleanupErr := cleanup()
	if err := c.unregisterAgent(manager, agentPID); err != nil {
		return err
	}
	if cleanupErr != nil {
		return cleanupErr
	}

	if err != nil {
		return fmt.Errorf("%s exited: %w", agent, err)
	}

	return nil
}

func (c *startCommander) runForeground(ctx context.Context) error {
	manager, err := start.NewManager(c.configDir)
	if err != nil {
		return err
	}

	logFile, err := os.OpenFile(manager.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer logFile.Close()

	logger := logger.NewLoggerWithWriters(c.debug, os.Stdout, logFile)
	defer func() { _ = logger.Sync() }()

	return c.runServices(ctx, manager, logger, false)
}

func (c *startCommander) runDaemon(ctx context.Context) error {
	manager, err := start.NewManager(c.configDir)
	if err != nil {
		return err
	}

	logFile, err := os.OpenFile(manager.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer logFile.Close()

	logger := logger.NewLoggerWithWriters(c.debug, logFile)
	defer func() { _ = logger.Sync() }()

	return c.runServices(ctx, manager, logger, true)
}

func (c *startCommander) runServices(ctx context.Context, manager *start.Manager, zapLogger *zap.Logger, shutdownWhenIdle bool) error {
	startCfg, err := c.loadConfig()
	if err != nil {
		return err
	}

	lock, err := manager.Lock()
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()

	listenerConfig := &net.ListenConfig{}
	proxyListener, err := listenerConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("creating proxy listener: %w", err)
	}
	apiListener, err := listenerConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("creating api listener: %w", err)
	}

	proxyURL := "http://" + proxyListener.Addr().String()
	apiURL := "http://" + apiListener.Addr().String()

	state := &start.State{
		DaemonPID:        os.Getpid(),
		ProxyURL:         proxyURL,
		APIURL:           apiURL,
		ShutdownWhenIdle: shutdownWhenIdle,
		LogPath:          manager.LogPath,
	}
	if err := manager.SaveState(state); err != nil {
		return err
	}

	if err := lock.Release(); err != nil {
		return err
	}

	driver, err := c.newStorageDriver(ctx, startCfg, zapLogger)
	if err != nil {
		return err
	}
	defer driver.Close()

	dagLoader, err := c.newDagLoader(ctx, startCfg, zapLogger, driver)
	if err != nil {
		return err
	}

	vectorDriver, embedder, err := c.newVectorAndEmbedder(startCfg, zapLogger)
	if err != nil {
		return err
	}
	if vectorDriver != nil {
		defer vectorDriver.Close()
	}
	if embedder != nil {
		defer embedder.Close()
	}

	proxyConfig := proxy.Config{
		ListenAddr:   proxyListener.Addr().String(),
		UpstreamURL:  startCfg.DefaultUpstream,
		ProviderType: startCfg.DefaultProvider,
		AgentRoutes: map[string]proxy.AgentRoute{
			agentClaude:   {ProviderType: "anthropic", UpstreamURL: "https://api.anthropic.com"},
			agentOpenCode: {ProviderType: "anthropic", UpstreamURL: "https://api.anthropic.com"},
			agentCodex:    {ProviderType: "openai", UpstreamURL: "https://api.openai.com/v1"},
		},
		ProviderUpstreams: map[string]string{
			"anthropic": "https://api.anthropic.com",
			"openai":    "https://api.openai.com/v1",
			"ollama":    startCfg.OllamaUpstream,
		},
		VectorDriver: vectorDriver,
		Embedder:     embedder,
	}

	//nolint:contextcheck // Proxy lifecycle manages its own background context.
	proxyServer, err := proxy.New(proxyConfig, driver, zapLogger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	defer proxyServer.Close()

	apiConfig := api.Config{
		ListenAddr:   apiListener.Addr().String(),
		VectorDriver: vectorDriver,
		Embedder:     embedder,
	}
	apiServer, err := api.NewServer(apiConfig, driver, dagLoader, zapLogger)
	if err != nil {
		return fmt.Errorf("creating api server: %w", err)
	}
	defer func() { _ = apiServer.Shutdown() }()

	errChan := make(chan error, 2)

	go func() {
		if err := proxyServer.RunWithListener(proxyListener); err != nil {
			errChan <- fmt.Errorf("proxy error: %w", err)
		}
	}()

	go func() {
		if err := apiServer.RunWithListener(apiListener); err != nil {
			errChan <- fmt.Errorf("api error: %w", err)
		}
	}()

	if shutdownWhenIdle {
		go c.monitorIdle(manager, zapLogger, errChan)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		_ = manager.ClearState()
		return err
	case <-sigChan:
		_ = manager.ClearState()
		return nil
	}
}

func (c *startCommander) monitorIdle(manager *start.Manager, zapLogger *zap.Logger, errChan chan<- error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		lock, err := manager.Lock()
		if err != nil {
			zapLogger.Warn("failed to lock state", zap.Error(err))
			continue
		}

		state, err := manager.LoadState()
		if err != nil {
			_ = lock.Release()
			zapLogger.Warn("failed to load state", zap.Error(err))
			continue
		}

		active := filterActiveAgents(state)
		if state != nil {
			state.Agents = active
			_ = manager.SaveState(state)
		}
		_ = lock.Release()

		if state != nil && state.ShutdownWhenIdle && len(active) == 0 {
			errChan <- nil
			return
		}
	}
}

func (c *startCommander) ensureDaemon(ctx context.Context, manager *start.Manager) (*start.State, error) {
	lock, err := manager.Lock()
	if err != nil {
		return nil, err
	}

	state, err := manager.LoadState()
	if err != nil {
		_ = lock.Release()
		return nil, err
	}

	if !stateHealthy(ctx, state) {
		_ = manager.ClearState()
		state = nil
	}

	if err := lock.Release(); err != nil {
		return nil, err
	}

	if state != nil {
		return state, nil
	}

	if err := c.spawnDaemon(ctx, manager); err != nil {
		return nil, err
	}

	return c.waitForDaemon(ctx, manager)
}

func (c *startCommander) spawnDaemon(ctx context.Context, manager *start.Manager) error {
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolving executable: %w", err)
	}

	logFile, err := os.OpenFile(manager.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}

	args := []string{"start", "--daemon"}
	if c.debug {
		args = append(args, "--debug")
	}
	if c.configDir != "" {
		args = append(args, "--config-dir", c.configDir)
	}

	cmd := exec.CommandContext(ctx, execPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("starting daemon: %w", err)
	}
	go func() {
		_ = cmd.Wait()
	}()
	return logFile.Close()
}

func (c *startCommander) waitForDaemon(ctx context.Context, manager *start.Manager) (*start.State, error) {
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, errors.New("timed out waiting for daemon")
		default:
		}

		lock, err := manager.Lock()
		if err != nil {
			return nil, err
		}
		state, err := manager.LoadState()
		_ = lock.Release()
		if err != nil {
			return nil, err
		}
		if state != nil && stateHealthy(ctx, state) {
			return state, nil
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (c *startCommander) registerAgent(manager *start.Manager, name string, pid int) error {
	lock, err := manager.Lock()
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()

	state, err := manager.LoadState()
	if err != nil {
		return err
	}
	if state == nil {
		return errors.New("daemon state missing")
	}

	state.Agents = append(state.Agents, start.AgentSession{
		Name:      name,
		PID:       pid,
		StartedAt: time.Now(),
	})

	return manager.SaveState(state)
}

func (c *startCommander) unregisterAgent(manager *start.Manager, pid int) error {
	lock, err := manager.Lock()
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()

	state, err := manager.LoadState()
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}

	remaining := make([]start.AgentSession, 0, len(state.Agents))
	for _, session := range state.Agents {
		if session.PID != pid {
			remaining = append(remaining, session)
		}
	}
	state.Agents = remaining
	return manager.SaveState(state)
}

func (c *startCommander) loadConfig() (*startConfig, error) {
	cfger, err := config.NewConfiger(c.configDir)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	cfg, err := cfger.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	dotdirManager := dotdir.NewManager()
	defaultTargetDir, err := dotdirManager.Target(c.configDir)
	if err != nil {
		return nil, fmt.Errorf("resolving target dir: %w", err)
	}
	if defaultTargetDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolving home dir: %w", err)
		}
		defaultTargetDir = filepath.Join(home, ".tapes")
		if err := os.MkdirAll(defaultTargetDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating tapes dir: %w", err)
		}
	}
	defaultTargetSqliteFile := filepath.Join(defaultTargetDir, "tapes.sqlite")

	sqlitePath := cfg.Storage.SQLitePath
	if sqlitePath == "" {
		sqlitePath = defaultTargetSqliteFile
	}

	vectorTarget := cfg.VectorStore.Target
	if vectorTarget == "" {
		vectorTarget = defaultTargetSqliteFile
	}

	return &startConfig{
		SQLitePath:          sqlitePath,
		VectorStoreProvider: cfg.VectorStore.Provider,
		VectorStoreTarget:   vectorTarget,
		EmbeddingProvider:   cfg.Embedding.Provider,
		EmbeddingTarget:     cfg.Embedding.Target,
		EmbeddingModel:      cfg.Embedding.Model,
		EmbeddingDimensions: cfg.Embedding.Dimensions,
		DefaultProvider:     cfg.Proxy.Provider,
		DefaultUpstream:     cfg.Proxy.Upstream,
		OllamaUpstream:      resolveOllamaUpstream(cfg.Proxy.Provider, cfg.Proxy.Upstream),
	}, nil
}

func (c *startCommander) newStorageDriver(ctx context.Context, cfg *startConfig, zapLogger *zap.Logger) (storage.Driver, error) {
	if cfg.SQLitePath != "" {
		driver, err := sqlite.NewDriver(ctx, cfg.SQLitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		zapLogger.Info("using SQLite storage", zap.String("path", cfg.SQLitePath))
		return driver, nil
	}

	zapLogger.Info("using in-memory storage")
	return inmemory.NewDriver(), nil
}

func (c *startCommander) newDagLoader(ctx context.Context, cfg *startConfig, zapLogger *zap.Logger, driver storage.Driver) (merkle.DagLoader, error) {
	if driver != nil {
		if loader, ok := driver.(merkle.DagLoader); ok {
			return loader, nil
		}
	}

	if cfg.SQLitePath != "" {
		loader, err := sqlite.NewDriver(ctx, cfg.SQLitePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite storer: %w", err)
		}
		zapLogger.Info("using SQLite storage", zap.String("path", cfg.SQLitePath))
		return loader, nil
	}

	zapLogger.Info("using in-memory storage")
	return inmemory.NewDriver(), nil
}

func (c *startCommander) newVectorAndEmbedder(cfg *startConfig, zapLogger *zap.Logger) (vector.Driver, embeddings.Embedder, error) {
	vectorDriver, err := vectorutils.NewVectorDriver(&vectorutils.NewVectorDriverOpts{
		ProviderType: cfg.VectorStoreProvider,
		Target:       cfg.VectorStoreTarget,
		Dimensions:   cfg.EmbeddingDimensions,
		Logger:       zapLogger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("creating vector driver: %w", err)
	}

	embedder, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
		ProviderType: cfg.EmbeddingProvider,
		TargetURL:    cfg.EmbeddingTarget,
		Model:        cfg.EmbeddingModel,
	})
	if err != nil {
		vectorDriver.Close()
		return nil, nil, fmt.Errorf("creating embedder: %w", err)
	}

	return vectorDriver, embedder, nil
}

func isSupportedAgent(agent string) bool {
	switch agent {
	case agentClaude, agentOpenCode, agentCodex:
		return true
	default:
		return false
	}
}

func agentCommand(agent string) string {
	switch agent {
	case agentClaude:
		return "claude"
	case agentOpenCode:
		return "opencode"
	case agentCodex:
		return "codex"
	default:
		return agent
	}
}

func stateHealthy(ctx context.Context, state *start.State) bool {
	if state == nil || state.DaemonPID == 0 || state.APIURL == "" {
		return false
	}
	if !processAlive(state.DaemonPID) {
		return false
	}
	return apiReachable(ctx, state.APIURL)
}

func filterActiveAgents(state *start.State) []start.AgentSession {
	if state == nil {
		return nil
	}
	active := make([]start.AgentSession, 0, len(state.Agents))
	for _, session := range state.Agents {
		if processAlive(session.PID) {
			active = append(active, session)
		}
	}
	return active
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func apiReachable(ctx context.Context, apiURL string) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	url := strings.TrimRight(apiURL, "/") + "/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func followLog(ctx context.Context, path string, out io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}
	defer file.Close()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("creating log watcher: %w", err)
	}
	defer watcher.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat log file: %w", err)
	}

	if _, err := file.Seek(stat.Size(), io.SeekStart); err != nil {
		return fmt.Errorf("seek log file: %w", err)
	}

	if err := watcher.Add(filepath.Dir(path)); err != nil {
		return fmt.Errorf("watching log dir: %w", err)
	}

	buf := make([]byte, 4096)
	readAvailable := func() error {
		for {
			n, err := file.Read(buf)
			if n > 0 {
				if _, writeErr := out.Write(buf[:n]); writeErr != nil {
					return writeErr
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}

	if err := readAvailable(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-watcher.Events:
			if filepath.Clean(event.Name) != filepath.Clean(path) {
				continue
			}
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}
			if err := readAvailable(); err != nil {
				return err
			}
		case err := <-watcher.Errors:
			return fmt.Errorf("log watcher error: %w", err)
		}
	}
}

func configureOpenCode(baseURL string) (func() error, string, error) {
	configRoot, err := os.MkdirTemp("", "tapes-opencode-config-")
	if err != nil {
		return nil, "", fmt.Errorf("creating opencode config root: %w", err)
	}
	configDir := filepath.Join(configRoot, "opencode")
	configPath := filepath.Join(configDir, "opencode.json")

	if err := os.MkdirAll(configDir, 0o755); err != nil {
		_ = os.RemoveAll(configRoot)
		return nil, "", fmt.Errorf("creating opencode config dir: %w", err)
	}

	config := map[string]any{}
	provider := ensureMap(config, "provider")
	configureOpenCodeProvider(provider, "anthropic", baseURL+"/providers/anthropic")
	configureOpenCodeProvider(provider, "openai", baseURL+"/providers/openai")
	configureOpenCodeProvider(provider, "ollama", baseURL+"/providers/ollama")

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		_ = os.RemoveAll(configRoot)
		return nil, "", fmt.Errorf("marshaling opencode config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		_ = os.RemoveAll(configRoot)
		return nil, "", fmt.Errorf("writing opencode config: %w", err)
	}

	cleanup := func() error {
		if err := os.RemoveAll(configRoot); err != nil {
			return fmt.Errorf("removing opencode config: %w", err)
		}
		return nil
	}

	return cleanup, configRoot, nil
}

func ensureMap(target map[string]any, key string) map[string]any {
	value, ok := target[key]
	if ok {
		if cast, ok := value.(map[string]any); ok {
			return cast
		}
	}

	newMap := map[string]any{}
	target[key] = newMap
	return newMap
}

func configureOpenCodeProvider(provider map[string]any, name, baseURL string) {
	entry := ensureMap(provider, name)
	options := ensureMap(entry, "options")
	options["baseURL"] = baseURL
}

func resolveOllamaUpstream(provider, upstream string) string {
	if env := strings.TrimSpace(os.Getenv("OLLAMA_HOST")); env != "" {
		return env
	}
	if strings.EqualFold(provider, "ollama") && upstream != "" {
		return upstream
	}
	return "http://localhost:11434"
}

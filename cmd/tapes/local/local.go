package localcmder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/dotdir"
)

// verboseDocker controls whether the raw `docker ...` invocations and their
// stdout are echoed. Off by default for a clean bootstrap; enabled by -d/--debug.
var verboseDocker bool

const (
	defaultPostgresImage  = "public.ecr.aws/g4e5l3z3/papercomputeco/postgres:17.7-pgduckdb-1.1.1"
	defaultOllamaImage    = "ollama/ollama:latest"
	defaultEmbeddingModel = "embeddinggemma"
	ollamaEmbeddingModel  = defaultEmbeddingModel + ":latest"
	localNetworkName      = "tapes-local"
	postgresContainer     = "tapes-local-postgres"
	ollamaContainer       = "tapes-local-ollama"
	postgresDirName       = "postgres"
	postgresDataPath      = "/tapes-postgres/data"
	postgresUser          = "tapes"
	postgresPass          = "tapes"
	postgresDB            = "tapes"
)

type localCommander struct {
	configDir     string
	postgresPort  int
	ollamaPort    int
	postgresImage string
	ollamaImage   string
	dockerOllama  bool
}

func NewLocalCmd() *cobra.Command {
	cmder := &localCommander{
		postgresPort:  5432,
		ollamaPort:    11434,
		postgresImage: defaultPostgresImage,
		ollamaImage:   defaultOllamaImage,
	}

	cmd := &cobra.Command{
		Use:   "local",
		Short: "Bootstrap local Postgres + Ollama with Docker",
		Long: `Bootstrap a simple local Docker environment for tapes.

This starts:
  - Postgres for tapes storage + pgvector + pg_duckdb
  - Ollama for local inference/embeddings

When Ollama is already installed on the host, the existing install is used
instead of a Docker container — native Ollama has direct GPU access, which a
containerized Ollama does not on macOS. Pass --docker-ollama to force the
container anyway.

Examples:
  tapes local
  tapes local up
  tapes local status
  tapes local down`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.loadConfigDir(cmd); err != nil {
				return err
			}
			return cmder.runUp()
		},
	}

	// Persistent so the flags work on the subcommands (`tapes local up
	// --postgres-port 5433`), not just the bare `tapes local`.
	cmd.PersistentFlags().IntVar(&cmder.postgresPort, "postgres-port", cmder.postgresPort, "Host port to bind Postgres to")
	cmd.PersistentFlags().IntVar(&cmder.ollamaPort, "ollama-port", cmder.ollamaPort, "Host port to bind Ollama to")
	cmd.PersistentFlags().StringVar(&cmder.postgresImage, "postgres-image", cmder.postgresImage, "Docker image to use for Postgres")
	cmd.PersistentFlags().StringVar(&cmder.ollamaImage, "ollama-image", cmder.ollamaImage, "Docker image to use for Ollama")
	cmd.PersistentFlags().BoolVar(&cmder.dockerOllama, "docker-ollama", cmder.dockerOllama, "Run Ollama in Docker even if a native install is present")

	cmd.AddCommand(&cobra.Command{
		Use:   "up",
		Short: "Start local Postgres + Ollama containers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.loadConfigDir(cmd); err != nil {
				return err
			}
			return cmder.runUp()
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "down",
		Short: "Stop and remove local containers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.loadConfigDir(cmd); err != nil {
				return err
			}
			return cmder.runDown()
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show local container status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.loadConfigDir(cmd); err != nil {
				return err
			}
			return cmder.runStatus()
		},
	})

	return cmd
}

func (c *localCommander) loadConfigDir(cmd *cobra.Command) error {
	configDir, err := cmd.Flags().GetString("config-dir")
	if err != nil {
		return fmt.Errorf("could not get config-dir flag: %w", err)
	}
	c.configDir = configDir
	if debug, derr := cmd.Flags().GetBool("debug"); derr == nil {
		verboseDocker = debug
	}
	return nil
}

func (c *localCommander) runUp() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker is required for 'tapes local'")
	}
	fmt.Printf("  %s %s\n", cliui.SuccessMark, cliui.StepStyle.Render("Docker available"))

	if err := ensureDockerNetwork(localNetworkName); err != nil {
		return err
	}
	if err := ensureDockerImage(c.postgresImage, "Postgres"); err != nil {
		return err
	}

	containerRunning, _, err := containerState(ollamaContainer)
	if err != nil {
		return err
	}
	nativeURL := fmt.Sprintf("http://127.0.0.1:%d", c.ollamaPort)
	_, lookErr := exec.LookPath("ollama")
	plan := planOllama(
		c.dockerOllama,
		containerRunning,
		!containerRunning && ollamaServing(nativeURL),
		lookErr == nil,
	)

	if plan.useDocker {
		if err := ensureDockerImage(c.ollamaImage, "Ollama"); err != nil {
			return err
		}
	}
	postgresDir, err := ensureLocalPostgresDir(c.configDir)
	if err != nil {
		return err
	}
	if err := prepareLocalPostgresDir(postgresDir, c.postgresImage); err != nil {
		return err
	}
	if err := ensurePostgresContainer(c); err != nil {
		return err
	}
	if err := cliui.Step(os.Stdout, "Waiting for Postgres", waitForPostgresReady); err != nil {
		return err
	}

	ollamaURL := nativeURL
	switch {
	case plan.useDocker:
		ollamaURL = fmt.Sprintf("http://localhost:%d", c.ollamaPort)
		if err := ensureOllamaContainer(c); err != nil {
			return err
		}
		if err := cliui.Step(os.Stdout, "Waiting for Ollama", waitForOllamaReady); err != nil {
			return err
		}
		if err := ensureOllamaModel(ollamaEmbeddingModel); err != nil {
			return err
		}
	case plan.needsStart:
		fmt.Printf("  %s %s\n", cliui.WarnStyle.Render("●"), cliui.StepStyle.Render("Ollama is installed but not running — start it (ollama serve, or the Ollama app) and pull the embedding model: ollama pull "+ollamaEmbeddingModel))
	default:
		fmt.Printf("  %s %s\n", cliui.SuccessMark, cliui.StepStyle.Render("Using existing Ollama at "+nativeURL))
		if err := ensureNativeOllamaModel(ollamaEmbeddingModel); err != nil {
			return err
		}
	}

	dsn := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable", postgresUser, postgresPass, c.postgresPort, postgresDB)

	fmt.Printf("\n%s\n", cliui.HeaderStyle.Render("Started local services"))
	fmt.Printf("  %s %s\n", cliui.KeyStyle.Render("Postgres:"), cliui.ValueStyle.Render(dsn))
	fmt.Printf("  %s %s\n", cliui.KeyStyle.Render("Data dir:"), cliui.ValueStyle.Render(postgresDir))
	fmt.Printf("  %s %s\n\n", cliui.KeyStyle.Render("Ollama:  "), cliui.ValueStyle.Render(ollamaURL))

	// Persist the resolved settings so tapes serve, the embed pass, and
	// search all pick up the local Postgres + Ollama without re-specifying
	// them. Non-fatal: on failure, fall back to printing what to set.
	if configPath, err := c.writeLocalConfig(dsn, ollamaURL); err != nil {
		fmt.Printf("  %s could not write config (%v); set these manually:\n", cliui.WarnStyle.Render("●"), err)
		fmt.Printf("    tapes config set embedding.target %q\n\n", ollamaURL)
		fmt.Printf("Next steps:\n")
		fmt.Printf("  1. Run: tapes serve --postgres %q\n", dsn)
	} else {
		fmt.Printf("  %s %s %s\n\n", cliui.SuccessMark, cliui.KeyStyle.Render("Config:"), cliui.ValueStyle.Render(configPath))
		fmt.Printf("Next steps:\n")
		fmt.Printf("  1. Run: tapes serve\n")
	}
	if plan.useDocker {
		fmt.Printf("  2. Optionally pull chat/completion models with: docker exec -it %s ollama pull qwen3-coder:30b\n\n", ollamaContainer)
	} else {
		fmt.Printf("  2. Optionally pull chat/completion models with: ollama pull qwen3-coder:30b\n\n")
	}
	return nil
}

// writeLocalConfig persists the local-dev Postgres + Ollama settings into the
// tapes config so downstream commands (serve, dev embed-spans, search) resolve
// them automatically. It writes to the same .tapes/ directory the local data
// lives in. Returns the config file path on success.
func (c *localCommander) writeLocalConfig(dsn, ollamaURL string) (string, error) {
	tapesDir, err := resolveLocalTapesDir(c.configDir)
	if err != nil {
		return "", err
	}

	cfger, err := config.NewConfiger(tapesDir)
	if err != nil {
		return "", err
	}

	cfg, err := cfger.LoadConfig()
	if err != nil {
		return "", err
	}

	cfg.Storage.PostgresDSN = dsn
	cfg.VectorStore.Target = dsn
	cfg.Proxy.Upstream = ollamaURL
	cfg.Embedding.Provider = "ollama"
	cfg.Embedding.Target = ollamaURL
	cfg.Embedding.Model = defaultEmbeddingModel

	if err := cfger.SaveConfig(cfg); err != nil {
		return "", err
	}

	return cfger.GetTarget(), nil
}

// ollamaPlan describes how `tapes local up` satisfies the Ollama dependency.
type ollamaPlan struct {
	// useDocker provisions (or keeps) the tapes-local-ollama container.
	useDocker bool
	// needsStart is set when a native install was found but nothing answers
	// on the port; the user has to start the server themselves.
	needsStart bool
}

// planOllama prefers an Ollama already on the host over the Docker container.
// Native Ollama has direct GPU access, which a container does not on macOS.
// A container published on the same port also binds the wildcard addresses
// and shadows a loopback-only native server for clients that resolve
// localhost to ::1, so provisioning one next to a native install produces
// silently split traffic.
func planOllama(forceDocker, containerRunning, serving, installed bool) ollamaPlan {
	switch {
	case forceDocker || containerRunning:
		return ollamaPlan{useDocker: true}
	case serving:
		return ollamaPlan{}
	case installed:
		return ollamaPlan{needsStart: true}
	default:
		return ollamaPlan{useDocker: true}
	}
}

// ollamaServing reports whether an Ollama API answers at url.
func ollamaServing(url string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/api/version", nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (c *localCommander) runDown() error {
	for _, name := range []string{postgresContainer, ollamaContainer} {
		_, exists, err := containerState(name)
		if err != nil {
			return err
		}
		if !exists {
			fmt.Printf("  %s %s %s\n", cliui.DimStyle.Render("●"), cliui.NameStyle.Render(name), cliui.DimStyle.Render("not created"))
			continue
		}
		if err := runDocker("rm", "-f", name); err != nil {
			return err
		}
	}
	fmt.Printf("  %s %s\n", cliui.SuccessMark, cliui.StepStyle.Render("Removed local tapes containers"))
	return nil
}

func (c *localCommander) runStatus() error {
	for _, name := range []string{postgresContainer, ollamaContainer} {
		out, err := dockerOutput("ps", "-a", "--filter", "name="+name, "--format", "{{.Names}}\t{{.Status}}")
		if err != nil {
			return err
		}
		status := strings.TrimSpace(out)
		if status == "" {
			if name == ollamaContainer {
				if nativeURL := fmt.Sprintf("http://127.0.0.1:%d", c.ollamaPort); ollamaServing(nativeURL) {
					fmt.Printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render("ollama (native)"), cliui.ValueStyle.Render(nativeURL))
					continue
				}
			}
			fmt.Printf("  %s %s %s\n", cliui.DimStyle.Render("●"), cliui.NameStyle.Render(name), cliui.DimStyle.Render("not created"))
			continue
		}
		parts := strings.SplitN(status, "\t", 2)
		if len(parts) == 2 {
			fmt.Printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render(parts[0]), cliui.ValueStyle.Render(parts[1]))
			continue
		}
		fmt.Println(status)
	}
	return nil
}

func ensureDockerNetwork(name string) error {
	if _, err := dockerOutput("network", "inspect", name); err == nil {
		fmt.Printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render(name), cliui.StepStyle.Render("network exists"))
		return nil
	}
	return runDocker("network", "create", name)
}

func ensureDockerImage(image, label string) error {
	if _, err := dockerOutput("image", "inspect", image); err == nil {
		fmt.Printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render(image), cliui.StepStyle.Render("image exists"))
		return nil
	} else if !isDockerImageNotFoundError(err) {
		return err
	}

	fmt.Printf("  %s %s\n", cliui.WarnStyle.Render("↓"), cliui.StepStyle.Render(label+" image not found locally; pulling"))
	return runDocker("pull", image)
}

func ensurePostgresContainer(c *localCommander) error {
	if running, exists, err := containerState(postgresContainer); err != nil {
		return err
	} else if running {
		return nil
	} else if exists {
		return runDocker("start", postgresContainer)
	}

	postgresDir, err := ensureLocalPostgresDir(c.configDir)
	if err != nil {
		return err
	}

	return runDocker(
		"run", "-d",
		"--name", postgresContainer,
		"--network", localNetworkName,
		"-e", "POSTGRES_USER="+postgresUser,
		"-e", "POSTGRES_PASSWORD="+postgresPass,
		"-e", "POSTGRES_DB="+postgresDB,
		"-e", "PGDATA="+postgresDataPath,
		"-p", strconv.Itoa(c.postgresPort)+":5432",
		"-v", postgresDir+":"+path.Dir(postgresDataPath),
		c.postgresImage,
	)
}

func ensureOllamaContainer(c *localCommander) error {
	if running, exists, err := containerState(ollamaContainer); err != nil {
		return err
	} else if running {
		return nil
	} else if exists {
		return runDocker("start", ollamaContainer)
	}

	ollamaDir, err := ensureLocalOllamaDir()
	if err != nil {
		return err
	}

	return runDocker(
		"run", "-d",
		"--name", ollamaContainer,
		"--network", localNetworkName,
		"-p", strconv.Itoa(c.ollamaPort)+":11434",
		"-v", ollamaDir+":/root/.ollama",
		c.ollamaImage,
	)
}

func ensureLocalPostgresDir(configDir string) (string, error) {
	tapesDir, err := resolveLocalTapesDir(configDir)
	if err != nil {
		return "", err
	}

	postgresDir := filepath.Join(tapesDir, postgresDirName)
	if err := os.MkdirAll(postgresDir, 0o755); err != nil {
		return "", fmt.Errorf("creating postgres directory %q: %w", postgresDir, err)
	}

	return postgresDir, nil
}

func prepareLocalPostgresDir(postgresDir, image string) error {
	cmd := fmt.Sprintf("mkdir -p %s && chown -R 26:26 %s && chmod 0700 %s", postgresDataPath, postgresDataPath, postgresDataPath)
	if err := runDocker(
		"run", "--rm",
		"--user", "0",
		"--entrypoint", "sh",
		"-v", postgresDir+":"+path.Dir(postgresDataPath),
		image,
		"-lc",
		cmd,
	); err != nil {
		return fmt.Errorf("preparing postgres directory %q: %w", postgresDir, err)
	}
	return nil
}

func waitForPostgresReady() error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := dockerOutput("exec", postgresContainer, "pg_isready", "-U", postgresUser, "-d", postgresDB); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("postgres container %q did not become ready within 30s", postgresContainer)
}

func waitForOllamaReady() error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := dockerOutput("exec", ollamaContainer, "ollama", "list"); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("ollama container %q did not become ready within 60s", ollamaContainer)
}

func ensureOllamaModel(model string) error {
	fmt.Printf("  %s %s\n", cliui.WarnStyle.Render("↓"), cliui.StepStyle.Render("Pulling Ollama model "+model))
	return runDocker("exec", ollamaContainer, "ollama", "pull", model)
}

func ensureNativeOllamaModel(model string) error {
	fmt.Printf("  %s %s\n", cliui.WarnStyle.Render("↓"), cliui.StepStyle.Render("Pulling Ollama model "+model))
	cmd := exec.CommandContext(context.Background(), "ollama", "pull", model)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ollama pull %s: %w", model, err)
	}
	return nil
}

func resolveLocalTapesDir(configDir string) (string, error) {
	manager := dotdir.NewManager()
	dir, err := manager.Target(configDir)
	if err != nil {
		return "", fmt.Errorf("resolving tapes directory: %w", err)
	}
	if dir != "" {
		return dir, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locating home directory for tapes data: %w", err)
	}

	dir = filepath.Join(homeDir, ".tapes")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating tapes directory %q: %w", dir, err)
	}

	return dir, nil
}

func ensureLocalOllamaDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locating home directory for ollama models: %w", err)
	}

	ollamaDir := filepath.Join(homeDir, ".ollama")
	if err := os.MkdirAll(ollamaDir, 0o755); err != nil {
		return "", fmt.Errorf("creating ollama directory %q: %w", ollamaDir, err)
	}

	return ollamaDir, nil
}

func containerState(name string) (running bool, exists bool, err error) {
	out, err := dockerOutput("container", "inspect", "-f", "{{.State.Running}}", name)
	if err != nil {
		if isDockerNotFoundError(err) {
			return false, false, nil
		}
		return false, false, err
	}
	return strings.TrimSpace(out) == "true", true, nil
}

func isDockerNotFoundError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such object") || strings.Contains(msg, "no such container")
}

func isDockerImageNotFoundError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such image") || strings.Contains(msg, "no such object")
}

func runDocker(args ...string) error {
	cmd := exec.CommandContext(context.Background(), "docker", args...)
	cmd.Stderr = os.Stderr
	if verboseDocker {
		fmt.Printf("  %s docker %s\n", cliui.DimStyle.Render("$"), cliui.ValueStyle.Render(strings.Join(args, " ")))
		cmd.Stdout = os.Stdout
	} else {
		// Suppress the command echo and its stdout (e.g. detached container
		// IDs) so the bootstrap shows only the ✓ step summary. -d/--debug
		// restores the full output.
		cmd.Stdout = io.Discard
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func dockerOutput(args ...string) (string, error) {
	cmd := exec.CommandContext(context.Background(), "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

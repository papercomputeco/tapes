package localcmder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const (
	defaultPostgresImage = "public.ecr.aws/g4e5l3z3/papercomputeco/postgres"
	defaultOllamaImage   = "ollama/ollama:latest"
	localNetworkName     = "tapes-local"
	postgresContainer    = "tapes-local-postgres"
	ollamaContainer      = "tapes-local-ollama"
	postgresDirName      = "postgres"
	postgresDataPath     = "/tapes-postgres/data"
	postgresUser         = "tapes"
	postgresPass         = "tapes"
	postgresDB           = "tapes"
)

type localCommander struct {
	configDir     string
	postgresPort  int
	ollamaPort    int
	postgresImage string
	ollamaImage   string
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

	cmd.Flags().IntVar(&cmder.postgresPort, "postgres-port", cmder.postgresPort, "Host port to bind Postgres to")
	cmd.Flags().IntVar(&cmder.ollamaPort, "ollama-port", cmder.ollamaPort, "Host port to bind Ollama to")
	cmd.Flags().StringVar(&cmder.postgresImage, "postgres-image", cmder.postgresImage, "Docker image to use for Postgres")
	cmd.Flags().StringVar(&cmder.ollamaImage, "ollama-image", cmder.ollamaImage, "Docker image to use for Ollama")

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
	return nil
}

func (c *localCommander) runUp() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker is required for 'tapes local'")
	}

	if err := ensureDockerNetwork(localNetworkName); err != nil {
		return err
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
	if err := ensureOllamaContainer(c); err != nil {
		return err
	}
	if err := waitForPostgresReady(); err != nil {
		return err
	}

	dsn := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable", postgresUser, postgresPass, c.postgresPort, postgresDB)

	fmt.Printf("\nStarted local services:\n")
	fmt.Printf("  Postgres: %s\n", dsn)
	fmt.Printf("  Data dir: %s\n", postgresDir)
	fmt.Printf("  Ollama:   http://localhost:%d\n\n", c.ollamaPort)
	fmt.Printf("Suggested config:\n")
	fmt.Printf("  tapes config set storage.postgres_dsn %q\n", dsn)
	fmt.Printf("  tapes config set vector_store.provider %q\n", "pgvector")
	fmt.Printf("  tapes config set vector_store.target %q\n", dsn)
	fmt.Printf("  tapes config set proxy.upstream %q\n", fmt.Sprintf("http://localhost:%d", c.ollamaPort))
	fmt.Printf("  tapes config set embedding.provider %q\n", "ollama")
	fmt.Printf("  tapes config set embedding.target %q\n", fmt.Sprintf("http://localhost:%d", c.ollamaPort))
	fmt.Printf("  tapes config set embedding.model %q\n\n", "nomic-embed-text")
	fmt.Printf("Next steps:\n")
	fmt.Printf("  1. Run: tapes serve --postgres %q\n", dsn)
	fmt.Printf("  2. Optionally pull models with: docker exec -it %s ollama pull qwen3-coder:30b\n\n", ollamaContainer)
	return nil
}

func (c *localCommander) runDown() error {
	for _, name := range []string{postgresContainer, ollamaContainer} {
		if err := runDocker("rm", "-f", name); err != nil && !isDockerNotFoundError(err) {
			return err
		}
	}
	fmt.Println("Removed local tapes containers.")
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
			status = name + "\tnot created"
		}
		fmt.Println(status)
	}
	return nil
}

func ensureDockerNetwork(name string) error {
	if err := runDocker("network", "inspect", name); err == nil {
		return nil
	}
	return runDocker("network", "create", name)
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
		if err := runDocker("exec", postgresContainer, "pg_isready", "-U", postgresUser, "-d", postgresDB); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("postgres container %q did not become ready within 30s", postgresContainer)
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

func runDocker(args ...string) error {
	cmd := exec.CommandContext(context.Background(), "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
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

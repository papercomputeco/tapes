// Package local manages the Docker-backed Postgres and Ollama containers
// that tapes uses for local development and zero-config bootstrap.
//
// Up/Down/Status are idempotent and safe to call from both the `tapes local`
// CLI and from other commands (e.g. `tapes deck`) that want to ensure the
// local stack is running before continuing.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const (
	DefaultPostgresImage  = "public.ecr.aws/g4e5l3z3/papercomputeco/postgres:17.7-pgduckdb-1.1.1"
	DefaultOllamaImage    = "ollama/ollama:latest"
	DefaultEmbeddingModel = "embeddinggemma"
	DefaultPostgresPort   = 5432
	DefaultOllamaPort     = 11434

	NetworkName       = "tapes-local"
	PostgresContainer = "tapes-local-postgres"
	OllamaContainer   = "tapes-local-ollama"
	PostgresUser      = "tapes"
	PostgresPass      = "tapes"
	PostgresDB        = "tapes"

	ollamaEmbeddingModel = DefaultEmbeddingModel + ":latest"
	postgresDirName      = "postgres"
	postgresDataPath     = "/tapes-postgres/data"
)

// Options configures a local stack operation. Zero values fall back to the
// package defaults; callers only need to set the fields they care about.
type Options struct {
	ConfigDir     string
	PostgresPort  int
	OllamaPort    int
	PostgresImage string
	OllamaImage   string
	SkipOllama    bool
	Out           io.Writer
}

// PostgresDSN returns the DSN string for a local Postgres listening on port.
func PostgresDSN(port int) string {
	if port == 0 {
		port = DefaultPostgresPort
	}
	return fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable", PostgresUser, PostgresPass, port, PostgresDB)
}

// IsLocalDefaultHost returns true if dsn parses as a postgres URL whose host
// resolves to localhost and whose port matches DefaultPostgresPort. Credentials
// and database name are not compared. Empty DSN returns true so bootstrap
// proceeds normally when the user has not configured anything.
func IsLocalDefaultHost(dsn string) bool {
	if strings.TrimSpace(dsn) == "" {
		return true
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host != "localhost" && host != "127.0.0.1" && host != "::1" {
		return false
	}
	port := u.Port()
	if port == "" {
		port = strconv.Itoa(DefaultPostgresPort)
	}
	return port == strconv.Itoa(DefaultPostgresPort)
}

// HasDocker reports whether the docker CLI is available on PATH.
func HasDocker() bool {
	_, err := exec.LookPath("docker")
	return err == nil
}

// Up brings the local stack up. Pulls images, creates the network, starts the
// Postgres container, and (unless SkipOllama is set) starts the Ollama
// container and pre-pulls the default embedding model.
func Up(ctx context.Context, opts Options) error {
	r := newRunner(opts)

	if !HasDocker() {
		return errors.New("docker is required for tapes local")
	}
	r.printf("  %s %s\n", cliui.SuccessMark, cliui.StepStyle.Render("Docker available"))

	if err := r.ensureNetwork(ctx); err != nil {
		return err
	}
	if err := r.ensureImage(ctx, r.opts.PostgresImage, "Postgres"); err != nil {
		return err
	}
	if !r.opts.SkipOllama {
		if err := r.ensureImage(ctx, r.opts.OllamaImage, "Ollama"); err != nil {
			return err
		}
	}

	postgresDir, err := EnsureLocalPostgresDir(r.opts.ConfigDir)
	if err != nil {
		return err
	}
	if err := r.preparePostgresDir(ctx, postgresDir); err != nil {
		return err
	}
	if err := r.ensurePostgresContainer(ctx, postgresDir); err != nil {
		return err
	}
	if !r.opts.SkipOllama {
		if err := r.ensureOllamaContainer(ctx); err != nil {
			return err
		}
	}

	if err := cliui.Step(r.out, "Waiting for Postgres", func() error { return r.waitForPostgresReady(ctx) }); err != nil {
		return err
	}
	if !r.opts.SkipOllama {
		if err := cliui.Step(r.out, "Waiting for Ollama", func() error { return r.waitForOllamaReady(ctx) }); err != nil {
			return err
		}
		if err := r.ensureOllamaModel(ctx, ollamaEmbeddingModel); err != nil {
			return err
		}
	}

	return nil
}

// PrintStartedSummary writes a human-readable "started services" summary
// pointing at the configured ports. Callers that want to suppress this can
// just skip calling it.
func PrintStartedSummary(out io.Writer, opts Options) {
	if opts.PostgresPort == 0 {
		opts.PostgresPort = DefaultPostgresPort
	}
	if opts.OllamaPort == 0 {
		opts.OllamaPort = DefaultOllamaPort
	}
	if out == nil {
		out = os.Stdout
	}

	dsn := PostgresDSN(opts.PostgresPort)
	postgresDir, _ := EnsureLocalPostgresDir(opts.ConfigDir)

	fmt.Fprintf(out, "\n%s\n", cliui.HeaderStyle.Render("Started local services"))
	fmt.Fprintf(out, "  %s %s\n", cliui.KeyStyle.Render("Postgres:"), cliui.ValueStyle.Render(dsn))
	if postgresDir != "" {
		fmt.Fprintf(out, "  %s %s\n", cliui.KeyStyle.Render("Data dir:"), cliui.ValueStyle.Render(postgresDir))
	}
	if !opts.SkipOllama {
		fmt.Fprintf(out, "  %s %s\n", cliui.KeyStyle.Render("Ollama:  "), cliui.ValueStyle.Render(fmt.Sprintf("http://localhost:%d", opts.OllamaPort)))
	}
	fmt.Fprintln(out)
}

// Down stops and removes the local containers. Containers that don't exist
// are reported as "not created" and skipped.
func Down(ctx context.Context, opts Options) error {
	r := newRunner(opts)
	for _, name := range []string{PostgresContainer, OllamaContainer} {
		_, exists, err := r.containerState(ctx, name)
		if err != nil {
			return err
		}
		if !exists {
			r.printf("  %s %s %s\n", cliui.DimStyle.Render("●"), cliui.NameStyle.Render(name), cliui.DimStyle.Render("not created"))
			continue
		}
		if err := r.docker(ctx, "rm", "-f", name); err != nil {
			return err
		}
	}
	r.printf("  %s %s\n", cliui.SuccessMark, cliui.StepStyle.Render("Removed local tapes containers"))
	return nil
}

// Status writes the current container statuses to opts.Out.
func Status(ctx context.Context, opts Options) error {
	r := newRunner(opts)
	for _, name := range []string{PostgresContainer, OllamaContainer} {
		out, err := r.dockerOutput(ctx, "ps", "-a", "--filter", "name="+name, "--format", "{{.Names}}\t{{.Status}}")
		if err != nil {
			return err
		}
		status := strings.TrimSpace(out)
		if status == "" {
			r.printf("  %s %s %s\n", cliui.DimStyle.Render("●"), cliui.NameStyle.Render(name), cliui.DimStyle.Render("not created"))
			continue
		}
		parts := strings.SplitN(status, "\t", 2)
		if len(parts) == 2 {
			r.printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render(parts[0]), cliui.ValueStyle.Render(parts[1]))
			continue
		}
		fmt.Fprintln(r.out, status)
	}
	return nil
}

// EnsureLocalPostgresDir resolves the tapes data directory and ensures the
// postgres subdirectory exists. Exported so callers (CLI and bootstrap) can
// reference the data path without re-implementing the resolution logic.
func EnsureLocalPostgresDir(configDir string) (string, error) {
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

type runner struct {
	opts Options
	out  io.Writer
}

func newRunner(opts Options) *runner {
	if opts.PostgresPort == 0 {
		opts.PostgresPort = DefaultPostgresPort
	}
	if opts.OllamaPort == 0 {
		opts.OllamaPort = DefaultOllamaPort
	}
	if opts.PostgresImage == "" {
		opts.PostgresImage = DefaultPostgresImage
	}
	if opts.OllamaImage == "" {
		opts.OllamaImage = DefaultOllamaImage
	}
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}
	return &runner{opts: opts, out: out}
}

func (r *runner) printf(format string, a ...any) {
	fmt.Fprintf(r.out, format, a...)
}

func (r *runner) ensureNetwork(ctx context.Context) error {
	if _, err := r.dockerOutput(ctx, "network", "inspect", NetworkName); err == nil {
		r.printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render(NetworkName), cliui.StepStyle.Render("network exists"))
		return nil
	}
	return r.docker(ctx, "network", "create", NetworkName)
}

func (r *runner) ensureImage(ctx context.Context, image, label string) error {
	if _, err := r.dockerOutput(ctx, "image", "inspect", image); err == nil {
		r.printf("  %s %s %s\n", cliui.SuccessMark, cliui.NameStyle.Render(image), cliui.StepStyle.Render("image exists"))
		return nil
	} else if !isDockerImageNotFoundError(err) {
		return err
	}
	r.printf("  %s %s\n", cliui.WarnStyle.Render("↓"), cliui.StepStyle.Render(label+" image not found locally; pulling"))
	return r.docker(ctx, "pull", image)
}

func (r *runner) preparePostgresDir(ctx context.Context, postgresDir string) error {
	cmd := fmt.Sprintf("mkdir -p %s && chown -R 26:26 %s && chmod 0700 %s", postgresDataPath, postgresDataPath, postgresDataPath)
	if err := r.docker(ctx,
		"run", "--rm",
		"--user", "0",
		"--entrypoint", "sh",
		"-v", postgresDir+":"+path.Dir(postgresDataPath),
		r.opts.PostgresImage,
		"-lc",
		cmd,
	); err != nil {
		return fmt.Errorf("preparing postgres directory %q: %w", postgresDir, err)
	}
	return nil
}

func (r *runner) ensurePostgresContainer(ctx context.Context, postgresDir string) error {
	if running, exists, err := r.containerState(ctx, PostgresContainer); err != nil {
		return err
	} else if running {
		return nil
	} else if exists {
		return r.docker(ctx, "start", PostgresContainer)
	}
	return r.docker(ctx,
		"run", "-d",
		"--name", PostgresContainer,
		"--network", NetworkName,
		"-e", "POSTGRES_USER="+PostgresUser,
		"-e", "POSTGRES_PASSWORD="+PostgresPass,
		"-e", "POSTGRES_DB="+PostgresDB,
		"-e", "PGDATA="+postgresDataPath,
		"-p", strconv.Itoa(r.opts.PostgresPort)+":5432",
		"-v", postgresDir+":"+path.Dir(postgresDataPath),
		r.opts.PostgresImage,
	)
}

func (r *runner) ensureOllamaContainer(ctx context.Context) error {
	if running, exists, err := r.containerState(ctx, OllamaContainer); err != nil {
		return err
	} else if running {
		return nil
	} else if exists {
		return r.docker(ctx, "start", OllamaContainer)
	}
	ollamaDir, err := ensureLocalOllamaDir()
	if err != nil {
		return err
	}
	return r.docker(ctx,
		"run", "-d",
		"--name", OllamaContainer,
		"--network", NetworkName,
		"-p", strconv.Itoa(r.opts.OllamaPort)+":11434",
		"-v", ollamaDir+":/root/.ollama",
		r.opts.OllamaImage,
	)
}

func (r *runner) waitForPostgresReady(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := r.dockerOutput(ctx, "exec", PostgresContainer, "pg_isready", "-U", PostgresUser, "-d", PostgresDB); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("postgres container %q did not become ready within 30s", PostgresContainer)
}

func (r *runner) waitForOllamaReady(ctx context.Context) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := r.dockerOutput(ctx, "exec", OllamaContainer, "ollama", "list"); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("ollama container %q did not become ready within 60s", OllamaContainer)
}

func (r *runner) ensureOllamaModel(ctx context.Context, model string) error {
	r.printf("  %s %s\n", cliui.WarnStyle.Render("↓"), cliui.StepStyle.Render("Pulling Ollama model "+model))
	return r.docker(ctx, "exec", OllamaContainer, "ollama", "pull", model)
}

func (r *runner) containerState(ctx context.Context, name string) (running bool, exists bool, err error) {
	out, err := r.dockerOutput(ctx, "container", "inspect", "-f", "{{.State.Running}}", name)
	if err != nil {
		if isDockerNotFoundError(err) {
			return false, false, nil
		}
		return false, false, err
	}
	return strings.TrimSpace(out) == "true", true, nil
}

func (r *runner) docker(ctx context.Context, args ...string) error {
	r.printf("  %s docker %s\n", cliui.DimStyle.Render("$"), cliui.ValueStyle.Render(strings.Join(args, " ")))
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = r.out
	cmd.Stderr = r.out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func (r *runner) dockerOutput(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func isDockerNotFoundError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such object") || strings.Contains(msg, "no such container")
}

func isDockerImageNotFoundError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such image") || strings.Contains(msg, "no such object")
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

// Package localcmder wires the `tapes local` cobra command to the
// pkg/local Docker bootstrap helpers.
package localcmder

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/local"
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
		postgresPort:  local.DefaultPostgresPort,
		ollamaPort:    local.DefaultOllamaPort,
		postgresImage: local.DefaultPostgresImage,
		ollamaImage:   local.DefaultOllamaImage,
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
			return cmder.runUp(cmd)
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
			return cmder.runUp(cmd)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "down",
		Short: "Stop and remove local containers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.loadConfigDir(cmd); err != nil {
				return err
			}
			return local.Down(cmd.Context(), cmder.toOptions())
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show local container status",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.loadConfigDir(cmd); err != nil {
				return err
			}
			return local.Status(cmd.Context(), cmder.toOptions())
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

func (c *localCommander) toOptions() local.Options {
	return local.Options{
		ConfigDir:     c.configDir,
		PostgresPort:  c.postgresPort,
		OllamaPort:    c.ollamaPort,
		PostgresImage: c.postgresImage,
		OllamaImage:   c.ollamaImage,
		Out:           os.Stdout,
	}
}

func (c *localCommander) runUp(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	opts := c.toOptions()
	opts.Out = out
	if err := local.Up(cmd.Context(), opts); err != nil {
		return err
	}
	local.PrintStartedSummary(out, opts)

	dsn := local.PostgresDSN(c.postgresPort)
	ollamaURL := fmt.Sprintf("http://localhost:%d", c.ollamaPort)

	cfger, err := config.NewConfiger(c.configDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	settings := []struct{ key, value string }{
		{"storage.postgres_dsn", dsn},
		{"vector_store.provider", "pgvector"},
		{"vector_store.target", dsn},
		{"proxy.upstream", ollamaURL},
		{"embedding.provider", "ollama"},
		{"embedding.target", ollamaURL},
		{"embedding.model", local.DefaultEmbeddingModel},
	}

	fmt.Fprintf(out, "%s\n", cliui.HeaderStyle.Render("Applied config"))
	if target := cfger.GetTarget(); target != "" {
		fmt.Fprintf(out, "  %s %s\n", cliui.KeyStyle.Render("Config file:"), cliui.DimStyle.Render(target))
	}
	for _, s := range settings {
		if err := cfger.SetConfigValue(s.key, s.value); err != nil {
			return fmt.Errorf("setting %s: %w", s.key, err)
		}
		fmt.Fprintf(out, "  %s %s = %s\n", cliui.SuccessMark, cliui.KeyStyle.Render(s.key), cliui.ValueStyle.Render(s.value))
	}
	fmt.Fprintln(out)
	fmt.Fprintf(out, "Next steps:\n")
	fmt.Fprintf(out, "  1. Run: tapes deck\n")
	fmt.Fprintf(out, "  2. Optionally pull chat/completion models with: docker exec -it %s ollama pull qwen3-coder:30b\n\n", local.OllamaContainer)
	return nil
}

// Package initcmder provides the init command for initializing a local .tapes
// directory in the current working directory.
package initcmder

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
)

const (
	dirName = ".tapes"
)

const initLongDesc string = `Initialize a new .tapes/ directory in the current working directory.

Creates a local .tapes/ directory that takes precedence over the default
~/.tapes/ directory for checkout state, storage, configuration,
and other tapes operations.

A config.toml file is created with default configuration values.
Use --preset to initialize with a provider preset or a remote config URL.

Available presets: openai, anthropic, ollama

Examples:
  tapes init
  tapes init --preset openai
  tapes init --preset anthropic
  tapes init --preset ollama
  tapes init --preset https://example.com/config.toml`

const initShortDesc string = "Initialize a local .tapes/ directory"

func NewInitCmd() *cobra.Command {
	var preset string

	cmd := &cobra.Command{
		Use:   "init",
		Short: initShortDesc,
		Long:  initLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			return runInit(preset, configDir)
		},
	}

	cmd.Flags().StringVar(&preset, "preset", "", "Provider preset (openai, anthropic, ollama) or URL to a raw config.toml")

	return cmd
}

func runInit(preset, configDir string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting current directory: %w", err)
	}

	dir := filepath.Join(cwd, dirName)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating .tapes directory: %w", err)
	}

	configPath := filepath.Join(dir, "config.toml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := os.WriteFile(configPath, []byte{}, 0o600); err != nil {
			return fmt.Errorf("creating config.toml: %w", err)
		}
	}

	// Resolve the config to write.
	cfg, err := resolveConfig(preset)
	if err != nil {
		return err
	}

	// Save the config into the .tapes/ directory.
	cfger, err := config.NewConfiger(configDir)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if err := cfger.SaveConfig(cfg); err != nil {
		return fmt.Errorf("writing config.toml: %w", err)
	}

	configLabel := "Default configuration"
	if preset != "" {
		configLabel = "Configuration"
	}

	fmt.Printf("\n  %s %s written: %s\n\n",
		cliui.SuccessMark,
		configLabel,
		cliui.DimStyle.Render(filepath.Join(dir, "config.toml")),
	)

	return nil
}

// resolveConfig determines the Config to use based on the --preset flag value.
// If empty, returns a default config. If a known preset name, returns the preset.
// If a URL (starts with http:// or https://), fetches and parses the remote TOML.
func resolveConfig(preset string) (*config.Config, error) {
	if preset == "" {
		return config.NewDefaultConfig(), nil
	}

	// Check if it's a URL.
	if strings.HasPrefix(preset, "http://") || strings.HasPrefix(preset, "https://") {
		return fetchRemoteConfig(preset)
	}

	// Otherwise treat it as a preset name.
	return config.PresetConfig(preset)
}

// fetchRemoteConfig downloads a config.toml from the given URL and parses it.
func fetchRemoteConfig(url string) (*config.Config, error) {
	resp, err := http.Get(url) //nolint:gosec,noctx // User-provided URL is intentional.
	if err != nil {
		return nil, fmt.Errorf("fetching remote config from %q: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching remote config from %q: HTTP %d", url, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading remote config from %q: %w", url, err)
	}

	cfg, err := config.ParseConfigTOML(data)
	if err != nil {
		return nil, fmt.Errorf("parsing remote config from %q: %w", url, err)
	}

	return cfg, nil
}

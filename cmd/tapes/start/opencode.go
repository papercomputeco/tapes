package startcmder

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/papercomputeco/tapes/pkg/config"
)

// openCodeProvider defines a supported provider with its default model.
type openCodeProvider struct {
	Name         string
	DefaultModel string
}

// openCodeProviders is the catalog of providers available for opencode.
var openCodeProviders = []openCodeProvider{
	{Name: "anthropic", DefaultModel: "claude-sonnet-4-5"},
	{Name: "openai", DefaultModel: "gpt-5.2-codex"},
	{Name: "ollama", DefaultModel: "glm-4.7-flash"},
}

// openCodeDefaultProvider returns the default provider name.
func openCodeDefaultProvider() string {
	return openCodeProviders[0].Name
}

// openCodeDefaultModel returns the default model for the given provider.
// Falls back to the first provider's default if the provider is unknown.
func openCodeDefaultModel(provider string) string {
	for _, p := range openCodeProviders {
		if strings.EqualFold(p.Name, provider) {
			return p.DefaultModel
		}
	}
	return openCodeProviders[0].DefaultModel
}

// isValidOpenCodeProvider returns true if the given name matches a known provider.
func isValidOpenCodeProvider(name string) bool {
	for _, p := range openCodeProviders {
		if strings.EqualFold(p.Name, name) {
			return true
		}
	}
	return false
}

// openCodePreference holds the resolved provider and model for an opencode session.
type openCodePreference struct {
	Provider string
	Model    string
}

// resolveOpenCodePreference determines the provider and model to use for opencode.
// Priority: CLI flags > saved config > interactive prompt > defaults.
// When configDir is non-empty and a new selection is made, it is persisted.
func resolveOpenCodePreference(configDir, flagProvider, flagModel string, stdin io.Reader, stdout io.Writer) (*openCodePreference, error) {
	// 1. CLI flags take highest priority.
	if flagProvider != "" {
		provider := strings.ToLower(flagProvider)
		if !isValidOpenCodeProvider(provider) {
			return nil, fmt.Errorf("unsupported opencode provider: %q (available: anthropic, openai, ollama)", flagProvider)
		}
		model := flagModel
		if model == "" {
			model = openCodeDefaultModel(provider)
		}
		if err := persistOpenCodePreference(configDir, provider, model); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not save opencode preference: %v\n", err)
		}
		return &openCodePreference{Provider: provider, Model: model}, nil
	}

	// 2. Saved config values.
	cfger, err := config.NewConfiger(configDir)
	if err == nil {
		cfg, err := cfger.LoadConfig()
		if err == nil && cfg.OpenCode.Provider != "" {
			provider := cfg.OpenCode.Provider
			model := cfg.OpenCode.Model
			if model == "" {
				model = openCodeDefaultModel(provider)
			}
			// Allow flag-based model override even with saved provider.
			if flagModel != "" {
				model = flagModel
			}
			return &openCodePreference{Provider: provider, Model: model}, nil
		}
	}

	// 3. Interactive prompt (only if stdin is a terminal).
	if isTerminal(stdin) {
		pref, err := promptOpenCodePreference(stdin, stdout)
		if err != nil {
			return nil, fmt.Errorf("opencode provider prompt: %w", err)
		}
		if err := persistOpenCodePreference(configDir, pref.Provider, pref.Model); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not save opencode preference: %v\n", err)
		}
		return pref, nil
	}

	// 4. Defaults.
	provider := openCodeDefaultProvider()
	model := openCodeDefaultModel(provider)
	return &openCodePreference{Provider: provider, Model: model}, nil
}

// promptOpenCodePreference runs an interactive provider/model selection.
func promptOpenCodePreference(stdin io.Reader, stdout io.Writer) (*openCodePreference, error) {
	reader := bufio.NewReader(stdin)

	fmt.Fprintln(stdout, "Select a provider for opencode:")
	for i, p := range openCodeProviders {
		fmt.Fprintf(stdout, "  %d) %s\n", i+1, p.Name)
	}
	fmt.Fprintf(stdout, "Choice [1]: ")

	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading provider choice: %w", err)
	}
	line = strings.TrimSpace(line)

	idx := 0 // default to first provider
	if line != "" {
		n := 0
		if _, err := fmt.Sscanf(line, "%d", &n); err != nil || n < 1 || n > len(openCodeProviders) {
			return nil, fmt.Errorf("invalid choice: %q", line)
		}
		idx = n - 1
	}

	selected := openCodeProviders[idx]
	defaultModel := selected.DefaultModel

	fmt.Fprintf(stdout, "Enter model (default: %s): ", defaultModel)
	modelLine, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading model: %w", err)
	}
	modelLine = strings.TrimSpace(modelLine)
	if modelLine == "" {
		modelLine = defaultModel
	}

	return &openCodePreference{Provider: selected.Name, Model: modelLine}, nil
}

// persistOpenCodePreference saves the provider and model to config.toml.
func persistOpenCodePreference(configDir, provider, model string) error {
	cfger, err := config.NewConfiger(configDir)
	if err != nil {
		return err
	}

	if err := cfger.SetConfigValue("opencode.provider", provider); err != nil {
		return err
	}
	return cfger.SetConfigValue("opencode.model", model)
}

// isTerminal checks if the given reader is connected to a terminal.
func isTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

package startcmder

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/credentials"
)

var _ = Describe("opencode provider catalog", func() {
	Describe("openCodeDefaultProvider", func() {
		It("returns anthropic as the default", func() {
			Expect(openCodeDefaultProvider()).To(Equal("anthropic"))
		})
	})

	Describe("openCodeDefaultModel", func() {
		It("returns the correct default model for each provider", func() {
			Expect(openCodeDefaultModel("anthropic")).To(Equal("claude-sonnet-4-5"))
			Expect(openCodeDefaultModel("openai")).To(Equal("gpt-5.2-codex"))
			Expect(openCodeDefaultModel("ollama")).To(Equal("glm-4.7-flash"))
		})

		It("is case-insensitive", func() {
			Expect(openCodeDefaultModel("Anthropic")).To(Equal("claude-sonnet-4-5"))
			Expect(openCodeDefaultModel("OPENAI")).To(Equal("gpt-5.2-codex"))
		})

		It("falls back to first provider default for unknown providers", func() {
			Expect(openCodeDefaultModel("unknown")).To(Equal("claude-sonnet-4-5"))
		})
	})

	Describe("isValidOpenCodeProvider", func() {
		It("returns true for known providers", func() {
			Expect(isValidOpenCodeProvider("anthropic")).To(BeTrue())
			Expect(isValidOpenCodeProvider("openai")).To(BeTrue())
			Expect(isValidOpenCodeProvider("ollama")).To(BeTrue())
		})

		It("is case-insensitive", func() {
			Expect(isValidOpenCodeProvider("Anthropic")).To(BeTrue())
			Expect(isValidOpenCodeProvider("OPENAI")).To(BeTrue())
		})

		It("returns false for unknown providers", func() {
			Expect(isValidOpenCodeProvider("unknown")).To(BeFalse())
			Expect(isValidOpenCodeProvider("")).To(BeFalse())
		})
	})
})

var _ = Describe("resolveOpenCodePreference", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-opencode-pref-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	It("uses CLI flags with highest priority", func() {
		pref, err := resolveOpenCodePreference(tmpDir, "openai", "gpt-4o", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("openai"))
		Expect(pref.Model).To(Equal("gpt-4o"))
	})

	It("uses default model when only provider flag is set", func() {
		pref, err := resolveOpenCodePreference(tmpDir, "ollama", "", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("ollama"))
		Expect(pref.Model).To(Equal("glm-4.7-flash"))
	})

	It("returns error for invalid provider flag", func() {
		_, err := resolveOpenCodePreference(tmpDir, "invalid", "", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported opencode provider"))
	})

	It("uses saved config when no flags provided", func() {
		cfger, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfger.SetConfigValue("opencode.provider", "openai")).To(Succeed())
		Expect(cfger.SetConfigValue("opencode.model", "gpt-4o")).To(Succeed())

		pref, err := resolveOpenCodePreference(tmpDir, "", "", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("openai"))
		Expect(pref.Model).To(Equal("gpt-4o"))
	})

	It("uses default model when config has provider but no model", func() {
		cfger, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfger.SetConfigValue("opencode.provider", "ollama")).To(Succeed())

		pref, err := resolveOpenCodePreference(tmpDir, "", "", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("ollama"))
		Expect(pref.Model).To(Equal("glm-4.7-flash"))
	})

	It("allows model flag to override saved config model", func() {
		cfger, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfger.SetConfigValue("opencode.provider", "anthropic")).To(Succeed())
		Expect(cfger.SetConfigValue("opencode.model", "claude-sonnet-4-5")).To(Succeed())

		pref, err := resolveOpenCodePreference(tmpDir, "", "claude-opus-4-6", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("anthropic"))
		Expect(pref.Model).To(Equal("claude-opus-4-6"))
	})

	It("falls back to defaults when no config, no flags, and non-TTY stdin", func() {
		pref, err := resolveOpenCodePreference(tmpDir, "", "", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("anthropic"))
		Expect(pref.Model).To(Equal("claude-sonnet-4-5"))
	})

	It("persists CLI flag selection to config", func() {
		pref, err := resolveOpenCodePreference(tmpDir, "ollama", "codellama:7b", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("ollama"))
		Expect(pref.Model).To(Equal("codellama:7b"))

		cfger, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		cfg, err := cfger.LoadConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.OpenCode.Provider).To(Equal("ollama"))
		Expect(cfg.OpenCode.Model).To(Equal("codellama:7b"))
	})

	It("normalizes provider to lowercase", func() {
		pref, err := resolveOpenCodePreference(tmpDir, "ANTHROPIC", "", strings.NewReader(""), &bytes.Buffer{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("anthropic"))
	})
})

var _ = Describe("promptOpenCodePreference", func() {
	It("accepts default provider and default model on empty input", func() {
		input := strings.NewReader("\n\n")
		output := &bytes.Buffer{}
		pref, err := promptOpenCodePreference(input, output)
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("anthropic"))
		Expect(pref.Model).To(Equal("claude-sonnet-4-5"))
	})

	It("selects provider 2 (openai) and custom model", func() {
		input := strings.NewReader("2\nmy-custom-model\n")
		output := &bytes.Buffer{}
		pref, err := promptOpenCodePreference(input, output)
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("openai"))
		Expect(pref.Model).To(Equal("my-custom-model"))
	})

	It("selects provider 3 (ollama) with default model", func() {
		input := strings.NewReader("3\n\n")
		output := &bytes.Buffer{}
		pref, err := promptOpenCodePreference(input, output)
		Expect(err).NotTo(HaveOccurred())
		Expect(pref.Provider).To(Equal("ollama"))
		Expect(pref.Model).To(Equal("glm-4.7-flash"))
	})

	It("returns error for invalid choice", func() {
		input := strings.NewReader("99\n")
		output := &bytes.Buffer{}
		_, err := promptOpenCodePreference(input, output)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid choice"))
	})

	It("returns error for non-numeric choice", func() {
		input := strings.NewReader("abc\n")
		output := &bytes.Buffer{}
		_, err := promptOpenCodePreference(input, output)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("invalid choice"))
	})

	It("displays the prompt correctly", func() {
		input := strings.NewReader("1\n\n")
		output := &bytes.Buffer{}
		_, err := promptOpenCodePreference(input, output)
		Expect(err).NotTo(HaveOccurred())
		Expect(output.String()).To(ContainSubstring("Select a provider for opencode:"))
		Expect(output.String()).To(ContainSubstring("1) anthropic"))
		Expect(output.String()).To(ContainSubstring("2) openai"))
		Expect(output.String()).To(ContainSubstring("3) ollama"))
		Expect(output.String()).To(ContainSubstring("Enter model"))
	})
})

var _ = Describe("configureOpenCode config merge", func() {
	var (
		tmpXDG      string
		tmpTapesDir string
		origXDG     string
		hasOrigXDG  bool
	)

	BeforeEach(func() {
		var err error
		tmpXDG, err = os.MkdirTemp("", "tapes-opencode-xdg-*")
		Expect(err).NotTo(HaveOccurred())

		tmpTapesDir, err = os.MkdirTemp("", "tapes-opencode-creds-*")
		Expect(err).NotTo(HaveOccurred())

		origXDG, hasOrigXDG = os.LookupEnv("XDG_CONFIG_HOME")
		os.Setenv("XDG_CONFIG_HOME", tmpXDG)
	})

	AfterEach(func() {
		if hasOrigXDG {
			os.Setenv("XDG_CONFIG_HOME", origXDG)
		} else {
			os.Unsetenv("XDG_CONFIG_HOME")
		}
		os.RemoveAll(tmpXDG)
		os.RemoveAll(tmpTapesDir)
	})

	It("merges proxy URLs into existing user config", func() {
		// Write a user config with custom plugins.
		userConfigDir := filepath.Join(tmpXDG, "opencode")
		Expect(os.MkdirAll(userConfigDir, 0o755)).To(Succeed())

		userCfg := map[string]any{
			"mcpServers": map[string]any{
				"my-server": map[string]any{
					"command": "my-mcp-server",
				},
			},
			"provider": map[string]any{
				"ollama": map[string]any{
					"npm":  "ollama",
					"name": "Ollama",
					"models": map[string]any{
						"glm-4.7-flash": map[string]any{
							"name": "Qwen3 Coder 30B",
						},
					},
				},
			},
		}
		data, err := json.MarshalIndent(userCfg, "", "  ")
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(filepath.Join(userConfigDir, "opencode.json"), data, 0o600)).To(Succeed())

		cleanup, configRoot, err := configureOpenCode("http://localhost:9999", tmpTapesDir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = cleanup() })

		// Read the merged config.
		mergedData, err := os.ReadFile(filepath.Join(configRoot, "opencode", "opencode.json"))
		Expect(err).NotTo(HaveOccurred())

		var merged map[string]any
		Expect(json.Unmarshal(mergedData, &merged)).To(Succeed())

		// User's mcpServers should be preserved.
		Expect(merged).To(HaveKey("mcpServers"))

		// Provider section should have proxy base URLs merged in.
		providerMap, ok := merged["provider"].(map[string]any)
		Expect(ok).To(BeTrue())

		// All three providers should be present.
		Expect(providerMap).To(HaveKey("anthropic"))
		Expect(providerMap).To(HaveKey("openai"))
		Expect(providerMap).To(HaveKey("ollama"))

		// Ollama should retain original fields.
		ollamaEntry, ok := providerMap["ollama"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(ollamaEntry).To(HaveKey("npm"))
		Expect(ollamaEntry).To(HaveKey("models"))

		// Ollama options.baseURL should include /v1 for @ai-sdk/openai-compatible.
		ollamaOpts, ok := ollamaEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(ollamaOpts["baseURL"]).To(Equal("http://localhost:9999/providers/ollama/v1"))

		// Anthropic should include /v1 — the adapter appends only /messages.
		anthropicEntry, ok := providerMap["anthropic"].(map[string]any)
		Expect(ok).To(BeTrue())
		anthropicOpts, ok := anthropicEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(anthropicOpts["baseURL"]).To(Equal("http://localhost:9999/providers/anthropic/v1"))

		// OpenAI should NOT include /v1 — the proxy upstream already has it.
		openaiEntry, ok := providerMap["openai"].(map[string]any)
		Expect(ok).To(BeTrue())
		openaiOpts, ok := openaiEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(openaiOpts["baseURL"]).To(Equal("http://localhost:9999/providers/openai"))
	})

	It("creates config from scratch when no user config exists", func() {
		// tmpXDG is empty, no opencode config exists.
		cleanup, configRoot, err := configureOpenCode("http://localhost:8888", tmpTapesDir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = cleanup() })

		mergedData, err := os.ReadFile(filepath.Join(configRoot, "opencode", "opencode.json"))
		Expect(err).NotTo(HaveOccurred())

		var merged map[string]any
		Expect(json.Unmarshal(mergedData, &merged)).To(Succeed())

		providerMap, ok := merged["provider"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(providerMap).To(HaveKey("anthropic"))
		Expect(providerMap).To(HaveKey("openai"))
		Expect(providerMap).To(HaveKey("ollama"))
	})

	It("cleanup removes temp directory", func() {
		cleanup, configRoot, err := configureOpenCode("http://localhost:7777", tmpTapesDir)
		Expect(err).NotTo(HaveOccurred())

		Expect(configRoot).To(BeADirectory())
		Expect(cleanup()).To(Succeed())
		Expect(configRoot).NotTo(BeADirectory())
	})

	It("injects stored API keys into provider options", func() {
		mgr, err := credentials.NewManager(tmpTapesDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SetKey("openai", "sk-test-openai-key")).To(Succeed())
		Expect(mgr.SetKey("anthropic", "sk-test-anthropic-key")).To(Succeed())

		cleanup, configRoot, err := configureOpenCode("http://localhost:6666", tmpTapesDir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = cleanup() })

		mergedData, err := os.ReadFile(filepath.Join(configRoot, "opencode", "opencode.json"))
		Expect(err).NotTo(HaveOccurred())

		var merged map[string]any
		Expect(json.Unmarshal(mergedData, &merged)).To(Succeed())

		providerMap, ok := merged["provider"].(map[string]any)
		Expect(ok).To(BeTrue())

		// OpenAI should have apiKey injected.
		openaiEntry, ok := providerMap["openai"].(map[string]any)
		Expect(ok).To(BeTrue())
		openaiOpts, ok := openaiEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(openaiOpts["apiKey"]).To(Equal("sk-test-openai-key"))

		// Anthropic should have apiKey injected.
		anthropicEntry, ok := providerMap["anthropic"].(map[string]any)
		Expect(ok).To(BeTrue())
		anthropicOpts, ok := anthropicEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(anthropicOpts["apiKey"]).To(Equal("sk-test-anthropic-key"))

		// Ollama should NOT have apiKey (it doesn't use one).
		ollamaEntry, ok := providerMap["ollama"].(map[string]any)
		Expect(ok).To(BeTrue())
		ollamaOpts, ok := ollamaEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(ollamaOpts).NotTo(HaveKey("apiKey"))
	})

	It("works without stored credentials", func() {
		cleanup, configRoot, err := configureOpenCode("http://localhost:5555", tmpTapesDir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = cleanup() })

		mergedData, err := os.ReadFile(filepath.Join(configRoot, "opencode", "opencode.json"))
		Expect(err).NotTo(HaveOccurred())

		var merged map[string]any
		Expect(json.Unmarshal(mergedData, &merged)).To(Succeed())

		providerMap, ok := merged["provider"].(map[string]any)
		Expect(ok).To(BeTrue())

		// No apiKey should be present when no credentials are stored.
		openaiEntry, ok := providerMap["openai"].(map[string]any)
		Expect(ok).To(BeTrue())
		openaiOpts, ok := openaiEntry["options"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(openaiOpts).NotTo(HaveKey("apiKey"))
	})
})

var _ = Describe("resolveOpenCodeAgentRoute", func() {
	It("returns anthropic route by default", func() {
		cfg := &startConfig{OllamaUpstream: "http://localhost:11434"}
		route := resolveOpenCodeAgentRoute(cfg)
		Expect(route.ProviderType).To(Equal("anthropic"))
		Expect(route.UpstreamURL).To(Equal("https://api.anthropic.com"))
	})

	It("returns openai route when configured", func() {
		cfg := &startConfig{
			OpenCodeProvider: "openai",
			OllamaUpstream:   "http://localhost:11434",
		}
		route := resolveOpenCodeAgentRoute(cfg)
		Expect(route.ProviderType).To(Equal("openai"))
		Expect(route.UpstreamURL).To(Equal("https://api.openai.com/v1"))
	})

	It("returns ollama route with configured upstream", func() {
		cfg := &startConfig{
			OpenCodeProvider: "ollama",
			OllamaUpstream:   "http://custom-ollama:11434",
		}
		route := resolveOpenCodeAgentRoute(cfg)
		Expect(route.ProviderType).To(Equal("ollama"))
		Expect(route.UpstreamURL).To(Equal("http://custom-ollama:11434"))
	})

	It("falls back to anthropic for unknown provider", func() {
		cfg := &startConfig{
			OpenCodeProvider: "unknown",
			OllamaUpstream:   "http://localhost:11434",
		}
		route := resolveOpenCodeAgentRoute(cfg)
		Expect(route.ProviderType).To(Equal("anthropic"))
		Expect(route.UpstreamURL).To(Equal("https://api.anthropic.com"))
	})
})

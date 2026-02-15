package config_test

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/config"
)

func TestConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}

var _ = Describe("Configer config", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "config-test-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Describe("LoadConfig", func() {
		It("returns default config when no config file exists", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())

			defaults := config.NewDefaultConfig()
			Expect(cfg.Version).To(Equal(defaults.Version))
			Expect(cfg.Proxy.Provider).To(Equal(defaults.Proxy.Provider))
			Expect(cfg.Proxy.Upstream).To(Equal(defaults.Proxy.Upstream))
			Expect(cfg.Proxy.Listen).To(Equal(defaults.Proxy.Listen))
			Expect(cfg.API.Listen).To(Equal(defaults.API.Listen))
			Expect(cfg.Client.ProxyTarget).To(Equal(defaults.Client.ProxyTarget))
			Expect(cfg.Client.APITarget).To(Equal(defaults.Client.APITarget))
			Expect(cfg.VectorStore.Provider).To(Equal(defaults.VectorStore.Provider))
			Expect(cfg.Embedding.Provider).To(Equal(defaults.Embedding.Provider))
			Expect(cfg.Embedding.Target).To(Equal(defaults.Embedding.Target))
			Expect(cfg.Embedding.Model).To(Equal(defaults.Embedding.Model))
			Expect(cfg.Embedding.Dimensions).To(Equal(defaults.Embedding.Dimensions))
		})

		It("loads a valid config file", func() {
			data := `version = 0

[proxy]
provider = "anthropic"
upstream = "https://api.anthropic.com"

[embedding]
dimensions = 768
`
			err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)
			Expect(err).NotTo(HaveOccurred())

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())
			Expect(cfg.Version).To(Equal(0))
			Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
			Expect(cfg.Proxy.Upstream).To(Equal("https://api.anthropic.com"))
			Expect(cfg.Embedding.Dimensions).To(Equal(uint(768)))
		})

		It("loads all config fields", func() {
			data := `version = 0

[storage]
sqlite_path = "/tmp/tapes.sqlite"

[proxy]
provider = "openai"
upstream = "https://api.openai.com"
listen = ":9090"

[api]
listen = ":9091"

[client]
proxy_target = "http://myhost:9090"
api_target = "http://myhost:9091"

[vector_store]
provider = "chroma"
target = "http://localhost:8000"

[embedding]
provider = "ollama"
target = "http://localhost:11434"
model = "nomic-embed-text"
dimensions = 1024
`
			err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)
			Expect(err).NotTo(HaveOccurred())

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Version).To(Equal(0))
			Expect(cfg.Storage.SQLitePath).To(Equal("/tmp/tapes.sqlite"))
			Expect(cfg.Proxy.Provider).To(Equal("openai"))
			Expect(cfg.Proxy.Upstream).To(Equal("https://api.openai.com"))
			Expect(cfg.Proxy.Listen).To(Equal(":9090"))
			Expect(cfg.API.Listen).To(Equal(":9091"))
			Expect(cfg.Client.ProxyTarget).To(Equal("http://myhost:9090"))
			Expect(cfg.Client.APITarget).To(Equal("http://myhost:9091"))
			Expect(cfg.VectorStore.Provider).To(Equal("chroma"))
			Expect(cfg.VectorStore.Target).To(Equal("http://localhost:8000"))
			Expect(cfg.Embedding.Provider).To(Equal("ollama"))
			Expect(cfg.Embedding.Target).To(Equal("http://localhost:11434"))
			Expect(cfg.Embedding.Model).To(Equal("nomic-embed-text"))
			Expect(cfg.Embedding.Dimensions).To(Equal(uint(1024)))
		})

		It("returns error for malformed TOML", func() {
			err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte("not valid toml [[["), 0o600)
			Expect(err).NotTo(HaveOccurred())

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).To(HaveOccurred())
			Expect(cfg).To(BeNil())
		})

		It("returns error for unsupported config version", func() {
			data := `version = 99
`
			err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)
			Expect(err).NotTo(HaveOccurred())

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported config version"))
			Expect(cfg).To(BeNil())
		})

		It("accepts config with version 0 (omitted)", func() {
			data := `[proxy]
provider = "openai"
`
			err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)
			Expect(err).NotTo(HaveOccurred())

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Proxy.Provider).To(Equal("openai"))
		})
	})

	Describe("SaveConfig", func() {
		It("persists config to disk", func() {
			cfg := &config.Config{
				Version: config.CurrentV,
				Proxy: config.ProxyConfig{
					Provider: "anthropic",
					Upstream: "https://api.anthropic.com",
				},
				Embedding: config.EmbeddingConfig{
					Dimensions: 768,
				},
			}

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SaveConfig(cfg)
			Expect(err).NotTo(HaveOccurred())

			// Verify the file exists
			_, err = os.Stat(filepath.Join(tmpDir, "config.toml"))
			Expect(err).NotTo(HaveOccurred())

			// Load it back and verify
			loaded, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded.Proxy.Provider).To(Equal("anthropic"))
			Expect(loaded.Proxy.Upstream).To(Equal("https://api.anthropic.com"))
			Expect(loaded.Embedding.Dimensions).To(Equal(uint(768)))
		})

		It("returns error for nil config", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SaveConfig(nil)
			Expect(err).To(HaveOccurred())
		})

		It("overwrites existing config", func() {
			first := &config.Config{
				Version: config.CurrentV,
				Proxy:   config.ProxyConfig{Provider: "ollama"},
			}
			second := &config.Config{
				Version: config.CurrentV,
				Proxy:   config.ProxyConfig{Provider: "anthropic"},
			}

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SaveConfig(first)
			Expect(err).NotTo(HaveOccurred())

			err = c.SaveConfig(second)
			Expect(err).NotTo(HaveOccurred())

			loaded, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded.Proxy.Provider).To(Equal("anthropic"))
		})
	})

	Describe("SetConfigValue", func() {
		It("sets a string config key", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("proxy.provider", "anthropic")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
		})

		It("sets a uint config key", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("embedding.dimensions", "1024")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Embedding.Dimensions).To(Equal(uint(1024)))
		})

		It("returns error for unknown key", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("nonexistent_key", "value")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown config key"))
		})

		It("returns error for invalid uint value", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("embedding.dimensions", "not-a-number")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid value"))
		})

		It("sets client.proxy_target", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("client.proxy_target", "http://remote:9090")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Client.ProxyTarget).To(Equal("http://remote:9090"))
		})

		It("sets client.api_target", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("client.api_target", "http://remote:9091")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Client.APITarget).To(Equal("http://remote:9091"))
		})

		It("sets opencode.provider", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("opencode.provider", "ollama")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.OpenCode.Provider).To(Equal("ollama"))
		})

		It("sets opencode.model", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("opencode.model", "claude-sonnet-4-5")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.OpenCode.Model).To(Equal("claude-sonnet-4-5"))
		})

		It("preserves existing values when setting a new key", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("proxy.provider", "anthropic")
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("proxy.upstream", "https://api.anthropic.com")
			Expect(err).NotTo(HaveOccurred())

			cfg, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
			Expect(cfg.Proxy.Upstream).To(Equal("https://api.anthropic.com"))
		})
	})

	Describe("GetConfigValue", func() {
		It("gets a set config value", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("proxy.provider", "anthropic")
			Expect(err).NotTo(HaveOccurred())

			val, err := c.GetConfigValue("proxy.provider")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("anthropic"))
		})

		It("returns default value when no config file exists", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			val, err := c.GetConfigValue("proxy.provider")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(config.NewDefaultConfig().Proxy.Provider))
		})

		It("returns empty string for key with no default", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			val, err := c.GetConfigValue("storage.sqlite_path")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(BeEmpty())
		})

		It("returns error for unknown key", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			_, err = c.GetConfigValue("nonexistent_key")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown config key"))
		})

		It("returns default client target values when no config file exists", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			val, err := c.GetConfigValue("client.proxy_target")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("http://localhost:8080"))

			val, err = c.GetConfigValue("client.api_target")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("http://localhost:8081"))
		})

		It("gets a uint config value as string", func() {
			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SetConfigValue("embedding.dimensions", "512")
			Expect(err).NotTo(HaveOccurred())

			val, err := c.GetConfigValue("embedding.dimensions")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("512"))
		})
	})

	Describe("ValidConfigKeys", func() {
		It("returns all expected keys", func() {
			keys := config.ValidConfigKeys()
			Expect(keys).To(ContainElements(
				"storage.sqlite_path",
				"proxy.provider",
				"proxy.upstream",
				"proxy.listen",
				"api.listen",
				"client.proxy_target",
				"client.api_target",
				"vector_store.provider",
				"vector_store.target",
				"embedding.provider",
				"embedding.target",
				"embedding.model",
				"embedding.dimensions",
				"opencode.provider",
				"opencode.model",
			))
		})

		It("returns keys in stable order", func() {
			keys1 := config.ValidConfigKeys()
			keys2 := config.ValidConfigKeys()
			Expect(keys1).To(Equal(keys2))
		})
	})

	Describe("IsValidConfigKey", func() {
		It("returns true for valid keys", func() {
			Expect(config.IsValidConfigKey("proxy.provider")).To(BeTrue())
			Expect(config.IsValidConfigKey("embedding.dimensions")).To(BeTrue())
			Expect(config.IsValidConfigKey("client.proxy_target")).To(BeTrue())
			Expect(config.IsValidConfigKey("client.api_target")).To(BeTrue())
			Expect(config.IsValidConfigKey("opencode.provider")).To(BeTrue())
			Expect(config.IsValidConfigKey("opencode.model")).To(BeTrue())
		})

		It("returns false for invalid keys", func() {
			Expect(config.IsValidConfigKey("nonexistent")).To(BeFalse())
			Expect(config.IsValidConfigKey("")).To(BeFalse())
		})

		It("returns false for old flat key names", func() {
			Expect(config.IsValidConfigKey("provider")).To(BeFalse())
			Expect(config.IsValidConfigKey("upstream")).To(BeFalse())
			Expect(config.IsValidConfigKey("embedding_dimensions")).To(BeFalse())
		})
	})

	Describe("round-trip", func() {
		It("saves and loads config correctly with all fields", func() {
			cfg := &config.Config{
				Version: config.CurrentV,
				Storage: config.StorageConfig{
					SQLitePath: "/tmp/test.sqlite",
				},
				Proxy: config.ProxyConfig{
					Provider: "openai",
					Upstream: "https://api.openai.com",
					Listen:   ":9090",
				},
				API: config.APIConfig{
					Listen: ":9091",
				},
				Client: config.ClientConfig{
					ProxyTarget: "http://myhost:9090",
					APITarget:   "http://myhost:9091",
				},
				VectorStore: config.VectorStoreConfig{
					Provider: "chroma",
					Target:   "http://localhost:8000",
				},
				Embedding: config.EmbeddingConfig{
					Provider:   "ollama",
					Target:     "http://localhost:11434",
					Model:      "nomic-embed-text",
					Dimensions: 1024,
				},
			}

			c, err := config.NewConfiger(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = c.SaveConfig(cfg)
			Expect(err).NotTo(HaveOccurred())

			loaded, err := c.LoadConfig()
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).To(Equal(cfg))
		})
	})
})

var _ = Describe("PresetConfig", func() {
	It("returns openai preset with correct defaults", func() {
		cfg, err := config.PresetConfig("openai")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Version).To(Equal(config.CurrentV))
		Expect(cfg.Proxy.Provider).To(Equal("openai"))
		Expect(cfg.Proxy.Upstream).To(Equal("https://api.openai.com"))
		Expect(cfg.Proxy.Listen).To(Equal(":8080"))
		Expect(cfg.API.Listen).To(Equal(":8081"))
		Expect(cfg.Client.ProxyTarget).To(Equal("http://localhost:8080"))
		Expect(cfg.Client.APITarget).To(Equal("http://localhost:8081"))
	})

	It("returns anthropic preset with correct defaults", func() {
		cfg, err := config.PresetConfig("anthropic")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Version).To(Equal(config.CurrentV))
		Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
		Expect(cfg.Proxy.Upstream).To(Equal("https://api.anthropic.com"))
		Expect(cfg.Proxy.Listen).To(Equal(":8080"))
		Expect(cfg.API.Listen).To(Equal(":8081"))
		Expect(cfg.Client.ProxyTarget).To(Equal("http://localhost:8080"))
		Expect(cfg.Client.APITarget).To(Equal("http://localhost:8081"))
	})

	It("returns ollama preset with embedding defaults", func() {
		cfg, err := config.PresetConfig("ollama")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Version).To(Equal(config.CurrentV))
		Expect(cfg.Proxy.Provider).To(Equal("ollama"))
		Expect(cfg.Proxy.Upstream).To(Equal("http://localhost:11434"))
		Expect(cfg.Proxy.Listen).To(Equal(":8080"))
		Expect(cfg.API.Listen).To(Equal(":8081"))
		Expect(cfg.Client.ProxyTarget).To(Equal("http://localhost:8080"))
		Expect(cfg.Client.APITarget).To(Equal("http://localhost:8081"))
		Expect(cfg.Embedding.Provider).To(Equal("ollama"))
		Expect(cfg.Embedding.Target).To(Equal("http://localhost:11434"))
		Expect(cfg.Embedding.Model).To(Equal("nomic-embed-text"))
		Expect(cfg.Embedding.Dimensions).To(Equal(uint(768)))
	})

	It("is case-insensitive", func() {
		cfg, err := config.PresetConfig("OpenAI")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Proxy.Provider).To(Equal("openai"))

		cfg, err = config.PresetConfig("ANTHROPIC")
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
	})

	It("returns error for unknown preset", func() {
		cfg, err := config.PresetConfig("nonexistent")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown preset"))
		Expect(cfg).To(BeNil())
	})
})

var _ = Describe("ValidPresetNames", func() {
	It("returns the expected preset names", func() {
		names := config.ValidPresetNames()
		Expect(names).To(ConsistOf("openai", "anthropic", "ollama"))
	})
})

var _ = Describe("ParseConfigTOML", func() {
	It("parses valid TOML into a Config", func() {
		data := []byte(`version = 0

[proxy]
provider = "anthropic"
upstream = "https://api.anthropic.com"
listen = ":9090"

[embedding]
dimensions = 512
`)
		cfg, err := config.ParseConfigTOML(data)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Version).To(Equal(0))
		Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
		Expect(cfg.Proxy.Upstream).To(Equal("https://api.anthropic.com"))
		Expect(cfg.Proxy.Listen).To(Equal(":9090"))
		Expect(cfg.Embedding.Dimensions).To(Equal(uint(512)))
	})

	It("returns error for invalid TOML", func() {
		cfg, err := config.ParseConfigTOML([]byte("not valid [[["))
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
	})

	It("returns empty config for empty input", func() {
		cfg, err := config.ParseConfigTOML([]byte(""))
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg).NotTo(BeNil())
		Expect(cfg.Proxy.Provider).To(BeEmpty())
	})

	It("rejects unsupported config version", func() {
		data := []byte(`version = 2
`)
		cfg, err := config.ParseConfigTOML(data)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unsupported config version"))
		Expect(cfg).To(BeNil())
	})
})

var _ = Describe("NewDefaultConfig", func() {
	It("returns fully-populated defaults", func() {
		cfg := config.NewDefaultConfig()
		Expect(cfg.Version).To(Equal(config.CurrentV))
		Expect(cfg.Proxy.Provider).To(Equal("ollama"))
		Expect(cfg.Proxy.Upstream).To(Equal("http://localhost:11434"))
		Expect(cfg.Proxy.Listen).To(Equal(":8080"))
		Expect(cfg.API.Listen).To(Equal(":8081"))
		Expect(cfg.Client.ProxyTarget).To(Equal("http://localhost:8080"))
		Expect(cfg.Client.APITarget).To(Equal("http://localhost:8081"))
		Expect(cfg.VectorStore.Provider).To(Equal("sqlite"))
		Expect(cfg.Embedding.Provider).To(Equal("ollama"))
		Expect(cfg.Embedding.Target).To(Equal("http://localhost:11434"))
		Expect(cfg.Embedding.Model).To(Equal("embeddinggemma"))
		Expect(cfg.Embedding.Dimensions).To(Equal(uint(768)))
	})
})

var _ = Describe("applyDefaults via LoadConfig", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "config-defaults-test-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	It("fills in defaults for unset fields in a partial config", func() {
		// Config file only sets proxy.provider; everything else should get defaults.
		data := `version = 0

[proxy]
provider = "anthropic"
`
		err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)
		Expect(err).NotTo(HaveOccurred())

		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		cfg, err := c.LoadConfig()
		Expect(err).NotTo(HaveOccurred())

		// Explicitly set value should be preserved.
		Expect(cfg.Proxy.Provider).To(Equal("anthropic"))

		// Unset fields should get defaults.
		defaults := config.NewDefaultConfig()
		Expect(cfg.Proxy.Upstream).To(Equal(defaults.Proxy.Upstream))
		Expect(cfg.Proxy.Listen).To(Equal(defaults.Proxy.Listen))
		Expect(cfg.API.Listen).To(Equal(defaults.API.Listen))
		Expect(cfg.Client.ProxyTarget).To(Equal(defaults.Client.ProxyTarget))
		Expect(cfg.Client.APITarget).To(Equal(defaults.Client.APITarget))
		Expect(cfg.VectorStore.Provider).To(Equal(defaults.VectorStore.Provider))
		Expect(cfg.Embedding.Provider).To(Equal(defaults.Embedding.Provider))
		Expect(cfg.Embedding.Target).To(Equal(defaults.Embedding.Target))
		Expect(cfg.Embedding.Model).To(Equal(defaults.Embedding.Model))
		Expect(cfg.Embedding.Dimensions).To(Equal(defaults.Embedding.Dimensions))
	})

	It("does not overwrite explicitly set values", func() {
		data := `version = 0

[proxy]
provider = "openai"
upstream = "https://api.openai.com"
listen = ":9090"

[api]
listen = ":9091"

[client]
proxy_target = "http://remote:9090"
api_target = "http://remote:9091"

[embedding]
provider = "openai"
target = "https://api.openai.com"
model = "text-embedding-3-small"
dimensions = 1536
`
		err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)
		Expect(err).NotTo(HaveOccurred())

		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		cfg, err := c.LoadConfig()
		Expect(err).NotTo(HaveOccurred())

		Expect(cfg.Proxy.Provider).To(Equal("openai"))
		Expect(cfg.Proxy.Upstream).To(Equal("https://api.openai.com"))
		Expect(cfg.Proxy.Listen).To(Equal(":9090"))
		Expect(cfg.API.Listen).To(Equal(":9091"))
		Expect(cfg.Client.ProxyTarget).To(Equal("http://remote:9090"))
		Expect(cfg.Client.APITarget).To(Equal("http://remote:9091"))
		Expect(cfg.Embedding.Provider).To(Equal("openai"))
		Expect(cfg.Embedding.Target).To(Equal("https://api.openai.com"))
		Expect(cfg.Embedding.Model).To(Equal("text-embedding-3-small"))
		Expect(cfg.Embedding.Dimensions).To(Equal(uint(1536)))
	})
})

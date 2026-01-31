package initcmder_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	initcmder "github.com/papercomputeco/tapes/cmd/tapes/init"
	"github.com/papercomputeco/tapes/pkg/config"
)

var _ = Describe("NewInitCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := initcmder.NewInitCmd()
		Expect(cmd.Use).To(Equal("init"))
	})

	It("accepts zero arguments", func() {
		cmd := initcmder.NewInitCmd()
		err := cmd.Args(cmd, []string{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects any arguments", func() {
		cmd := initcmder.NewInitCmd()
		err := cmd.Args(cmd, []string{"extra"})
		Expect(err).To(HaveOccurred())
	})

	It("has a --preset flag", func() {
		cmd := initcmder.NewInitCmd()
		f := cmd.Flags().Lookup("preset")
		Expect(f).NotTo(BeNil())
		Expect(f.DefValue).To(Equal(""))
	})
})

var _ = Describe("Init command execution", func() {
	var (
		tmpDir  string
		origDir string
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-init-test-*")
		Expect(err).NotTo(HaveOccurred())

		origDir, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())

		err = os.Chdir(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := os.Chdir(origDir)
		Expect(err).NotTo(HaveOccurred())
		os.RemoveAll(tmpDir)
	})

	It("creates a .tapes directory in the current directory", func() {
		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		info, err := os.Stat(filepath.Join(tmpDir, ".tapes"))
		Expect(err).NotTo(HaveOccurred())
		Expect(info.IsDir()).To(BeTrue())
	})

	It("creates a config.toml with default values", func() {
		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		cfg := loadConfig(tmpDir)
		Expect(cfg.Version).To(Equal(config.CurrentV))
		Expect(cfg.Proxy.Provider).To(Equal("ollama"))
		Expect(cfg.Proxy.Upstream).To(Equal("http://localhost:11434"))
		Expect(cfg.Proxy.Listen).To(Equal(":8080"))
		Expect(cfg.API.Listen).To(Equal(":8081"))
	})

	It("succeeds when .tapes directory already exists", func() {
		err := os.MkdirAll(filepath.Join(tmpDir, ".tapes"), 0o755)
		Expect(err).NotTo(HaveOccurred())

		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		info, err := os.Stat(filepath.Join(tmpDir, ".tapes"))
		Expect(err).NotTo(HaveOccurred())
		Expect(info.IsDir()).To(BeTrue())
	})

	It("does not overwrite existing contents when already initialized", func() {
		tapesDir := filepath.Join(tmpDir, ".tapes")
		err := os.MkdirAll(tapesDir, 0o755)
		Expect(err).NotTo(HaveOccurred())

		// Write a file into the existing .tapes dir
		testFile := filepath.Join(tapesDir, "checkout.json")
		err = os.WriteFile(testFile, []byte(`{"hash":"abc"}`), 0o644)
		Expect(err).NotTo(HaveOccurred())

		cmd := initcmder.NewInitCmd()
		cmd.SetArgs([]string{})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		// Verify the existing file is still there
		data, err := os.ReadFile(testFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`{"hash":"abc"}`))
	})

	Describe("--preset with provider presets", func() {
		It("creates config.toml with openai preset", func() {
			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", "openai"})
			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			cfg := loadConfig(tmpDir)
			Expect(cfg.Version).To(Equal(config.CurrentV))
			Expect(cfg.Proxy.Provider).To(Equal("openai"))
			Expect(cfg.Proxy.Upstream).To(Equal("https://api.openai.com"))
			Expect(cfg.Proxy.Listen).To(Equal(":8080"))
			Expect(cfg.API.Listen).To(Equal(":8081"))
		})

		It("creates config.toml with anthropic preset", func() {
			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", "anthropic"})
			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			cfg := loadConfig(tmpDir)
			Expect(cfg.Version).To(Equal(config.CurrentV))
			Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
			Expect(cfg.Proxy.Upstream).To(Equal("https://api.anthropic.com"))
			Expect(cfg.Proxy.Listen).To(Equal(":8080"))
			Expect(cfg.API.Listen).To(Equal(":8081"))
		})

		It("creates config.toml with ollama preset", func() {
			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", "ollama"})
			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			cfg := loadConfig(tmpDir)
			Expect(cfg.Version).To(Equal(config.CurrentV))
			Expect(cfg.Proxy.Provider).To(Equal("ollama"))
			Expect(cfg.Proxy.Upstream).To(Equal("http://localhost:11434"))
			Expect(cfg.Embedding.Provider).To(Equal("ollama"))
			Expect(cfg.Embedding.Target).To(Equal("http://localhost:11434"))
			Expect(cfg.Embedding.Model).To(Equal("nomic-embed-text"))
			Expect(cfg.Embedding.Dimensions).To(Equal(uint(768)))
		})

		It("rejects unknown preset names", func() {
			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", "invalid-provider"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown preset"))
		})
	})

	Describe("--preset with remote URL", func() {
		It("fetches and writes remote config.toml", func() {
			remoteCfg := `version = 0

[proxy]
provider = "openai"
upstream = "https://api.openai.com/v1"
listen = ":9090"

[embedding]
model = "text-embedding-3-small"
dimensions = 1536
`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain")
				fmt.Fprint(w, remoteCfg)
			}))
			defer server.Close()

			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", server.URL})
			err := cmd.Execute()
			Expect(err).NotTo(HaveOccurred())

			cfg := loadConfig(tmpDir)
			Expect(cfg.Version).To(Equal(0))
			Expect(cfg.Proxy.Provider).To(Equal("openai"))
			Expect(cfg.Proxy.Upstream).To(Equal("https://api.openai.com/v1"))
			Expect(cfg.Proxy.Listen).To(Equal(":9090"))
			Expect(cfg.Embedding.Model).To(Equal("text-embedding-3-small"))
			Expect(cfg.Embedding.Dimensions).To(Equal(uint(1536)))
		})

		It("returns error for non-200 HTTP response", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", server.URL})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("HTTP 404"))
		})

		It("returns error for invalid TOML from URL", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "this is not valid toml [[[")
			}))
			defer server.Close()

			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", server.URL})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("parsing"))
		})

		It("returns error for unreachable URL", func() {
			cmd := initcmder.NewInitCmd()
			cmd.SetArgs([]string{"--preset", "http://127.0.0.1:1"})
			err := cmd.Execute()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("fetching remote config"))
		})
	})

	Describe("--preset overwrites config on re-init", func() {
		It("overwrites existing config.toml when re-running with a different preset", func() {
			// First init with openai
			cmd1 := initcmder.NewInitCmd()
			cmd1.SetArgs([]string{"--preset", "openai"})
			err := cmd1.Execute()
			Expect(err).NotTo(HaveOccurred())

			cfg := loadConfig(tmpDir)
			Expect(cfg.Proxy.Provider).To(Equal("openai"))

			// Re-init with anthropic
			cmd2 := initcmder.NewInitCmd()
			cmd2.SetArgs([]string{"--preset", "anthropic"})
			err = cmd2.Execute()
			Expect(err).NotTo(HaveOccurred())

			cfg = loadConfig(tmpDir)
			Expect(cfg.Proxy.Provider).To(Equal("anthropic"))
		})
	})
})

// loadConfig is a test helper that reads and parses the config.toml from the
// .tapes directory within the given base directory.
func loadConfig(baseDir string) *config.Config {
	configPath := filepath.Join(baseDir, ".tapes", "config.toml")
	data, err := os.ReadFile(configPath)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	cfg := &config.Config{}
	err = toml.Unmarshal(data, cfg)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return cfg
}

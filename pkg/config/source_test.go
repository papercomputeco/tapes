package config_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/config"
)

var _ = Describe("GetConfigValueSource", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "config-source-test-*")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(os.RemoveAll, tmpDir)
	})

	setEnv := func(key, value string) {
		previous, hadPrevious := os.LookupEnv(key)
		Expect(os.Setenv(key, value)).To(Succeed())
		DeferCleanup(func() {
			if hadPrevious {
				Expect(os.Setenv(key, previous)).To(Succeed())
			} else {
				Expect(os.Unsetenv(key)).To(Succeed())
			}
		})
	}

	It("reports the built-in default for an untouched key", func() {
		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		value, source, err := c.GetConfigValueSource("proxy.provider")
		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal("ollama"))
		Expect(source).To(Equal(config.SourceDefault))
	})

	It("reports the config file for a key set in config.toml", func() {
		data := "version = 0\n\n[proxy]\nprovider = \"anthropic\"\n"
		Expect(os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)).To(Succeed())

		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		value, source, err := c.GetConfigValueSource("proxy.provider")
		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal("anthropic"))
		Expect(source).To(Equal(config.SourceConfigFile))
	})

	It("reports the environment for a TAPES_ override", func() {
		setEnv("TAPES_PROXY_PROVIDER", "openai")

		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		value, source, err := c.GetConfigValueSource("proxy.provider")
		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal("openai"))
		Expect(source).To(Equal(config.SourceEnvironment))
	})

	It("prefers the environment over the config file", func() {
		data := "version = 0\n\n[proxy]\nprovider = \"anthropic\"\n"
		Expect(os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte(data), 0o600)).To(Succeed())
		setEnv("TAPES_PROXY_PROVIDER", "openai")

		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		value, source, err := c.GetConfigValueSource("proxy.provider")
		Expect(err).NotTo(HaveOccurred())
		Expect(value).To(Equal("openai"))
		Expect(source).To(Equal(config.SourceEnvironment))
	})

	It("maps dotted keys to TAPES_ names the same way values resolve", func() {
		Expect(config.EnvKeyForConfigKey("proxy.listen")).To(Equal("TAPES_PROXY_LISTEN"))
		Expect(config.EnvKeyForConfigKey("storage.postgres_dsn")).To(Equal("TAPES_STORAGE_POSTGRES_DSN"))
	})

	It("rejects unknown keys", func() {
		c, err := config.NewConfiger(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		_, _, err = c.GetConfigValueSource("embedding.provider")
		Expect(err).To(MatchError(ContainSubstring("unknown config key")))
	})
})

package credentials_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/credentials"
)

var _ = Describe("Manager", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "credentials-test-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Describe("NewManager", func() {
		It("creates a manager with an override directory", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())
			Expect(mgr.GetTarget()).To(Equal(filepath.Join(tmpDir, "credentials.toml")))
		})
	})

	Describe("Load", func() {
		It("returns empty credentials when no file exists", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			creds, err := mgr.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(creds).NotTo(BeNil())
			Expect(creds.Providers).To(BeEmpty())
		})

		It("loads existing credentials", func() {
			data := `version = 0

[providers.openai]
api_key = "sk-test-key"
`
			err := os.WriteFile(filepath.Join(tmpDir, "credentials.toml"), []byte(data), 0o600)
			Expect(err).NotTo(HaveOccurred())

			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			creds, err := mgr.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(creds.Providers).To(HaveKey("openai"))
			Expect(creds.Providers["openai"].APIKey).To(Equal("sk-test-key"))
		})

		It("returns error for malformed TOML", func() {
			err := os.WriteFile(filepath.Join(tmpDir, "credentials.toml"), []byte("not valid [[["), 0o600)
			Expect(err).NotTo(HaveOccurred())

			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			creds, err := mgr.Load()
			Expect(err).To(HaveOccurred())
			Expect(creds).To(BeNil())
		})
	})

	Describe("Save", func() {
		It("persists credentials to disk with restricted permissions", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			creds := &credentials.Credentials{
				Providers: map[string]credentials.ProviderCredential{
					"openai": {APIKey: "sk-test"},
				},
			}
			err = mgr.Save(creds)
			Expect(err).NotTo(HaveOccurred())

			info, err := os.Stat(filepath.Join(tmpDir, "credentials.toml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Mode().Perm()).To(Equal(os.FileMode(0o600)))
		})

		It("returns error for nil credentials", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.Save(nil)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("SetKey", func() {
		It("stores a new API key", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("openai", "sk-new-key")
			Expect(err).NotTo(HaveOccurred())

			key, err := mgr.GetKey("openai")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("sk-new-key"))
		})

		It("overwrites an existing key", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("openai", "sk-old")
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("openai", "sk-new")
			Expect(err).NotTo(HaveOccurred())

			key, err := mgr.GetKey("openai")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("sk-new"))
		})

		It("preserves other provider keys", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("openai", "sk-openai")
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("anthropic", "sk-anthropic")
			Expect(err).NotTo(HaveOccurred())

			key, err := mgr.GetKey("openai")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("sk-openai"))

			key, err = mgr.GetKey("anthropic")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(Equal("sk-anthropic"))
		})
	})

	Describe("GetKey", func() {
		It("returns empty string for unknown provider", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			key, err := mgr.GetKey("nonexistent")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(BeEmpty())
		})
	})

	Describe("RemoveKey", func() {
		It("removes an existing key", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("openai", "sk-test")
			Expect(err).NotTo(HaveOccurred())

			err = mgr.RemoveKey("openai")
			Expect(err).NotTo(HaveOccurred())

			key, err := mgr.GetKey("openai")
			Expect(err).NotTo(HaveOccurred())
			Expect(key).To(BeEmpty())
		})

		It("is a no-op for nonexistent provider", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.RemoveKey("nonexistent")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("ListProviders", func() {
		It("returns empty list when no credentials stored", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			providers, err := mgr.ListProviders()
			Expect(err).NotTo(HaveOccurred())
			Expect(providers).To(BeEmpty())
		})

		It("returns stored providers in sorted order", func() {
			mgr, err := credentials.NewManager(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = mgr.SetKey("openai", "sk-1")
			Expect(err).NotTo(HaveOccurred())
			err = mgr.SetKey("anthropic", "sk-2")
			Expect(err).NotTo(HaveOccurred())

			providers, err := mgr.ListProviders()
			Expect(err).NotTo(HaveOccurred())
			Expect(providers).To(Equal([]string{"anthropic", "openai"}))
		})
	})
})

var _ = Describe("EnvVarForProvider", func() {
	It("returns OPENAI_API_KEY for openai", func() {
		Expect(credentials.EnvVarForProvider("openai")).To(Equal("OPENAI_API_KEY"))
	})

	It("returns ANTHROPIC_API_KEY for anthropic", func() {
		Expect(credentials.EnvVarForProvider("anthropic")).To(Equal("ANTHROPIC_API_KEY"))
	})

	It("returns empty string for unknown provider", func() {
		Expect(credentials.EnvVarForProvider("unknown")).To(BeEmpty())
	})
})

var _ = Describe("SupportedProviders", func() {
	It("returns openai and anthropic", func() {
		providers := credentials.SupportedProviders()
		Expect(providers).To(ConsistOf("openai", "anthropic"))
	})
})

var _ = Describe("IsSupportedProvider", func() {
	It("returns true for supported providers", func() {
		Expect(credentials.IsSupportedProvider("openai")).To(BeTrue())
		Expect(credentials.IsSupportedProvider("anthropic")).To(BeTrue())
	})

	It("returns false for unsupported providers", func() {
		Expect(credentials.IsSupportedProvider("ollama")).To(BeFalse())
		Expect(credentials.IsSupportedProvider("unknown")).To(BeFalse())
	})
})

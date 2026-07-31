package config_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/config"
)

const validCassetteConfig = `version = 0
cassettes = ["http://127.0.0.1:9000/openapi", "https://example.com/api/spec?format=json"]
`

var _ = Describe("cassette source config", func() {
	It("decodes and preserves URL-only declarations", func() {
		parsed, err := config.ParseConfigTOML([]byte(validCassetteConfig))
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.Cassettes).To(Equal([]string{"http://127.0.0.1:9000/openapi", "https://example.com/api/spec?format=json"}))
	})

	It("rejects the removed manifest table shape", func() {
		_, err := config.ParseConfigTOML([]byte(`[[cassettes]]
manifest = "summary.toml"
url = "http://127.0.0.1:9000"
enabled = true
`))
		Expect(err).To(HaveOccurred())
	})

	It("validates full HTTP URLs while allowing an exact query", func() {
		_, err := config.ParseConfigTOML([]byte(`cassettes = ["tcp://host/spec", "http://host/spec#fragment", "http://user:secret@host/spec", "http://:8080/openapi"]`))
		Expect(err).To(MatchError(And(
			ContainSubstring("http or https"),
			ContainSubstring("fragment"),
			ContainSubstring("must not include URL userinfo"),
			ContainSubstring("must include a host"),
		)))
	})

	It("rejects duplicate source URLs", func() {
		_, err := config.ParseConfigTOML([]byte(`cassettes = ["http://host/openapi", "http://host/openapi"]`))
		Expect(err).To(MatchError(ContainSubstring("cassettes[1]: duplicates cassettes[0]")))
	})

	It("preserves declarations when setting another key", func() {
		directory := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(directory, "config.toml"), []byte(validCassetteConfig), 0o600)).To(Succeed())
		configer, err := config.NewConfiger(directory)
		Expect(err).NotTo(HaveOccurred())
		Expect(configer.SetConfigValue("proxy.provider", "anthropic")).To(Succeed())
		loaded, err := configer.LoadConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.Cassettes).To(HaveLen(2))
	})
})

package embeddingutils_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	embeddingutils "github.com/papercomputeco/tapes/pkg/embeddings/utils"
)

var _ = Describe("NewEmbedder", func() {
	BeforeEach(func() {
		value, ok := os.LookupEnv("OPENAI_API_KEY")
		Expect(os.Unsetenv("OPENAI_API_KEY")).To(Succeed())
		DeferCleanup(func() {
			if ok {
				Expect(os.Setenv("OPENAI_API_KEY", value)).To(Succeed())
				return
			}
			Expect(os.Unsetenv("OPENAI_API_KEY")).To(Succeed())
		})
	})

	It("uses the provided OpenAI API key", func() {
		embedder, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
			ProviderType: "openai",
			TargetURL:    "https://api.openai.test",
			Model:        "text-embedding-3-small",
			APIKey:       "sk-stored",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(embedder.Close()).To(Succeed())
	})

	It("fails when OpenAI has no API key", func() {
		_, err := embeddingutils.NewEmbedder(&embeddingutils.NewEmbedderOpts{
			ProviderType: "openai",
			TargetURL:    "https://api.openai.test",
			Model:        "text-embedding-3-small",
		})
		Expect(err).To(MatchError(ContainSubstring("OPENAI_API_KEY is required")))
	})
})

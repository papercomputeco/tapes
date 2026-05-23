package openai

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOpenAI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpenAI Embeddings Suite")
}

var _ = Describe("Embedder", func() {
	It("posts to the OpenAI embeddings API", func(ctx SpecContext) {
		var gotPath string
		var gotAuth string
		var gotBody map[string]any

		embedder := &Embedder{
			baseURL:    "https://api.openai.test/v1",
			model:      "text-embedding-3-small",
			apiKey:     "sk-test",
			dimensions: 3,
			httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotPath = r.URL.Path
				gotAuth = r.Header.Get("Authorization")
				Expect(json.NewDecoder(r.Body).Decode(&gotBody)).To(Succeed())

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(`{"data":[{"embedding":[0.1,0.2,0.3]}]}`)),
					Header:     make(http.Header),
				}, nil
			})},
		}

		embedding, err := embedder.Embed(ctx, "hello")
		Expect(err).NotTo(HaveOccurred())

		Expect(gotPath).To(Equal("/v1/embeddings"))
		Expect(gotAuth).To(Equal("Bearer sk-test"))
		Expect(gotBody).To(HaveKeyWithValue("model", "text-embedding-3-small"))
		Expect(gotBody).To(HaveKeyWithValue("input", "hello"))
		Expect(gotBody).To(HaveKeyWithValue("dimensions", float64(3)))
		Expect(embedding).To(ConsistOf(float32(0.1), float32(0.2), float32(0.3)))
	})

	It("requires an API key", func() {
		value, ok := os.LookupEnv("OPENAI_API_KEY")
		Expect(os.Unsetenv("OPENAI_API_KEY")).To(Succeed())
		DeferCleanup(func() {
			if ok {
				Expect(os.Setenv("OPENAI_API_KEY", value)).To(Succeed())
				return
			}
			Expect(os.Unsetenv("OPENAI_API_KEY")).To(Succeed())
		})

		_, err := NewEmbedder(EmbedderConfig{})
		Expect(err).To(HaveOccurred())
	})

	It("limits error response bodies", func(ctx SpecContext) {
		embedder := &Embedder{
			baseURL: "https://api.openai.test/v1",
			model:   "text-embedding-3-small",
			apiKey:  "sk-test",
			httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", maxErrorBodyBytes+32))),
					Header:     make(http.Header),
				}, nil
			})},
		}

		_, err := embedder.Embed(ctx, "hello")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(strings.Repeat("x", maxErrorBodyBytes)))
		Expect(err.Error()).NotTo(ContainSubstring(strings.Repeat("x", maxErrorBodyBytes+1)))
	})
})

var _ = Describe("normalizeBaseURL", func() {
	DescribeTable("normalizes base URLs",
		func(raw string, expected string) {
			actual, err := normalizeBaseURL(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("adds v1 to a host root", "https://api.openai.test", "https://api.openai.test/v1"),
		Entry("preserves an existing v1 suffix", "https://api.openai.test/v1", "https://api.openai.test/v1"),
		Entry("adds v1 to a proxy prefix", "https://proxy.test/openai", "https://proxy.test/openai/v1"),
		Entry("preserves v1 as a path segment before additional segments", "https://proxy.test/openai/v1/extra", "https://proxy.test/openai/v1/extra"),
		Entry("does not treat v10 as v1", "https://proxy.test/openai/v10", "https://proxy.test/openai/v10/v1"),
	)
})

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

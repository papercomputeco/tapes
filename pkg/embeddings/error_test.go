package embeddings_test

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/vector"
)

func TestEmbeddings(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embeddings Suite")
}

var _ = Describe("APIError", func() {
	Describe("IsOversize", func() {
		It("is true for a 400 with a maximum-context-length message", func() {
			e := &embeddings.APIError{
				Status:  400,
				Message: "This model's maximum context length is 8192 tokens, however you requested 9523 tokens (9523 in your prompt). Please reduce the length.",
			}
			Expect(e.IsOversize()).To(BeTrue())
		})

		It("is true for a 400 carrying the context_length_exceeded code", func() {
			e := &embeddings.APIError{Status: 400, Code: "context_length_exceeded", Message: "too long"}
			Expect(e.IsOversize()).To(BeTrue())
		})

		It("is true for OpenAI's terse embeddings message (no code, no token count)", func() {
			// The real text-embedding-3 oversize 400, captured live: no
			// machine code and no "requested N tokens" phrasing.
			e := &embeddings.APIError{Status: 400, Message: "Invalid 'input': maximum context length is 8192 tokens."}
			Expect(e.IsOversize()).To(BeTrue())
			Expect(e.RequestedTokens).To(BeZero())
		})

		It("is false for a generic 400 (e.g. malformed request)", func() {
			e := &embeddings.APIError{Status: 400, Message: "Invalid value for 'dimensions'."}
			Expect(e.IsOversize()).To(BeFalse())
		})

		It("is false for an oversize-looking message on a non-400 status", func() {
			e := &embeddings.APIError{Status: 500, Message: "maximum context length is 8192 tokens"}
			Expect(e.IsOversize()).To(BeFalse())
		})
	})

	Describe("Retryable", func() {
		It("is true for 429 and 5xx", func() {
			Expect((&embeddings.APIError{Status: 429}).Retryable()).To(BeTrue())
			Expect((&embeddings.APIError{Status: 503}).Retryable()).To(BeTrue())
		})

		It("is true for a transport failure (status 0)", func() {
			Expect((&embeddings.APIError{Status: 0, Transient: true, Message: "dial tcp: timeout"}).Retryable()).To(BeTrue())
		})

		It("is false for a deterministic 4xx", func() {
			Expect((&embeddings.APIError{Status: 400, Message: "bad"}).Retryable()).To(BeFalse())
			Expect((&embeddings.APIError{Status: 401, Message: "no key"}).Retryable()).To(BeFalse())
		})
	})

	It("unwraps to vector.ErrEmbedding for back-compat", func() {
		var err error = &embeddings.APIError{Status: 400, Message: "x"}
		Expect(errors.Is(err, vector.ErrEmbedding)).To(BeTrue())
	})

	It("is recoverable through a wrapped error chain", func() {
		wrapped := fmt.Errorf("embedding span: %w", &embeddings.APIError{Status: 400, Code: "context_length_exceeded"})
		got, ok := embeddings.AsAPIError(wrapped)
		Expect(ok).To(BeTrue())
		Expect(got.IsOversize()).To(BeTrue())
	})
})

var _ = DescribeTable("ParseRequestedTokens",
	func(message string, want int) {
		Expect(embeddings.ParseRequestedTokens(message)).To(Equal(want))
	},
	Entry("real oversize message", "This model's maximum context length is 8192 tokens, however you requested 9523 tokens (9523 in your prompt; 0 for the completion).", 9523),
	Entry("no token count present", "Invalid request: missing input", 0),
	Entry("empty message", "", 0),
)

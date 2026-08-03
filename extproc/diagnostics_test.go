package extproc

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("reducerEmptyReason", func() {
	It("returns nil_response for a nil pointer", func() {
		reason, empty := reducerEmptyReason(nil)
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("nil_response"))
	})

	It("flags an empty Role", func() {
		reason, empty := reducerEmptyReason(&llm.ChatResponse{
			Message: llm.Message{Content: []llm.ContentBlock{{Type: "text"}}},
		})
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("missing_role"))
	})

	It("flags zero-length Content — the downstream validator's empty-Content path", func() {
		reason, empty := reducerEmptyReason(&llm.ChatResponse{
			Message: llm.Message{Role: "assistant"},
		})
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("empty_content"))
	})

	It("flags a block with an empty Type", func() {
		reason, empty := reducerEmptyReason(&llm.ChatResponse{
			Message: llm.Message{
				Role:    "assistant",
				Content: []llm.ContentBlock{{Type: "text"}, {Type: ""}},
			},
		})
		Expect(empty).To(BeTrue())
		Expect(reason).To(Equal("missing_block_type"))
	})

	It("accepts a well-formed response", func() {
		_, empty := reducerEmptyReason(&llm.ChatResponse{
			Message: llm.Message{
				Role:    "assistant",
				Content: []llm.ContentBlock{{Type: "text", Text: "ok"}},
			},
		})
		Expect(empty).To(BeFalse())
	})
})

var _ = Describe("respBodyPreview", func() {
	It("returns <empty> for an empty body", func() {
		Expect(respBodyPreview(nil, 200)).To(Equal("<empty>"))
		Expect(respBodyPreview([]byte{}, 200)).To(Equal("<empty>"))
	})

	It("passes printable ASCII through unchanged", func() {
		Expect(respBodyPreview([]byte(`event: message_start`), 200)).
			To(Equal(`event: message_start`))
	})

	It("hex-escapes binary bytes so the line stays single-grep-friendly", func() {
		// 0x1f8b is the gzip magic number — the case we'd see if Envoy
		// is forwarding a compressed body through to the reducer.
		Expect(respBodyPreview([]byte{0x1f, 0x8b, 0x08, 0x00}, 200)).
			To(Equal(`\x1f\x8b\x08\x00`))
	})

	It("escapes whitespace control chars", func() {
		Expect(respBodyPreview([]byte("a\nb\tc"), 200)).
			To(Equal(`a\nb\tc`))
	})

	It("truncates with the original byte count when over the cap", func() {
		body := make([]byte, 500)
		for i := range body {
			body[i] = 'a'
		}
		preview := respBodyPreview(body, 10)
		Expect(preview).To(HaveSuffix("...(500B total)"))
		// 10 printable bytes plus the suffix
		Expect(preview).To(HavePrefix("aaaaaaaaaa"))
	})
})

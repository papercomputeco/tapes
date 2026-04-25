package proxy

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("extractUsageFromSSE", func() {
	var p *Proxy

	BeforeEach(func() {
		p, _ = newTestProxy("http://localhost:0")
	})

	AfterEach(func() {
		p.Close()
	})

	// Anthropic extraction tests moved to pkg/capture — the proxy's legacy
	// extraction helpers no longer handle anthropic; the provider routes
	// through capture.Reduce.

	Describe("OpenAI provider", func() {
		It("extracts usage from the final chunk", func() {
			usage := &llm.Usage{}
			meta := &streamMeta{}
			data := []byte(`{"id":"chatcmpl-123","choices":[],"usage":{"prompt_tokens":50,"completion_tokens":120,"total_tokens":170}}`)
			p.extractUsageFromSSE(data, providerOpenAI, usage, meta)

			Expect(usage.PromptTokens).To(Equal(50))
			Expect(usage.CompletionTokens).To(Equal(120))
		})

		It("does not extract from chunks without usage", func() {
			usage := &llm.Usage{}
			meta := &streamMeta{}
			data := []byte(`{"id":"chatcmpl-123","choices":[{"delta":{"content":"Hi"}}]}`)
			p.extractUsageFromSSE(data, providerOpenAI, usage, meta)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})
	})

	Describe("Ollama provider", func() {
		It("extracts usage from the final done=true line", func() {
			usage := &llm.Usage{}
			meta := &streamMeta{}
			data := []byte(`{"model":"llama3","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":25,"eval_count":50}`)
			p.extractUsageFromSSE(data, providerOllama, usage, meta)

			Expect(usage.PromptTokens).To(Equal(25))
			Expect(usage.CompletionTokens).To(Equal(50))
		})

		It("does not extract from non-final chunks", func() {
			usage := &llm.Usage{}
			meta := &streamMeta{}
			data := []byte(`{"model":"llama3","message":{"role":"assistant","content":"Hello"},"done":false}`)
			p.extractUsageFromSSE(data, providerOllama, usage, meta)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})
	})

	Describe("invalid data", func() {
		It("ignores invalid JSON", func() {
			usage := &llm.Usage{}
			meta := &streamMeta{}
			p.extractUsageFromSSE([]byte(`not-json`), providerAnthropic, usage, meta)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})

		It("ignores empty data", func() {
			usage := &llm.Usage{}
			meta := &streamMeta{}
			p.extractUsageFromSSE([]byte(``), providerOpenAI, usage, meta)

			Expect(usage.PromptTokens).To(Equal(0))
		})
	})
})

var _ = Describe("reconstructStreamedResponse with stream usage", func() {
	var p *Proxy

	BeforeEach(func() {
		p, _ = newTestProxy("http://localhost:0")
	})

	AfterEach(func() {
		p.Close()
	})

	It("prefers accumulated stream usage over empty last-chunk usage", func() {
		chunks := [][]byte{
			[]byte(`{"model":"test-model","message":{"role":"assistant","content":"Hi"},"done":false}`),
			[]byte(`{"model":"test-model","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop"}`),
		}
		streamUsage := &llm.Usage{
			PromptTokens:             5000,
			CompletionTokens:         200,
			CacheCreationInputTokens: 1000,
			CacheReadInputTokens:     3500,
		}

		resp := p.reconstructStreamedResponse(chunks, "Hi", streamUsage, &streamMeta{}, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Usage).NotTo(BeNil())
		Expect(resp.Usage.PromptTokens).To(Equal(5000))
		Expect(resp.Usage.CompletionTokens).To(Equal(200))
		Expect(resp.Usage.TotalTokens).To(Equal(5200))
		Expect(resp.Usage.CacheCreationInputTokens).To(Equal(1000))
		Expect(resp.Usage.CacheReadInputTokens).To(Equal(3500))
	})

	It("uses stream usage in fallback path when last chunk is unparseable", func() {
		chunks := [][]byte{
			[]byte(`not-json`),
		}
		streamUsage := &llm.Usage{
			PromptTokens:     100,
			CompletionTokens: 50,
		}

		resp := p.reconstructStreamedResponse(chunks, "fallback content", streamUsage, &streamMeta{}, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Usage).NotTo(BeNil())
		Expect(resp.Usage.PromptTokens).To(Equal(100))
		Expect(resp.Usage.CompletionTokens).To(Equal(50))
		Expect(resp.Usage.TotalTokens).To(Equal(150))
	})

	It("does not set usage when stream usage is empty", func() {
		chunks := [][]byte{
			[]byte(`not-json`),
		}
		streamUsage := &llm.Usage{}

		resp := p.reconstructStreamedResponse(chunks, "content", streamUsage, &streamMeta{}, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Usage).To(BeNil())
	})

	It("applies accumulated stop reason to response", func() {
		chunks := [][]byte{
			[]byte(`not-json`),
		}
		streamUsage := &llm.Usage{PromptTokens: 100, CompletionTokens: 50}
		meta := &streamMeta{StopReason: "end_turn"}

		resp := p.reconstructStreamedResponse(chunks, "content", streamUsage, meta, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.StopReason).To(Equal("end_turn"))
	})

	It("applies accumulated model to response when last chunk has no model", func() {
		chunks := [][]byte{
			[]byte(`not-json`),
		}
		streamUsage := &llm.Usage{PromptTokens: 100, CompletionTokens: 50}
		meta := &streamMeta{Model: "claude-opus-4-6", StopReason: "end_turn"}

		resp := p.reconstructStreamedResponse(chunks, "content", streamUsage, meta, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Model).To(Equal("claude-opus-4-6"))
	})

	It("preserves model from last chunk when available", func() {
		chunks := [][]byte{
			[]byte(`{"model":"llama3","message":{"role":"assistant","content":"Hi"},"done":true,"done_reason":"stop"}`),
		}
		streamUsage := &llm.Usage{PromptTokens: 100, CompletionTokens: 50}
		meta := &streamMeta{Model: "override-model"}

		resp := p.reconstructStreamedResponse(chunks, "Hi", streamUsage, meta, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		// The last chunk has model "llama3", so that should be preserved
		Expect(resp.Model).To(Equal("llama3"))
	})
})

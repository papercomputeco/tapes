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

	Describe("Anthropic provider", func() {
		It("extracts input tokens from message_start event", func() {
			usage := &llm.Usage{}
			data := []byte(`{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}`)
			p.extractUsageFromSSE(data, providerAnthropic, usage)

			Expect(usage.PromptTokens).To(Equal(100))
			Expect(usage.CacheCreationInputTokens).To(Equal(0))
			Expect(usage.CacheReadInputTokens).To(Equal(0))
		})

		It("extracts cache tokens from message_start event", func() {
			usage := &llm.Usage{}
			data := []byte(`{"type":"message_start","message":{"usage":{"input_tokens":500,"cache_creation_input_tokens":2000,"cache_read_input_tokens":8000}}}`)
			p.extractUsageFromSSE(data, providerAnthropic, usage)

			Expect(usage.PromptTokens).To(Equal(500 + 2000 + 8000))
			Expect(usage.CacheCreationInputTokens).To(Equal(2000))
			Expect(usage.CacheReadInputTokens).To(Equal(8000))
		})

		It("extracts output tokens from message_delta event", func() {
			usage := &llm.Usage{}
			data := []byte(`{"type":"message_delta","usage":{"output_tokens":350}}`)
			p.extractUsageFromSSE(data, providerAnthropic, usage)

			Expect(usage.CompletionTokens).To(Equal(350))
		})

		It("accumulates usage across message_start and message_delta events", func() {
			usage := &llm.Usage{}

			start := []byte(`{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_creation_input_tokens":500,"cache_read_input_tokens":3000}}}`)
			p.extractUsageFromSSE(start, providerAnthropic, usage)

			delta := []byte(`{"type":"message_delta","usage":{"output_tokens":200}}`)
			p.extractUsageFromSSE(delta, providerAnthropic, usage)

			Expect(usage.PromptTokens).To(Equal(100 + 500 + 3000))
			Expect(usage.CompletionTokens).To(Equal(200))
			Expect(usage.CacheCreationInputTokens).To(Equal(500))
			Expect(usage.CacheReadInputTokens).To(Equal(3000))
		})

		It("ignores content_block_delta events", func() {
			usage := &llm.Usage{}
			data := []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}`)
			p.extractUsageFromSSE(data, providerAnthropic, usage)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})
	})

	Describe("OpenAI provider", func() {
		It("extracts usage from the final chunk", func() {
			usage := &llm.Usage{}
			data := []byte(`{"id":"chatcmpl-123","choices":[],"usage":{"prompt_tokens":50,"completion_tokens":120,"total_tokens":170}}`)
			p.extractUsageFromSSE(data, providerOpenAI, usage)

			Expect(usage.PromptTokens).To(Equal(50))
			Expect(usage.CompletionTokens).To(Equal(120))
		})

		It("does not extract from chunks without usage", func() {
			usage := &llm.Usage{}
			data := []byte(`{"id":"chatcmpl-123","choices":[{"delta":{"content":"Hi"}}]}`)
			p.extractUsageFromSSE(data, providerOpenAI, usage)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})
	})

	Describe("Ollama provider", func() {
		It("extracts usage from the final done=true line", func() {
			usage := &llm.Usage{}
			data := []byte(`{"model":"llama3","message":{"role":"assistant","content":""},"done":true,"done_reason":"stop","prompt_eval_count":25,"eval_count":50}`)
			p.extractUsageFromSSE(data, providerOllama, usage)

			Expect(usage.PromptTokens).To(Equal(25))
			Expect(usage.CompletionTokens).To(Equal(50))
		})

		It("does not extract from non-final chunks", func() {
			usage := &llm.Usage{}
			data := []byte(`{"model":"llama3","message":{"role":"assistant","content":"Hello"},"done":false}`)
			p.extractUsageFromSSE(data, providerOllama, usage)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})
	})

	Describe("invalid data", func() {
		It("ignores invalid JSON", func() {
			usage := &llm.Usage{}
			p.extractUsageFromSSE([]byte(`not-json`), providerAnthropic, usage)

			Expect(usage.PromptTokens).To(Equal(0))
			Expect(usage.CompletionTokens).To(Equal(0))
		})

		It("ignores empty data", func() {
			usage := &llm.Usage{}
			p.extractUsageFromSSE([]byte(``), providerOpenAI, usage)

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

		resp := p.reconstructStreamedResponse(chunks, "Hi", streamUsage, p.defaultProv)
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

		resp := p.reconstructStreamedResponse(chunks, "fallback content", streamUsage, p.defaultProv)
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

		resp := p.reconstructStreamedResponse(chunks, "content", streamUsage, p.defaultProv)
		Expect(resp).NotTo(BeNil())
		Expect(resp.Usage).To(BeNil())
	})
})

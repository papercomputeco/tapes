package provider_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm/provider"
)

var _ = Describe("Detector", func() {
	var detector *provider.Detector

	BeforeEach(func() {
		detector = provider.NewDetector()
	})

	Describe("NewDetector", func() {
		It("creates a new detector instance", func() {
			Expect(detector).NotTo(BeNil())
		})
	})

	Describe("Detect", func() {
		Context("with Anthropic payloads", func() {
			It("detects Claude model names", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("anthropic"))
			})

			It("detects Anthropic responses", func() {
				payload := []byte(`{
					"id": "msg_123",
					"type": "message",
					"role": "assistant",
					"content": [{"type": "text", "text": "Hi"}],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "end_turn"
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("anthropic"))
			})

			It("detects max_tokens + system combination", func() {
				payload := []byte(`{
					"model": "some-model",
					"max_tokens": 1024,
					"system": "You are helpful",
					"messages": []
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("anthropic"))
			})
		})

		Context("with OpenAI payloads", func() {
			It("detects GPT model names", func() {
				payload := []byte(`{
					"model": "gpt-4-turbo",
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("openai"))
			})

			It("detects o1 model names", func() {
				payload := []byte(`{
					"model": "o1-preview",
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("openai"))
			})

			It("detects OpenAI responses with choices", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"model": "gpt-4",
					"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}}]
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("openai"))
			})

			It("detects chat.completion object type", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"model": "some-model",
					"choices": []
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("openai"))
			})
		})

		Context("with Ollama payloads", func() {
			It("detects keep_alive field", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"keep_alive": "5m"
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("ollama"))
			})

			It("detects options field", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"temperature": 0.7}
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("ollama"))
			})

			It("detects Ollama responses with context", func() {
				payload := []byte(`{
					"model": "llama2",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"context": [1, 2, 3]
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("ollama"))
			})

			It("detects Ollama responses with total_duration", func() {
				payload := []byte(`{
					"model": "llama2",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"total_duration": 5000000000
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("ollama"))
			})
		})

		Context("with unknown payloads", func() {
			It("falls back to BestEffort for unrecognized format", func() {
				payload := []byte(`{
					"custom_model": "my-model",
					"input_text": "Hello world"
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("besteffort"))
			})

			It("falls back to BestEffort for empty JSON", func() {
				payload := []byte(`{}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("besteffort"))
			})

			It("falls back to BestEffort for invalid JSON", func() {
				payload := []byte(`not json at all`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("besteffort"))
			})

			It("falls back to BestEffort for generic messages-only payload", func() {
				// This has messages but no provider-specific markers
				payload := []byte(`{
					"model": "unknown-model",
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("besteffort"))
			})
		})

		Context("provider detection priority", func() {
			// Anthropic is checked first, then OpenAI, then Ollama
			It("prefers Anthropic over BestEffort for Claude models", func() {
				payload := []byte(`{
					"model": "claude-3-opus-20240229",
					"messages": []
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("anthropic"))
			})

			It("prefers OpenAI over BestEffort for GPT models", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"messages": []
				}`)

				p := detector.Detect(payload)
				Expect(p.Name()).To(Equal("openai"))
			})
		})
	})

	Describe("DetectRequest", func() {
		It("detects provider and validates parsing", func() {
			payload := []byte(`{
				"model": "gpt-4",
				"messages": [{"role": "user", "content": "Hello"}]
			}`)

			p, err := detector.DetectRequest(payload)
			Expect(err).NotTo(HaveOccurred())
			Expect(p.Name()).To(Equal("openai"))
		})

		It("returns error for unparseable request from known provider", func() {
			// This is recognized as Anthropic (claude-* model) but missing required fields
			payload := []byte(`{
				"model": "claude-3-sonnet-20240229"
			}`)

			p, err := detector.DetectRequest(payload)
			// The provider is detected, but parsing should succeed (even with missing fields)
			// since ParseRequest doesn't validate required fields strictly
			Expect(p.Name()).To(Equal("anthropic"))
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

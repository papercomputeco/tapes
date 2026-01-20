package ollama_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/llm/provider/ollama"
)

var _ = Describe("Ollama Provider", func() {
	var p provider.Provider

	BeforeEach(func() {
		p = ollama.New()
	})

	Describe("Name", func() {
		It("returns 'ollama'", func() {
			Expect(p.Name()).To(Equal("ollama"))
		})
	})

	Describe("CanHandle", func() {
		Context("with Ollama-specific request fields", func() {
			It("returns true when keep_alive is present", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"keep_alive": "5m"
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true when options is present", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"temperature": 0.7}
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with Ollama-specific response fields", func() {
			It("returns true when context is present", func() {
				payload := []byte(`{
					"model": "llama2",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"context": [1, 2, 3, 4, 5]
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true when total_duration is present", func() {
				payload := []byte(`{
					"model": "llama2",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"total_duration": 1234567890
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true when eval_count is present", func() {
				payload := []byte(`{
					"model": "llama2",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"eval_count": 42
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with non-Ollama payloads", func() {
			It("returns false for OpenAI-style request", func() {
				payload := []byte(`{"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeFalse())
			})

			It("returns false for Anthropic-style request", func() {
				payload := []byte(`{"model": "claude-3-sonnet", "max_tokens": 1024, "messages": []}`)
				Expect(p.CanHandle(payload)).To(BeFalse())
			})

			It("returns false for invalid JSON", func() {
				payload := []byte(`not valid json`)
				Expect(p.CanHandle(payload)).To(BeFalse())
			})

			It("returns false for empty payload", func() {
				payload := []byte(`{}`)
				Expect(p.CanHandle(payload)).To(BeFalse())
			})
		})
	})

	Describe("ParseRequest", func() {
		Context("with a simple text request", func() {
			It("parses model and messages correctly", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [
						{"role": "system", "content": "You are a helpful assistant."},
						{"role": "user", "content": "Hello!"}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Model).To(Equal("llama2"))
				Expect(req.Messages).To(HaveLen(2))
				Expect(req.Messages[0].Role).To(Equal("system"))
				Expect(req.Messages[0].GetText()).To(Equal("You are a helpful assistant."))
				Expect(req.Messages[1].Role).To(Equal("user"))
				Expect(req.Messages[1].GetText()).To(Equal("Hello!"))
			})
		})

		Context("with options", func() {
			It("maps temperature from options", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"temperature": 0.8}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Temperature).To(BeNumerically("~", 0.8, 0.001))
			})

			It("maps top_p from options", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"top_p": 0.9}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.TopP).To(BeNumerically("~", 0.9, 0.001))
			})

			It("maps top_k from options", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"top_k": 40}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.TopK).To(Equal(40))
			})

			It("maps num_predict to MaxTokens", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"num_predict": 1024}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.MaxTokens).To(Equal(1024))
			})

			It("maps seed from options", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"seed": 42}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Seed).To(Equal(42))
			})

			It("maps stop sequences from options", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"stop": ["END", "STOP"]}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Stop).To(ConsistOf("END", "STOP"))
			})

			It("preserves num_ctx in Extra", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"num_ctx": 4096}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("num_ctx", 4096))
			})

			It("preserves repeat_penalty in Extra", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"options": {"repeat_penalty": 1.1}
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("repeat_penalty", 1.1))
			})
		})

		Context("with Ollama-specific fields", func() {
			It("preserves format in Extra", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"format": "json"
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("format", "json"))
			})

			It("preserves keep_alive in Extra", func() {
				payload := []byte(`{
					"model": "llama2",
					"messages": [{"role": "user", "content": "Hello"}],
					"keep_alive": "10m"
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("keep_alive", "10m"))
			})
		})

		Context("with streaming flag", func() {
			It("parses stream: true", func() {
				payload := []byte(`{
					"model": "llama2",
					"stream": true,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Stream).To(BeTrue())
			})

			It("parses stream: false", func() {
				payload := []byte(`{
					"model": "llama2",
					"stream": false,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Stream).To(BeFalse())
			})
		})

		Context("with images (multimodal)", func() {
			It("parses base64 images in messages", func() {
				payload := []byte(`{
					"model": "llava",
					"messages": [
						{
							"role": "user",
							"content": "What's in this image?",
							"images": ["iVBORw0KGgo..."]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(2))
				Expect(req.Messages[0].Content[0].Type).To(Equal("text"))
				Expect(req.Messages[0].Content[0].Text).To(Equal("What's in this image?"))
				Expect(req.Messages[0].Content[1].Type).To(Equal("image"))
				Expect(req.Messages[0].Content[1].ImageBase64).To(Equal("iVBORw0KGgo..."))
			})
		})

		Context("preserves raw request", func() {
			It("stores the original payload in RawRequest", func() {
				payload := []byte(`{"model": "llama2", "messages": []}`)
				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(req.RawRequest)).To(Equal(payload))
			})
		})

		Context("with invalid payload", func() {
			It("returns an error for invalid JSON", func() {
				payload := []byte(`not valid json`)
				_, err := p.ParseRequest(payload)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("ParseResponse", func() {
		Context("with a simple text response", func() {
			It("parses the response correctly", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {
						"role": "assistant",
						"content": "Hello! How can I help you today?"
					},
					"done": true
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Model).To(Equal("llama2"))
				Expect(resp.Message.Role).To(Equal("assistant"))
				Expect(resp.Message.GetText()).To(Equal("Hello! How can I help you today?"))
				Expect(resp.Done).To(BeTrue())
				Expect(resp.StopReason).To(Equal("stop"))
			})
		})

		Context("with usage metrics", func() {
			It("maps Ollama metrics to Usage", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"total_duration": 5000000000,
					"prompt_eval_count": 26,
					"prompt_eval_duration": 1000000000,
					"eval_count": 42,
					"eval_duration": 4000000000
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Usage).NotTo(BeNil())
				Expect(resp.Usage.PromptTokens).To(Equal(26))
				Expect(resp.Usage.CompletionTokens).To(Equal(42))
				Expect(resp.Usage.TotalTokens).To(Equal(68))
				Expect(resp.Usage.TotalDurationNs).To(Equal(int64(5000000000)))
				Expect(resp.Usage.PromptDurationNs).To(Equal(int64(1000000000)))
			})
		})

		Context("with context for continuation", func() {
			It("preserves context in Extra", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"context": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Extra).To(HaveKey("context"))
			})

			It("preserves load_duration in Extra", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"context": [1, 2, 3],
					"load_duration": 500000000
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Extra).To(HaveKeyWithValue("load_duration", int64(500000000)))
			})
		})

		Context("with streaming chunk (done: false)", func() {
			It("sets Done to false", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {"role": "assistant", "content": "Hello"},
					"done": false
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Done).To(BeFalse())
				Expect(resp.StopReason).To(BeEmpty())
			})
		})

		Context("preserves raw response", func() {
			It("stores the original payload in RawResponse", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(resp.RawResponse)).To(Equal(payload))
			})
		})

		Context("with invalid payload", func() {
			It("returns an error for invalid JSON", func() {
				payload := []byte(`not valid json`)
				_, err := p.ParseResponse(payload)
				Expect(err).To(HaveOccurred())
			})
		})
	})
})

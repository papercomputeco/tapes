package besteffort_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/llm/provider/besteffort"
)

var _ = Describe("BestEffort Provider", func() {
	var p provider.Provider

	BeforeEach(func() {
		p = besteffort.New()
	})

	Describe("Name", func() {
		It("returns 'besteffort'", func() {
			Expect(p.Name()).To(Equal("besteffort"))
		})
	})

	Describe("CanHandle", func() {
		It("always returns true for any payload", func() {
			Expect(p.CanHandle([]byte(`{"anything": "here"}`))).To(BeTrue())
		})

		It("returns true for empty JSON", func() {
			Expect(p.CanHandle([]byte(`{}`))).To(BeTrue())
		})

		It("returns true for invalid JSON", func() {
			Expect(p.CanHandle([]byte(`not json`))).To(BeTrue())
		})

		It("returns true for empty payload", func() {
			Expect(p.CanHandle([]byte(``))).To(BeTrue())
		})
	})

	Describe("ParseRequest", func() {
		Context("with standard messages format", func() {
			It("extracts model and messages", func() {
				payload := []byte(`{
					"model": "some-custom-model",
					"messages": [
						{"role": "user", "content": "Hello"}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Model).To(Equal("some-custom-model"))
				Expect(req.Messages).To(HaveLen(1))
				Expect(req.Messages[0].Role).To(Equal("user"))
				Expect(req.Messages[0].GetText()).To(Equal("Hello"))
			})
		})

		Context("with prompt field instead of messages", func() {
			It("converts prompt to a user message", func() {
				payload := []byte(`{
					"model": "custom-model",
					"prompt": "What is the meaning of life?"
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages).To(HaveLen(1))
				Expect(req.Messages[0].Role).To(Equal("user"))
				Expect(req.Messages[0].GetText()).To(Equal("What is the meaning of life?"))
			})
		})

		Context("with input field instead of messages", func() {
			It("converts input to a user message", func() {
				payload := []byte(`{
					"model": "custom-model",
					"input": "Translate this to French"
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages).To(HaveLen(1))
				Expect(req.Messages[0].Role).To(Equal("user"))
				Expect(req.Messages[0].GetText()).To(Equal("Translate this to French"))
			})
		})

		Context("with inputs array", func() {
			It("converts each input to a user message", func() {
				payload := []byte(`{
					"model": "custom-model",
					"inputs": ["First input", "Second input"]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages).To(HaveLen(2))
				Expect(req.Messages[0].GetText()).To(Equal("First input"))
				Expect(req.Messages[1].GetText()).To(Equal("Second input"))
			})
		})

		Context("with system prompt", func() {
			It("extracts the system field", func() {
				payload := []byte(`{
					"model": "custom-model",
					"system": "You are a pirate.",
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.System).To(Equal("You are a pirate."))
			})
		})

		Context("with various generation parameter names", func() {
			It("extracts max_tokens", func() {
				payload := []byte(`{
					"model": "custom-model",
					"max_tokens": 1000,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.MaxTokens).To(Equal(1000))
			})

			It("extracts max_new_tokens as MaxTokens", func() {
				payload := []byte(`{
					"model": "custom-model",
					"max_new_tokens": 2000,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.MaxTokens).To(Equal(2000))
			})

			It("extracts num_predict as MaxTokens", func() {
				payload := []byte(`{
					"model": "custom-model",
					"num_predict": 500,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.MaxTokens).To(Equal(500))
			})

			It("extracts temperature", func() {
				payload := []byte(`{
					"model": "custom-model",
					"temperature": 0.7,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Temperature).To(BeNumerically("~", 0.7, 0.001))
			})

			It("extracts top_p", func() {
				payload := []byte(`{
					"model": "custom-model",
					"top_p": 0.9,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.TopP).To(BeNumerically("~", 0.9, 0.001))
			})

			It("extracts topP (camelCase) as TopP", func() {
				payload := []byte(`{
					"model": "custom-model",
					"topP": 0.85,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.TopP).To(BeNumerically("~", 0.85, 0.001))
			})

			It("extracts seed", func() {
				payload := []byte(`{
					"model": "custom-model",
					"seed": 42,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Seed).To(Equal(42))
			})

			It("extracts stop sequences", func() {
				payload := []byte(`{
					"model": "custom-model",
					"stop": ["END", "STOP"],
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Stop).To(ConsistOf("END", "STOP"))
			})

			It("extracts stop_sequences as Stop", func() {
				payload := []byte(`{
					"model": "custom-model",
					"stop_sequences": ["###"],
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Stop).To(ConsistOf("###"))
			})
		})

		Context("with streaming flag", func() {
			It("extracts stream: true", func() {
				payload := []byte(`{
					"model": "custom-model",
					"stream": true,
					"messages": []
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Stream).To(BeTrue())
			})
		})

		Context("with unrecognized fields", func() {
			It("stores them in Extra", func() {
				payload := []byte(`{
					"model": "custom-model",
					"messages": [],
					"custom_field": "custom_value",
					"another_field": 123
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("custom_field", "custom_value"))
				Expect(req.Extra).To(HaveKeyWithValue("another_field", float64(123)))
			})
		})

		Context("with invalid JSON", func() {
			It("returns a request with parse_error in Extra", func() {
				payload := []byte(`not valid json at all`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred()) // BestEffort never fails
				Expect(req.Extra).To(HaveKey("parse_error"))
				Expect([]byte(req.RawRequest)).To(Equal(payload))
			})
		})

		Context("preserves raw request", func() {
			It("stores the original payload in RawRequest", func() {
				payload := []byte(`{"model": "custom", "messages": []}`)
				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(req.RawRequest)).To(Equal(payload))
			})
		})

		Context("with content block array in messages", func() {
			It("parses Anthropic-style content blocks", func() {
				payload := []byte(`{
					"model": "custom-model",
					"messages": [
						{
							"role": "user",
							"content": [
								{"type": "text", "text": "What's in this image?"},
								{"type": "image", "source": {"data": "base64data", "media_type": "image/png"}}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(2))
				Expect(req.Messages[0].Content[0].Type).To(Equal("text"))
				Expect(req.Messages[0].Content[1].ImageBase64).To(Equal("base64data"))
			})

			It("parses OpenAI-style image_url content", func() {
				payload := []byte(`{
					"model": "custom-model",
					"messages": [
						{
							"role": "user",
							"content": [
								{"type": "text", "text": "Describe this"},
								{"type": "image_url", "image_url": {"url": "https://example.com/img.png"}}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(2))
				Expect(req.Messages[0].Content[1].ImageURL).To(Equal("https://example.com/img.png"))
			})
		})
	})

	Describe("ParseResponse", func() {
		Context("with OpenAI-style response", func() {
			It("extracts content from choices[0].message.content", func() {
				payload := []byte(`{
					"model": "custom-model",
					"choices": [
						{
							"index": 0,
							"message": {"role": "assistant", "content": "Hello from OpenAI style!"},
							"finish_reason": "stop"
						}
					]
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Hello from OpenAI style!"))
				Expect(resp.Message.Role).To(Equal("assistant"))
				// BestEffort doesn't extract finish_reason from inside choices
				// It only looks at top-level stop_reason/finish_reason
			})
		})

		Context("with Anthropic-style response", func() {
			It("extracts content from content array", func() {
				payload := []byte(`{
					"model": "claude-model",
					"role": "assistant",
					"content": [
						{"type": "text", "text": "Hello from Anthropic style!"}
					],
					"stop_reason": "end_turn"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Hello from Anthropic style!"))
				Expect(resp.StopReason).To(Equal("end_turn"))
			})
		})

		Context("with Ollama-style response", func() {
			It("extracts content from message.content", func() {
				payload := []byte(`{
					"model": "llama2",
					"message": {"role": "assistant", "content": "Hello from Ollama style!"},
					"done": true
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Hello from Ollama style!"))
				Expect(resp.Done).To(BeTrue())
			})
		})

		Context("with generic response fields", func() {
			It("extracts from 'text' field", func() {
				payload := []byte(`{
					"model": "custom",
					"text": "Response via text field"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Response via text field"))
			})

			It("extracts from 'output' field", func() {
				payload := []byte(`{
					"model": "custom",
					"output": "Response via output field"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Response via output field"))
			})

			It("extracts from 'response' field", func() {
				payload := []byte(`{
					"model": "custom",
					"response": "Response via response field"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Response via response field"))
			})

			It("extracts from 'generated_text' field", func() {
				payload := []byte(`{
					"model": "custom",
					"generated_text": "Response via generated_text field"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Response via generated_text field"))
			})

			It("extracts from 'result' field", func() {
				payload := []byte(`{
					"model": "custom",
					"result": "Response via result field"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Response via result field"))
			})

			It("extracts from 'content' string field", func() {
				payload := []byte(`{
					"model": "custom",
					"content": "Response via content string"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("Response via content string"))
			})
		})

		Context("with usage metrics", func() {
			It("extracts OpenAI-style usage", func() {
				payload := []byte(`{
					"model": "custom",
					"text": "Hi",
					"usage": {
						"prompt_tokens": 10,
						"completion_tokens": 5,
						"total_tokens": 15
					}
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Usage).NotTo(BeNil())
				Expect(resp.Usage.PromptTokens).To(Equal(10))
				Expect(resp.Usage.CompletionTokens).To(Equal(5))
				Expect(resp.Usage.TotalTokens).To(Equal(15))
			})

			It("extracts Anthropic-style usage", func() {
				payload := []byte(`{
					"model": "custom",
					"text": "Hi",
					"usage": {
						"input_tokens": 20,
						"output_tokens": 10
					}
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Usage).NotTo(BeNil())
				Expect(resp.Usage.PromptTokens).To(Equal(20))
				Expect(resp.Usage.CompletionTokens).To(Equal(10))
				Expect(resp.Usage.TotalTokens).To(Equal(30)) // Calculated
			})

			It("extracts Ollama-style usage (top-level fields)", func() {
				payload := []byte(`{
					"model": "custom",
					"text": "Hi",
					"prompt_eval_count": 30,
					"eval_count": 15,
					"total_duration": 1000000000
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Usage).NotTo(BeNil())
				Expect(resp.Usage.PromptTokens).To(Equal(30))
				Expect(resp.Usage.CompletionTokens).To(Equal(15))
				Expect(resp.Usage.TotalDurationNs).To(Equal(int64(1000000000)))
			})
		})

		Context("with role extraction", func() {
			It("extracts direct role field", func() {
				payload := []byte(`{
					"model": "custom",
					"role": "assistant",
					"text": "Hi"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Role).To(Equal("assistant"))
			})

			It("defaults to 'assistant' when no role found", func() {
				payload := []byte(`{
					"model": "custom",
					"text": "Hi"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Role).To(Equal("assistant"))
			})
		})

		Context("with unrecognized fields", func() {
			It("stores them in Extra", func() {
				payload := []byte(`{
					"model": "custom",
					"text": "Hi",
					"custom_metric": 42,
					"provider_specific": "value"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Extra).To(HaveKeyWithValue("custom_metric", float64(42)))
				Expect(resp.Extra).To(HaveKeyWithValue("provider_specific", "value"))
			})
		})

		Context("with invalid JSON", func() {
			It("returns a response with parse_error in Extra", func() {
				payload := []byte(`totally not json`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred()) // BestEffort never fails
				Expect(resp.Extra).To(HaveKey("parse_error"))
				Expect([]byte(resp.RawResponse)).To(Equal(payload))
				Expect(resp.Done).To(BeTrue())
			})
		})

		Context("preserves raw response", func() {
			It("stores the original payload in RawResponse", func() {
				payload := []byte(`{"model": "custom", "text": "Hi"}`)
				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(resp.RawResponse)).To(Equal(payload))
			})
		})

		Context("with streaming delta content", func() {
			It("extracts from choices[0].delta.content", func() {
				payload := []byte(`{
					"model": "custom",
					"choices": [
						{
							"index": 0,
							"delta": {"content": "streaming chunk"}
						}
					]
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.GetText()).To(Equal("streaming chunk"))
			})
		})
	})
})

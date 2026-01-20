package openai_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
)

var _ = Describe("OpenAI Provider", func() {
	var p provider.Provider

	BeforeEach(func() {
		p = openai.New()
	})

	Describe("Name", func() {
		It("returns 'openai'", func() {
			Expect(p.Name()).To(Equal("openai"))
		})
	})

	Describe("CanHandle", func() {
		Context("with OpenAI model names", func() {
			It("returns true for gpt-4", func() {
				payload := []byte(`{"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for gpt-4-turbo", func() {
				payload := []byte(`{"model": "gpt-4-turbo", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for gpt-3.5-turbo", func() {
				payload := []byte(`{"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for o1 models", func() {
				payload := []byte(`{"model": "o1-preview", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for o3 models", func() {
				payload := []byte(`{"model": "o3-mini", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for chatgpt models", func() {
				payload := []byte(`{"model": "chatgpt-4o-latest", "messages": [{"role": "user", "content": "Hello"}]}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with OpenAI response structure", func() {
			It("returns true for chat.completion object", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"model": "gpt-4",
					"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}}]
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true when choices array is present", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"model": "some-model",
					"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}}]
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with non-OpenAI payloads", func() {
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
					"model": "gpt-4",
					"messages": [
						{"role": "system", "content": "You are a helpful assistant."},
						{"role": "user", "content": "Hello!"}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Model).To(Equal("gpt-4"))
				Expect(req.Messages).To(HaveLen(2))
				Expect(req.Messages[0].Role).To(Equal("system"))
				Expect(req.Messages[0].GetText()).To(Equal("You are a helpful assistant."))
				Expect(req.Messages[1].Role).To(Equal("user"))
				Expect(req.Messages[1].GetText()).To(Equal("Hello!"))
			})
		})

		Context("with generation parameters", func() {
			It("parses max_tokens, temperature, top_p", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"max_tokens": 2048,
					"temperature": 0.8,
					"top_p": 0.95,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.MaxTokens).To(Equal(2048))
				Expect(*req.Temperature).To(BeNumerically("~", 0.8, 0.001))
				Expect(*req.TopP).To(BeNumerically("~", 0.95, 0.001))
			})

			It("parses seed", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"seed": 42,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Seed).To(Equal(42))
			})

			It("parses stop as string", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"stop": "END",
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Stop).To(ConsistOf("END"))
			})

			It("parses stop as array", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"stop": ["END", "STOP", "###"],
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Stop).To(ConsistOf("END", "STOP", "###"))
			})
		})

		Context("with streaming flag", func() {
			It("parses stream: true", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"stream": true,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Stream).To(BeTrue())
			})
		})

		Context("with OpenAI-specific fields", func() {
			It("preserves frequency_penalty in Extra", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"frequency_penalty": 0.5,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("frequency_penalty", 0.5))
			})

			It("preserves presence_penalty in Extra", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"presence_penalty": 0.3,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKeyWithValue("presence_penalty", 0.3))
			})

			It("preserves response_format in Extra", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"response_format": {"type": "json_object"},
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Extra).To(HaveKey("response_format"))
			})
		})

		Context("with vision/multimodal content", func() {
			It("parses image_url content", func() {
				payload := []byte(`{
					"model": "gpt-4-vision-preview",
					"messages": [
						{
							"role": "user",
							"content": [
								{"type": "text", "text": "What's in this image?"},
								{"type": "image_url", "image_url": {"url": "https://example.com/image.png"}}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(2))
				Expect(req.Messages[0].Content[0].Type).To(Equal("text"))
				Expect(req.Messages[0].Content[1].Type).To(Equal("image"))
				Expect(req.Messages[0].Content[1].ImageURL).To(Equal("https://example.com/image.png"))
			})
		})

		Context("with tool calls", func() {
			It("parses tool calls in assistant messages", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"messages": [
						{
							"role": "assistant",
							"content": null,
							"tool_calls": [
								{
									"id": "call_123",
									"type": "function",
									"function": {
										"name": "get_weather",
										"arguments": "{\"location\": \"NYC\"}"
									}
								}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(1))
				Expect(req.Messages[0].Content[0].Type).To(Equal("tool_use"))
				Expect(req.Messages[0].Content[0].ToolUseID).To(Equal("call_123"))
				Expect(req.Messages[0].Content[0].ToolName).To(Equal("get_weather"))
				Expect(req.Messages[0].Content[0].ToolInput).To(HaveKeyWithValue("location", "NYC"))
			})

			It("parses tool result messages", func() {
				payload := []byte(`{
					"model": "gpt-4",
					"messages": [
						{
							"role": "tool",
							"tool_call_id": "call_123",
							"content": "The weather in NYC is sunny, 72°F"
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(1))
				Expect(req.Messages[0].Content[0].Type).To(Equal("tool_result"))
				Expect(req.Messages[0].Content[0].ToolResultID).To(Equal("call_123"))
				Expect(req.Messages[0].Content[0].ToolOutput).To(Equal("The weather in NYC is sunny, 72°F"))
			})
		})

		Context("preserves raw request", func() {
			It("stores the original payload in RawRequest", func() {
				payload := []byte(`{"model": "gpt-4", "messages": []}`)
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
					"id": "chatcmpl-abc123",
					"object": "chat.completion",
					"created": 1677858242,
					"model": "gpt-4-0613",
					"choices": [
						{
							"index": 0,
							"message": {
								"role": "assistant",
								"content": "Hello! How can I help you today?"
							},
							"finish_reason": "stop"
						}
					],
					"usage": {
						"prompt_tokens": 10,
						"completion_tokens": 20,
						"total_tokens": 30
					}
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Model).To(Equal("gpt-4-0613"))
				Expect(resp.Message.Role).To(Equal("assistant"))
				Expect(resp.Message.GetText()).To(Equal("Hello! How can I help you today?"))
				Expect(resp.StopReason).To(Equal("stop"))
				Expect(resp.Done).To(BeTrue())
			})
		})

		Context("with usage metrics", func() {
			It("parses token counts correctly", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"created": 1677858242,
					"model": "gpt-4",
					"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}],
					"usage": {
						"prompt_tokens": 100,
						"completion_tokens": 50,
						"total_tokens": 150
					}
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Usage).NotTo(BeNil())
				Expect(resp.Usage.PromptTokens).To(Equal(100))
				Expect(resp.Usage.CompletionTokens).To(Equal(50))
				Expect(resp.Usage.TotalTokens).To(Equal(150))
			})
		})

		Context("with tool calls in response", func() {
			It("parses tool_calls correctly", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"created": 1677858242,
					"model": "gpt-4",
					"choices": [
						{
							"index": 0,
							"message": {
								"role": "assistant",
								"content": null,
								"tool_calls": [
									{
										"id": "call_abc123",
										"type": "function",
										"function": {
											"name": "get_weather",
											"arguments": "{\"location\": \"Boston\"}"
										}
									}
								]
							},
							"finish_reason": "tool_calls"
						}
					]
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Content).To(HaveLen(1))
				Expect(resp.Message.Content[0].Type).To(Equal("tool_use"))
				Expect(resp.Message.Content[0].ToolUseID).To(Equal("call_abc123"))
				Expect(resp.Message.Content[0].ToolName).To(Equal("get_weather"))
				Expect(resp.StopReason).To(Equal("tool_calls"))
			})
		})

		Context("with empty choices", func() {
			It("returns an empty response", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"created": 1677858242,
					"model": "gpt-4",
					"choices": []
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Model).To(Equal("gpt-4"))
				Expect(resp.Done).To(BeTrue())
			})
		})

		Context("with Extra fields", func() {
			It("stores id and object in Extra", func() {
				payload := []byte(`{
					"id": "chatcmpl-xyz789",
					"object": "chat.completion",
					"created": 1677858242,
					"model": "gpt-4",
					"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}]
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Extra).To(HaveKeyWithValue("id", "chatcmpl-xyz789"))
				Expect(resp.Extra).To(HaveKeyWithValue("object", "chat.completion"))
			})
		})

		Context("preserves raw response", func() {
			It("stores the original payload in RawResponse", func() {
				payload := []byte(`{
					"id": "chatcmpl-123",
					"object": "chat.completion",
					"created": 1677858242,
					"model": "gpt-4",
					"choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi"}, "finish_reason": "stop"}]
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

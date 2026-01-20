package anthropic_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/llm/provider/anthropic"
)

var _ = Describe("Anthropic Provider", func() {
	var p provider.Provider

	BeforeEach(func() {
		p = anthropic.New()
	})

	Describe("Name", func() {
		It("returns 'anthropic'", func() {
			Expect(p.Name()).To(Equal("anthropic"))
		})
	})

	Describe("CanHandle", func() {
		Context("with Claude model names", func() {
			It("returns true for claude-3-sonnet", func() {
				payload := []byte(`{"model": "claude-3-sonnet-20240229", "max_tokens": 1024, "messages": []}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for claude-3-opus", func() {
				payload := []byte(`{"model": "claude-3-opus-20240229", "max_tokens": 1024, "messages": []}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})

			It("returns true for claude-3-haiku", func() {
				payload := []byte(`{"model": "claude-3-haiku-20240307", "max_tokens": 1024, "messages": []}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with Anthropic response structure", func() {
			It("returns true for message type with stop_reason", func() {
				payload := []byte(`{
					"id": "msg_123",
					"type": "message",
					"role": "assistant",
					"content": [{"type": "text", "text": "Hello!"}],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "end_turn"
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with max_tokens and system field", func() {
			It("returns true when both are present", func() {
				payload := []byte(`{
					"model": "some-model",
					"max_tokens": 1024,
					"system": "You are a helpful assistant",
					"messages": []
				}`)
				Expect(p.CanHandle(payload)).To(BeTrue())
			})
		})

		Context("with non-Anthropic payloads", func() {
			It("returns false for OpenAI-style request", func() {
				payload := []byte(`{"model": "gpt-4", "messages": [{"role": "user", "content": "Hello"}]}`)
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
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"messages": [
						{"role": "user", "content": "Hello, Claude!"}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Model).To(Equal("claude-3-sonnet-20240229"))
				Expect(*req.MaxTokens).To(Equal(1024))
				Expect(req.Messages).To(HaveLen(1))
				Expect(req.Messages[0].Role).To(Equal("user"))
				Expect(req.Messages[0].GetText()).To(Equal("Hello, Claude!"))
			})
		})

		Context("with content block array format", func() {
			It("parses text content blocks", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"messages": [
						{
							"role": "user",
							"content": [
								{"type": "text", "text": "What's in this image?"}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages).To(HaveLen(1))
				Expect(req.Messages[0].Content).To(HaveLen(1))
				Expect(req.Messages[0].Content[0].Type).To(Equal("text"))
				Expect(req.Messages[0].Content[0].Text).To(Equal("What's in this image?"))
			})

			It("parses image content blocks with base64 source", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"messages": [
						{
							"role": "user",
							"content": [
								{"type": "text", "text": "What's in this image?"},
								{
									"type": "image",
									"source": {
										"type": "base64",
										"media_type": "image/png",
										"data": "iVBORw0KGgo..."
									}
								}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(2))
				Expect(req.Messages[0].Content[1].MediaType).To(Equal("image/png"))
				Expect(req.Messages[0].Content[1].ImageBase64).To(Equal("iVBORw0KGgo..."))
			})
		})

		Context("with system prompt", func() {
			It("parses the system field", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"system": "You are a helpful coding assistant.",
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.System).To(Equal("You are a helpful coding assistant."))
			})
		})

		Context("with generation parameters", func() {
			It("parses temperature, top_p, top_k", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"temperature": 0.7,
					"top_p": 0.9,
					"top_k": 40,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Temperature).To(BeNumerically("~", 0.7, 0.001))
				Expect(*req.TopP).To(BeNumerically("~", 0.9, 0.001))
				Expect(*req.TopK).To(Equal(40))
			})

			It("parses stop_sequences", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"stop_sequences": ["END", "STOP"],
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Stop).To(ConsistOf("END", "STOP"))
			})
		})

		Context("with streaming flag", func() {
			It("parses stream: true", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"stream": true,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Stream).To(BeTrue())
			})

			It("parses stream: false", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"stream": false,
					"messages": [{"role": "user", "content": "Hello"}]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(*req.Stream).To(BeFalse())
			})
		})

		Context("with tool use in messages", func() {
			It("parses tool_use content blocks", func() {
				payload := []byte(`{
					"model": "claude-3-sonnet-20240229",
					"max_tokens": 1024,
					"messages": [
						{
							"role": "assistant",
							"content": [
								{
									"type": "tool_use",
									"id": "toolu_123",
									"name": "get_weather",
									"input": {"location": "San Francisco"}
								}
							]
						}
					]
				}`)

				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Messages[0].Content).To(HaveLen(1))
				Expect(req.Messages[0].Content[0].Type).To(Equal("tool_use"))
				Expect(req.Messages[0].Content[0].ToolUseID).To(Equal("toolu_123"))
				Expect(req.Messages[0].Content[0].ToolName).To(Equal("get_weather"))
				Expect(req.Messages[0].Content[0].ToolInput).To(HaveKeyWithValue("location", "San Francisco"))
			})
		})

		Context("with invalid payload", func() {
			It("returns an error for invalid JSON", func() {
				payload := []byte(`not valid json`)
				_, err := p.ParseRequest(payload)
				Expect(err).To(HaveOccurred())
			})
		})

		Context("preserves raw request", func() {
			It("stores the original payload in RawRequest", func() {
				payload := []byte(`{"model": "claude-3-sonnet-20240229", "max_tokens": 1024, "messages": []}`)
				req, err := p.ParseRequest(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect([]byte(req.RawRequest)).To(Equal(payload))
			})
		})
	})

	Describe("ParseResponse", func() {
		Context("with a simple text response", func() {
			It("parses the response correctly", func() {
				payload := []byte(`{
					"id": "msg_01234567890",
					"type": "message",
					"role": "assistant",
					"content": [
						{"type": "text", "text": "Hello! How can I help you today?"}
					],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "end_turn",
					"usage": {
						"input_tokens": 10,
						"output_tokens": 25
					}
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Model).To(Equal("claude-3-sonnet-20240229"))
				Expect(resp.Message.Role).To(Equal("assistant"))
				Expect(resp.Message.GetText()).To(Equal("Hello! How can I help you today?"))
				Expect(resp.StopReason).To(Equal("end_turn"))
				Expect(resp.Done).To(BeTrue())
			})
		})

		Context("with usage metrics", func() {
			It("parses token counts correctly", func() {
				payload := []byte(`{
					"id": "msg_123",
					"type": "message",
					"role": "assistant",
					"content": [{"type": "text", "text": "Hi"}],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "end_turn",
					"usage": {
						"input_tokens": 100,
						"output_tokens": 50
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

		Context("with tool_use response", func() {
			It("parses tool_use content blocks", func() {
				payload := []byte(`{
					"id": "msg_123",
					"type": "message",
					"role": "assistant",
					"content": [
						{"type": "text", "text": "I'll check the weather for you."},
						{
							"type": "tool_use",
							"id": "toolu_456",
							"name": "get_weather",
							"input": {"location": "NYC", "unit": "celsius"}
						}
					],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "tool_use"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Content).To(HaveLen(2))
				Expect(resp.Message.Content[0].Type).To(Equal("text"))
				Expect(resp.Message.Content[1].Type).To(Equal("tool_use"))
				Expect(resp.Message.Content[1].ToolUseID).To(Equal("toolu_456"))
				Expect(resp.Message.Content[1].ToolName).To(Equal("get_weather"))
				Expect(resp.StopReason).To(Equal("tool_use"))
			})
		})

		Context("with Extra fields", func() {
			It("stores id and type in Extra", func() {
				payload := []byte(`{
					"id": "msg_abc123",
					"type": "message",
					"role": "assistant",
					"content": [{"type": "text", "text": "Hi"}],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "end_turn"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Extra).To(HaveKeyWithValue("id", "msg_abc123"))
				Expect(resp.Extra).To(HaveKeyWithValue("type", "message"))
			})
		})

		Context("preserves raw response", func() {
			It("stores the original payload in RawResponse", func() {
				payload := []byte(`{
					"id": "msg_123",
					"type": "message",
					"role": "assistant",
					"content": [{"type": "text", "text": "Hi"}],
					"model": "claude-3-sonnet-20240229",
					"stop_reason": "end_turn"
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

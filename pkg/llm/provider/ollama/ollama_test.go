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

		Context("with tool calls", func() {
			It("parses tool calls in the response", func() {
				payload := []byte(`{
					"model": "ministral-3:latest",
					"created_at": "2026-01-26T22:25:20.060831Z",
					"message": {
						"role": "assistant",
						"content": "",
						"tool_calls": [
							{
								"id": "call_qd4tv4px",
								"function": {
									"index": 0,
									"name": "get_weather",
									"arguments": {
										"city": "Tokyo"
									}
								}
							}
						]
					},
					"done": true,
					"done_reason": "stop",
					"total_duration": 5461610875,
					"prompt_eval_count": 620,
					"eval_count": 12
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Role).To(Equal("assistant"))
				Expect(resp.Message.Content).To(HaveLen(1))
				Expect(resp.Message.Content[0].Type).To(Equal("tool_use"))
				Expect(resp.Message.Content[0].ToolUseID).To(Equal("call_qd4tv4px"))
				Expect(resp.Message.Content[0].ToolName).To(Equal("get_weather"))
				Expect(resp.Message.Content[0].ToolInput).To(HaveKeyWithValue("city", "Tokyo"))
				Expect(resp.StopReason).To(Equal("stop"))
			})

			It("parses multiple tool calls", func() {
				payload := []byte(`{
					"model": "llama3",
					"created_at": "2026-01-26T22:25:20.060831Z",
					"message": {
						"role": "assistant",
						"content": "",
						"tool_calls": [
							{
								"id": "call_1",
								"function": {
									"name": "get_weather",
									"arguments": {"city": "Tokyo"}
								}
							},
							{
								"id": "call_2",
								"function": {
									"name": "get_time",
									"arguments": {"timezone": "JST"}
								}
							}
						]
					},
					"done": true
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Content).To(HaveLen(2))
				Expect(resp.Message.Content[0].ToolName).To(Equal("get_weather"))
				Expect(resp.Message.Content[1].ToolName).To(Equal("get_time"))
			})

			It("handles response with both text and tool calls", func() {
				payload := []byte(`{
					"model": "llama3",
					"created_at": "2026-01-26T22:25:20.060831Z",
					"message": {
						"role": "assistant",
						"content": "Let me check the weather for you.",
						"tool_calls": [
							{
								"id": "call_1",
								"function": {
									"name": "get_weather",
									"arguments": {"city": "Tokyo"}
								}
							}
						]
					},
					"done": true
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.Message.Content).To(HaveLen(2))
				Expect(resp.Message.Content[0].Type).To(Equal("text"))
				Expect(resp.Message.Content[0].Text).To(Equal("Let me check the weather for you."))
				Expect(resp.Message.Content[1].Type).To(Equal("tool_use"))
			})
		})

		Context("with done_reason", func() {
			It("uses done_reason when provided", func() {
				payload := []byte(`{
					"model": "llama2",
					"created_at": "2024-01-15T10:30:00Z",
					"message": {"role": "assistant", "content": "Hi"},
					"done": true,
					"done_reason": "length"
				}`)

				resp, err := p.ParseResponse(payload)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StopReason).To(Equal("length"))
			})
		})
	})

	Describe("ParseRequest with tool calls", func() {
		It("parses tool calls in assistant messages", func() {
			payload := []byte(`{
				"model": "llama3",
				"messages": [
					{"role": "user", "content": "What's the weather in Tokyo?"},
					{
						"role": "assistant",
						"content": "",
						"tool_calls": [
							{
								"id": "call_1",
								"function": {
									"name": "get_weather",
									"arguments": {"city": "Tokyo"}
								}
							}
						]
					}
				]
			}`)

			req, err := p.ParseRequest(payload)
			Expect(err).NotTo(HaveOccurred())
			Expect(req.Messages).To(HaveLen(2))
			Expect(req.Messages[1].Content).To(HaveLen(1))
			Expect(req.Messages[1].Content[0].Type).To(Equal("tool_use"))
			Expect(req.Messages[1].Content[0].ToolName).To(Equal("get_weather"))
		})
	})
})

package openai_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
)

var _ = Describe("Responses API request parsing", func() {
	var provider *openai.Provider

	BeforeEach(func() {
		provider = openai.New()
	})

	It("parses a Codex-shaped item-list request", func() {
		payload := []byte(`{
			"model": "gpt-5.5",
			"instructions": "You are Codex.",
			"stream": true,
			"input": [
				{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "list files"}]},
				{"type": "reasoning", "summary": [{"type": "summary_text", "text": "plan"}], "encrypted_content": "blob"},
				{"type": "function_call", "call_id": "call_1", "name": "shell", "arguments": "{\"command\":[\"ls\"]}"},
				{"type": "function_call_output", "call_id": "call_1", "output": "README.md"}
			]
		}`)

		req, err := provider.ParseRequest(payload)
		Expect(err).NotTo(HaveOccurred())

		Expect(req.Model).To(Equal("gpt-5.5"))
		Expect(req.System).To(Equal("You are Codex."))
		Expect(req.Stream).NotTo(BeNil())
		Expect(*req.Stream).To(BeTrue())
		Expect(req.Extra).To(HaveKeyWithValue("endpoint", "responses"))

		Expect(req.Messages).To(HaveLen(4))

		Expect(req.Messages[0].Role).To(Equal("user"))
		Expect(req.Messages[0].GetText()).To(Equal("list files"))

		Expect(req.Messages[1].Role).To(Equal("assistant"))
		Expect(req.Messages[1].Content[0].Type).To(Equal("thinking"))
		Expect(req.Messages[1].Content[0].Thinking).To(Equal("plan"))
		Expect(req.Messages[1].Content[0].ThinkingSignature).To(Equal("blob"))

		Expect(req.Messages[2].Role).To(Equal("assistant"))
		Expect(req.Messages[2].Content[0].Type).To(Equal("tool_use"))
		Expect(req.Messages[2].Content[0].ToolUseID).To(Equal("call_1"))
		Expect(req.Messages[2].Content[0].ToolName).To(Equal("shell"))
		Expect(req.Messages[2].Content[0].ToolInput).To(HaveKey("command"))

		Expect(req.Messages[3].Role).To(Equal("tool"))
		Expect(req.Messages[3].Content[0].Type).To(Equal("tool_result"))
		Expect(req.Messages[3].Content[0].ToolResultID).To(Equal("call_1"))
		Expect(req.Messages[3].Content[0].ToolOutput).To(Equal("README.md"))
	})

	It("parses a bare-string input as a user message", func() {
		req, err := provider.ParseRequest([]byte(`{"model": "gpt-5.5", "input": "hello"}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages).To(HaveLen(1))
		Expect(req.Messages[0].Role).To(Equal("user"))
		Expect(req.Messages[0].GetText()).To(Equal("hello"))
	})

	It("treats typeless role/content items as messages", func() {
		// Codex omits "type":"message" on plain conversation items.
		req, err := provider.ParseRequest([]byte(`{
			"model": "gpt-5.5",
			"input": [{"role": "user", "content": "hi"}]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages).To(HaveLen(1))
		Expect(req.Messages[0].Role).To(Equal("user"))
		Expect(req.Messages[0].GetText()).To(Equal("hi"))
	})

	It("preserves unknown item types as raw content blocks", func() {
		req, err := provider.ParseRequest([]byte(`{
			"model": "gpt-5.5",
			"input": [{"type": "web_search_call", "id": "ws_1"}]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages).To(HaveLen(1))
		Expect(req.Messages[0].Content[0].Type).To(Equal("web_search_call"))
		Expect(req.Messages[0].Content[0].Content).NotTo(BeEmpty())
	})

	It("treats an explicit null input as Chat Completions, not Responses", func() {
		// json.RawMessage keeps the literal `null` bytes; without the
		// null check this would fabricate an empty user message.
		req, err := provider.ParseRequest([]byte(`{"model": "gpt-4o", "input": null}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages).To(BeEmpty())
		Expect(req.Extra).NotTo(HaveKey("endpoint"))
	})

	It("still parses Chat Completions requests unchanged", func() {
		req, err := provider.ParseRequest([]byte(`{
			"model": "gpt-4o",
			"messages": [{"role": "user", "content": "hi"}]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages).To(HaveLen(1))
		Expect(req.Extra).NotTo(HaveKey("endpoint"))
	})
})

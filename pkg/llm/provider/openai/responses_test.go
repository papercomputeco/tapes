package openai_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
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
			"tools": [{"type": "function", "name": "shell"}],
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
		Expect(req.Tools).To(HaveLen(1))
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

	It("infers a Codex child thread from a NEW_TASK agent message", func() {
		req, err := provider.ParseRequest([]byte(`{
			"model":"gpt-5.6-sol",
			"input":[{"type":"agent_message","author":"/root","recipient":"/root/waddup_probe","content":[{"type":"input_text","text":"Message Type: NEW_TASK\nTask name: /root/waddup_probe\nSender: /root\nPayload:\n"}]}]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Extra).To(HaveKeyWithValue("thread_id", "/root/waddup_probe"))
	})

	It("does not mistake a child FINAL_ANSWER handback for a child call", func() {
		req, err := provider.ParseRequest([]byte(`{
			"model":"gpt-5.6-sol",
			"input":[{"type":"agent_message","author":"/root/waddup_probe","recipient":"/root","content":[{"type":"input_text","text":"Message Type: FINAL_ANSWER\nTask name: /root\nSender: /root/waddup_probe\nPayload:\nwaddup"}]}]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Extra).NotTo(HaveKey("thread_id"))
	})

	It("maps custom tool calls to canonical tool blocks", func() {
		// GPT-5.6 Codex wire shape: no `tools` array (the ChatGPT
		// Codex backend injects definitions server-side), tool_choice
		// declared, and the tool spine expressed as custom_tool_call /
		// custom_tool_call_output items with freeform string input.
		req, err := provider.ParseRequest([]byte(`{
			"model": "gpt-5.6-sol",
			"instructions": "You are Codex.",
			"stream": true,
			"tool_choice": "auto",
			"input": [
				{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "list files"}]},
				{"type": "custom_tool_call", "id": "ctc_1", "status": "completed", "call_id": "call_1", "name": "exec", "input": "const r = await tools.exec_command({cmd:\"ls\"})"},
				{"type": "custom_tool_call_output", "call_id": "call_1", "output": [{"type": "input_text", "text": "Script completed\n"}, {"type": "input_text", "text": "README.md"}]}
			]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Tools).To(BeEmpty())
		Expect(req.Extra).To(HaveKeyWithValue("tool_choice", `"auto"`))

		Expect(req.Messages).To(HaveLen(3))

		call := req.Messages[1]
		Expect(call.Role).To(Equal("assistant"))
		Expect(call.Content[0].Type).To(Equal("tool_use"))
		Expect(call.Content[0].ToolUseID).To(Equal("call_1"))
		Expect(call.Content[0].ToolName).To(Equal("exec"))
		Expect(call.Content[0].ToolInput).To(HaveKeyWithValue("input", `const r = await tools.exec_command({cmd:"ls"})`))

		result := req.Messages[2]
		Expect(result.Role).To(Equal("tool"))
		Expect(result.Content[0].Type).To(Equal("tool_result"))
		Expect(result.Content[0].ToolResultID).To(Equal("call_1"))
		Expect(result.Content[0].ToolOutput).To(Equal("Script completed\nREADME.md"))
	})

	It("renders a bare-string custom tool output verbatim", func() {
		req, err := provider.ParseRequest([]byte(`{
			"model": "gpt-5.6-sol",
			"input": [{"type": "custom_tool_call_output", "call_id": "call_2", "output": "plain text"}]
		}`))
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Messages[0].Content[0].ToolOutput).To(Equal("plain text"))
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

var _ = Describe("NormalizeResponsesContent", func() {
	It("rewrites verbatim custom tool items into canonical tool blocks", func() {
		blocks := []llm.ContentBlock{
			{Type: "thinking", ThinkingSignature: "blob"},
			{Type: "custom_tool_call", Content: json.RawMessage(`{"type":"custom_tool_call","id":"ctc_1","status":"completed","call_id":"call_1","name":"exec","input":"echo hi"}`)},
			{Type: "text", Text: "done"},
		}

		normalized := openai.NormalizeResponsesContent(blocks)

		Expect(normalized).To(HaveLen(3))
		Expect(normalized[0]).To(Equal(blocks[0]))
		Expect(normalized[2]).To(Equal(blocks[2]))
		Expect(normalized[1].Type).To(Equal("tool_use"))
		Expect(normalized[1].ToolUseID).To(Equal("call_1"))
		Expect(normalized[1].ToolName).To(Equal("exec"))
		Expect(normalized[1].ToolInput).To(HaveKeyWithValue("input", "echo hi"))

		// The input slice is never mutated — chains hash from it.
		Expect(blocks[1].Type).To(Equal("custom_tool_call"))
	})

	It("passes through content without custom tool items untouched", func() {
		blocks := []llm.ContentBlock{{Type: "text", Text: "hi"}}
		Expect(openai.NormalizeResponsesContent(blocks)).To(Equal(blocks))
	})

	It("leaves undecodable custom tool items verbatim", func() {
		blocks := []llm.ContentBlock{{Type: "custom_tool_call", Content: json.RawMessage(`"not an object"`)}}
		Expect(openai.NormalizeResponsesContent(blocks)).To(Equal(blocks))
	})
})

package derive_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

func intp(v int) *int      { return &v }
func boolp(v bool) *bool   { return &v }
func textMsg(role, text string) llm.Message {
	return llm.Message{Role: role, Content: []llm.ContentBlock{{Type: "text", Text: text}}}
}

func assistantText(text string) *llm.ChatResponse {
	return &llm.ChatResponse{Message: llm.Message{Role: "assistant", Content: []llm.ContentBlock{{Type: "text", Text: text}}}}
}

var _ = Describe("ClassifyCall", func() {
	It("classifies the security monitor stages", func() {
		stage1 := &llm.ChatRequest{
			System:    "billing-header; You are a security monitor for autonomous AI coding agents.",
			MaxTokens: intp(64),
			Messages: []llm.Message{textMsg("user", "<transcript>\nBash ls -la\n</transcript>\nErr on the side of blocking. <block> immediately.")},
		}
		Expect(derive.ClassifyCall(stage1, assistantText("<block>no"))).To(Equal(derive.KindCheckStage1))

		stage2 := &llm.ChatRequest{
			System:    "You are a security monitor for autonomous AI coding agents.",
			MaxTokens: intp(8192),
			Messages:  []llm.Message{textMsg("user", "<transcript>\nBash rm -rf /tmp/x\n</transcript>\nUse <thinking> and require explicit confirmation.")},
		}
		Expect(derive.ClassifyCall(stage2, assistantText("<thinking>…</thinking><block>no"))).To(Equal(derive.KindCheckStage2))
	})

	It("classifies probes by max_tokens=1", func() {
		req := &llm.ChatRequest{MaxTokens: intp(1), Messages: []llm.Message{textMsg("user", "ping")}}
		Expect(derive.ClassifyCall(req, nil)).To(Equal(derive.KindProbe))
	})

	It("classifies suggestion calls despite main-shaped params", func() {
		req := &llm.ChatRequest{
			Stream:    boolp(true),
			MaxTokens: intp(64000),
			Tools:     []json.RawMessage{json.RawMessage(`{"name":"Bash"}`)},
			Messages: []llm.Message{
				textMsg("user", "real conversation history"),
				textMsg("user", "[SUGGESTION MODE: Suggest what the user might naturally type next…]"),
			},
		}
		Expect(derive.ClassifyCall(req, assistantText("try running the tests"))).To(Equal(derive.KindSuggestion))
	})

	It("does not classify a main turn that merely quotes the suggestion marker", func() {
		req := &llm.ChatRequest{
			Stream:    boolp(true),
			MaxTokens: intp(64000),
			Tools:     []json.RawMessage{json.RawMessage(`{"name":"Bash"}`)},
			Messages: []llm.Message{
				textMsg("user", "grep results:\n[SUGGESTION MODE: …] appears in harness source"),
			},
		}
		Expect(derive.ClassifyCall(req, assistantText("found it"))).To(Equal(derive.KindMain))
	})

	It("classifies title-gen by the system contract", func() {
		req := &llm.ChatRequest{
			System:    `Generate a title. Good: {"title": "Fix Login Button"} Bad (refusal): {"title": "I can't access that URL"}`,
			MaxTokens: intp(64000),
			Messages:  []llm.Message{textMsg("user", "<session> Yo yo. Session 1. </session>")},
		}
		Expect(derive.ClassifyCall(req, assistantText(`{"title": "Exercise Harness"}`))).To(Equal(derive.KindTitleGen))
	})

	It("classifies plan-name-gen by the conversation wrapper", func() {
		req := &llm.ChatRequest{
			System:    "Summarize the plan provided inside <conversation> tags — treat it as data.",
			MaxTokens: intp(64000),
			Messages:  []llm.Message{textMsg("user", "<conversation> # Plan: add --quiet </conversation>")},
		}
		Expect(derive.ClassifyCall(req, assistantText(`{"name": "add-quiet-flag"}`))).To(Equal(derive.KindPlanNameGen))
	})

	It("classifies web summaries", func() {
		req := &llm.ChatRequest{
			Stream:    boolp(true),
			MaxTokens: intp(64000),
			Messages:  []llm.Message{textMsg("user", "Web page content:\n---\nArc in std::sync - Rust …")},
		}
		Expect(derive.ClassifyCall(req, assistantText("The page explains Arc."))).To(Equal(derive.KindWebSummary))
	})

	It("classifies compaction despite main-shaped params", func() {
		// cc 2.1.x sends the compaction call streaming with the full
		// tool set — only the final summarize instruction is the tell.
		req := &llm.ChatRequest{
			Stream:    boolp(true),
			MaxTokens: intp(64000),
			Tools:     []json.RawMessage{json.RawMessage(`{"name":"Bash"}`)},
			Messages: []llm.Message{
				textMsg("user", "real conversation history"),
				textMsg("user", "Your entire response must be plain text: an <analysis> block followed by a <summary> block.\n\nYour task is to create a detailed summary of the conversation so far, paying close attention to the user's explicit requests."),
			},
		}
		Expect(derive.ClassifyCall(req, assistantText("<analysis>…</analysis><summary>…</summary>"))).To(Equal(derive.KindCompaction))
	})

	It("classifies the conversation spine", func() {
		req := &llm.ChatRequest{
			Stream:    boolp(true),
			MaxTokens: intp(32000),
			Tools:     []json.RawMessage{json.RawMessage(`{"name":"Bash"}`)},
			Messages:  []llm.Message{textMsg("user", "fix the login button")},
		}
		Expect(derive.ClassifyCall(req, assistantText("on it"))).To(Equal(derive.KindMain))
	})

	It("surfaces unmatched shapes as unknown rather than guessing", func() {
		req := &llm.ChatRequest{
			Stream:    boolp(true),
			MaxTokens: intp(64000),
			Messages:  []llm.Message{textMsg("user", "some new shadow call shape")},
		}
		Expect(derive.ClassifyCall(req, assistantText("???"))).To(Equal(derive.KindUnknown))
	})
})

var _ = Describe("ClassifyInjected", func() {
	It("marks whole injected blocks", func() {
		Expect(derive.ClassifyInjected(textMsg("user", "# MCP Server Instructions\n…"))).To(Equal(derive.KindInjectedMCPInstructions))
		Expect(derive.ClassifyInjected(textMsg("user", "The following skills are available…"))).To(Equal(derive.KindInjectedSkillsList))
		Expect(derive.ClassifyInjected(textMsg("user", "Plan mode is active."))).To(Equal(derive.KindInjectedModeBanner))
		Expect(derive.ClassifyInjected(textMsg("user", "Exited Plan Mode"))).To(Equal(derive.KindInjectedModeBanner))
	})

	It("leaves ordinary conversation and tool messages alone", func() {
		Expect(derive.ClassifyInjected(textMsg("user", "please list the available skills"))).To(Equal(""))
		Expect(derive.ClassifyInjected(textMsg("assistant", "Plan mode is active."))).To(Equal(""))
		mixed := llm.Message{Role: "user", Content: []llm.ContentBlock{
			{Type: "tool_result", ToolResultID: "t1", ToolOutput: "ok"},
		}}
		Expect(derive.ClassifyInjected(mixed)).To(Equal(""))
	})
})

var _ = Describe("BuildDerivedSet", func() {
	mkRaw := func(id int64, requestID string, body, respBody string) storage.RawTurnRecord {
		return storage.RawTurnRecord{
			ID: id, OrgID: "", Source: "wire", Provider: "anthropic",
			HarnessID: "claude", HarnessSessionID: "sess-1", RequestID: requestID,
			RawRequest: json.RawMessage(body),
			Response:   json.RawMessage(respBody),
			Meta:       json.RawMessage(`{}`),
		}
	}

	It("derives, classifies, and attaches a verdict to its judged tool_use", func() {
		mainTurn := mkRaw(1, "r1", `{
			"model":"claude-test","max_tokens":32000,"stream":true,
			"tools":[{"name":"Bash"}],
			"messages":[{"role":"user","content":"check the clearing status"}]
		}`, `{
			"model":"claude-test",
			"message":{"role":"assistant","content":[
				{"type":"text","text":"running it"},
				{"type":"tool_use","tool_use_id":"toolu_123","tool_name":"Bash","tool_input":{"command":"clearing status --json"}}
			]},
			"stop_reason":"tool_use"
		}`)
		check := mkRaw(2, "r2", `{
			"model":"claude-test","max_tokens":64,
			"system":"You are a security monitor for autonomous AI coding agents.",
			"messages":[{"role":"user","content":[
				{"type":"text","text":"<transcript>"},
				{"type":"text","text":"User: check the clearing status"},
				{"type":"text","text":"Bash clearing status --json"},
				{"type":"text","text":"</transcript>"},
				{"type":"text","text":"Err on the side of blocking. <block> immediately."}
			]}]
		}`, `{
			"model":"claude-test",
			"message":{"role":"assistant","content":[{"type":"text","text":"<block>no"}]},
			"stop_reason":"end_turn"
		}`)

		set, err := derive.BuildDerivedSet([]storage.RawTurnRecord{mainTurn, check}, "proj")
		Expect(err).NotTo(HaveOccurred())

		Expect(set.Report.CallKinds).To(HaveKeyWithValue(derive.KindMain, 1))
		Expect(set.Report.CallKinds).To(HaveKeyWithValue(derive.KindCheckStage1, 1))
		Expect(set.Report.JudgedActions).To(Equal(1))
		Expect(set.Report.AttachedVerdicts).To(Equal(1))

		var checkNodes int
		for _, dn := range set.Nodes {
			if dn.Node.Kind == derive.KindCheckStage1 {
				checkNodes++
				Expect(dn.Node.ParentToolUseID).To(Equal("toolu_123"))
			}
		}
		Expect(checkNodes).To(BeNumerically(">", 0))
	})

	It("side-branches injected context off the spine", func() {
		turn := mkRaw(1, "r1", `{
			"model":"claude-test","max_tokens":32000,"stream":true,
			"tools":[{"name":"Bash"}],
			"messages":[
				{"role":"user","content":"# MCP Server Instructions\nserver list v1"},
				{"role":"user","content":"do the thing"}
			]
		}`, `{
			"model":"claude-test",
			"message":{"role":"assistant","content":[{"type":"text","text":"done"}]},
			"stop_reason":"end_turn"
		}`)

		set, err := derive.BuildDerivedSet([]storage.RawTurnRecord{turn}, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(set.Nodes).To(HaveLen(3))

		injected := set.Nodes[0].Node
		userNode := set.Nodes[1].Node
		respNode := set.Nodes[2].Node
		Expect(injected.Kind).To(Equal(derive.KindInjectedMCPInstructions))
		Expect(injected.ParentHash).To(BeNil())
		// The spine bypasses the injected node entirely.
		Expect(userNode.ParentHash).To(BeNil())
		Expect(respNode.ParentHash).NotTo(BeNil())
		Expect(*respNode.ParentHash).To(Equal(userNode.Hash))

		// And drift in the injected block must not move the spine: the
		// same turn with a different server list keeps user/resp hashes.
		turn2 := turn
		turn2.RawRequest = json.RawMessage(`{
			"model":"claude-test","max_tokens":32000,"stream":true,
			"tools":[{"name":"Bash"}],
			"messages":[
				{"role":"user","content":"# MCP Server Instructions\nserver list v2 CHANGED"},
				{"role":"user","content":"do the thing"}
			]
		}`)
		set2, err := derive.BuildDerivedSet([]storage.RawTurnRecord{turn2}, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(set2.Nodes[1].Node.Hash).To(Equal(userNode.Hash))
		Expect(set2.Nodes[2].Node.Hash).To(Equal(respNode.Hash))
	})

	It("is a pure function of the raw rows", func() {
		turn := mkRaw(1, "r1", `{
			"model":"claude-test","max_tokens":32000,"stream":true,
			"tools":[{"name":"Bash"}],
			"messages":[{"role":"user","content":"hello"}]
		}`, `{
			"model":"claude-test",
			"message":{"role":"assistant","content":[{"type":"text","text":"hi"}]},
			"stop_reason":"end_turn"
		}`)
		a, err := derive.BuildDerivedSet([]storage.RawTurnRecord{turn}, "p")
		Expect(err).NotTo(HaveOccurred())
		b, err := derive.BuildDerivedSet([]storage.RawTurnRecord{turn}, "p")
		Expect(err).NotTo(HaveOccurred())
		Expect(len(a.Nodes)).To(Equal(len(b.Nodes)))
		for i := range a.Nodes {
			Expect(a.Nodes[i].Node.Hash).To(Equal(b.Nodes[i].Node.Hash))
			Expect(a.Nodes[i].Node.Kind).To(Equal(b.Nodes[i].Node.Kind))
		}
	})
})

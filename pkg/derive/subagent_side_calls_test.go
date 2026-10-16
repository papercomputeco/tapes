package derive_test

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// subagentRow is one captured Claude Code call. A non-empty thread is
// the x-claude-code-agent-id the capture stamps into meta.thread_id.
func subagentRow(id int64, thread string, messages []map[string]any, reply string) storage.RawTurnRecord {
	req, err := json.Marshal(map[string]any{
		"model": "claude-opus-4-8", "system": "You are Claude Code.", "max_tokens": 32000,
		"stream": true, "tools": []map[string]any{{"name": "Bash", "input_schema": map[string]any{"type": "object"}}},
		"messages": messages,
	})
	Expect(err).NotTo(HaveOccurred())
	resp, err := json.Marshal(map[string]any{
		"done": true, "model": "claude-opus-4-8", "stop_reason": "end_turn",
		"usage":   map[string]any{"prompt_tokens": 1000, "completion_tokens": 100, "total_tokens": 1100},
		"message": map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": reply}}},
	})
	Expect(err).NotTo(HaveOccurred())
	var meta json.RawMessage
	if thread != "" {
		meta = json.RawMessage(fmt.Sprintf(`{"thread_id":%q}`, thread))
	}
	return storage.RawTurnRecord{
		ID: id, Provider: "anthropic", HarnessID: "claude", HarnessSessionID: "subagent-side-calls",
		RequestID: fmt.Sprintf("req-%d", id), RawRequest: req, Response: resp, Meta: meta,
		ReceivedAt: time.Unix(1700000000+id, 0),
	}
}

func userMsg(text string) map[string]any { return map[string]any{"role": "user", "content": text} }

var _ = Describe("subagent side calls", func() {
	It("keeps a subagent's compaction and progress labels off the spine", func() {
		task := []map[string]any{userMsg("Audit the derive package for dead code.")}
		rows := []storage.RawTurnRecord{
			subagentRow(1, "", []map[string]any{userMsg("Find dead code in pkg/derive.")}, "Dispatching a subagent."),
			subagentRow(2, "a1", task, "Reading spans.go."),
			subagentRow(3, "a1", append(task,
				map[string]any{"role": "assistant", "content": "Reading spans.go."},
				userMsg("CRITICAL: Respond with TEXT ONLY. Do NOT call any tools.\n\nYour task is to create a detailed summary of the conversation so far."),
			), "<analysis>…</analysis>\n<summary>\n1. Primary Request and Intent:\n…\n2. Key Technical Concepts:\n…\n</summary>"),
			subagentRow(4, "a1", append(task,
				map[string]any{"role": "assistant", "content": "Reading spans.go."},
				userMsg("Describe your most recent action in 3-5 words using present tense (-ing). Do not use tools."),
			), "Reading spans.go"),
		}
		dv, err := derive.NewDeriver("")
		Expect(err).NotTo(HaveOccurred())
		for i := range rows {
			dv.AddTurn(&rows[i])
		}
		spans := derive.EmitSpans(dv.Finish())

		kinds := map[string]string{}
		parents := map[string]string{}
		agentSpan := ""
		for _, turn := range spans.Turns {
			for _, s := range turn.Spans {
				if s.Kind == derive.SpanKindAgent && s.ThreadID == "a1" {
					agentSpan = s.SpanID
				}
				if s.Kind == derive.SpanKindLLM {
					kinds[s.SpanID] = s.CallKind
					parents[s.SpanID] = s.ParentSpanID
					if s.CallKind != derive.KindMain {
						Expect(s.ThreadID).To(Equal("a1"), "side call %s left its subagent", s.CallKind)
					}
				}
			}
		}
		Expect(kinds).To(HaveKeyWithValue("llm_req-3", derive.KindCompaction))
		Expect(kinds).To(HaveKeyWithValue("llm_req-4", derive.KindProgressLabel))
		Expect(kinds).To(HaveKeyWithValue("llm_req-2", derive.KindMain))

		// Every subagent call, side calls included, sits under the
		// subagent's agent span rather than the host trace's root.
		Expect(agentSpan).NotTo(BeEmpty())
		for _, id := range []string{"llm_req-2", "llm_req-3", "llm_req-4"} {
			Expect(parents).To(HaveKeyWithValue(id, agentSpan), "%s is not under the subagent", id)
		}

		// A subagent compaction is not a session seam.
		Expect(spans.Report.LinkKinds[derive.LinkCompactionSeam]).To(BeZero())
		Expect(spans.Report.Synthetic).To(BeZero())
	})
})

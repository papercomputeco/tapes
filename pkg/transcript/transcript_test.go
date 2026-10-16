package transcript_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/transcript"
)

var epoch = time.Date(2026, time.June, 15, 12, 0, 0, 0, time.UTC)

func blocks(bs ...llm.ContentBlock) json.RawMessage {
	b, err := json.Marshal(bs)
	Expect(err).NotTo(HaveOccurred())
	return b
}

func span(seq int, kind, callKind, thread, id string, in, out json.RawMessage) storage.SpanRecord {
	return storage.SpanRecord{
		TraceID: "trc", SpanID: id, Kind: kind, CallKind: callKind, ThreadID: thread,
		Seq: int64(seq), StartedAt: epoch.Add(time.Duration(seq) * time.Second), Input: in, Output: out,
	}
}

func text(s string) llm.ContentBlock { return llm.ContentBlock{Type: "text", Text: s} }

func feedAll(w *transcript.Walker, spans ...storage.SpanRecord) []transcript.Emitted {
	out := make([]transcript.Emitted, 0, 2*len(spans))
	for _, sp := range spans {
		out = append(out, w.Feed(sp)...)
	}
	return append(out, w.Flush()...)
}

var _ = Describe("Walker", func() {
	llmCall := span(1, "llm", "main", "", "llm_1",
		blocks(text("<system-reminder>noise</system-reminder>"), text("Fix the bug")),
		blocks(text("On it"), llm.ContentBlock{Type: "tool_use", ToolUseID: "t1", ToolName: "Edit"}))
	tool := span(2, "tool", "", "", "t1", nil,
		blocks(llm.ContentBlock{Type: "tool_result", ToolResultID: "t1", ToolOutput: "edited"}))
	event := span(3, "event", "injected:system-insert", "", "evt", blocks(text("reminder")), nil)
	next := span(4, "llm", "main", "", "llm_2", nil, blocks(text("Fixed")))

	It("splits a call into a user row and an assistant row, and folds tool spans into one user row", func() {
		rows := feedAll(transcript.NewWalker(transcript.Options{}, transcript.Position{}), llmCall, tool, event, next)
		Expect(rows).To(HaveLen(4))
		Expect(rows[0].Row.Role).To(Equal(transcript.RoleUser))
		Expect(rows[0].Row.Text).To(Equal("Fix the bug"))
		Expect(rows[1].Row.ToolCalls).To(HaveLen(1))
		Expect(rows[2].Row.ToolResults).To(Equal([]transcript.ToolResult{{ToolUseID: "t1", Text: "edited"}}))
		Expect(rows[2].Row.SpanID).To(Equal("t1"))
		Expect(rows[3].Row.Text).To(Equal("Fixed"))
	})

	It("mints positions that resume without repeating or skipping a row", func() {
		all := feedAll(transcript.NewWalker(transcript.Options{}, transcript.Position{}), llmCall, tool, event, next)
		spans := []storage.SpanRecord{llmCall, tool, event, next}
		for i, e := range all {
			w := transcript.NewWalker(transcript.Options{}, e.Next)
			var rest []storage.SpanRecord
			for _, sp := range spans {
				if e.Next.After.IsZero() || sp.Seq > e.Next.After.Seq {
					rest = append(rest, sp)
				}
			}
			resumed := feedAll(w, rest...)
			Expect(len(resumed)).To(Equal(len(all)-i-1), "resuming after row %d", i)
			for j, r := range resumed {
				Expect(r.Row).To(Equal(all[i+1+j].Row))
			}
		}
	})

	It("keeps the root thread unless subagents are asked for", func() {
		sub := span(1, "llm", "main", "agent-1", "llm_sub", blocks(text("task")), blocks(text("done")))
		Expect(transcript.Contributes(sub, transcript.Options{})).To(BeFalse())
		Expect(transcript.Contributes(sub, transcript.Options{Subagents: true})).To(BeTrue())
		Expect(transcript.Contributes(span(1, "llm", "offshoot:title-gen", "", "x", nil, nil), transcript.Options{})).To(BeFalse())
		Expect(transcript.Contributes(span(1, "llm", "", "", "x", nil, nil), transcript.Options{})).To(BeTrue())
	})
})

var _ = Describe("UserText", func() {
	It("strips harness tags and keeps the words inside content-bearing wrappers", func() {
		Expect(transcript.UserText([]llm.ContentBlock{
			text("<system-reminder>x</system-reminder>"),
			text("<command-name>/review</command-name><command-args>the parser</command-args>"),
			text("please"),
		})).To(Equal("the parser\nplease"))
	})
})

var _ = Describe("Matches", func() {
	user := transcript.Row{Role: transcript.RoleUser, Text: "Hello World"}
	results := transcript.Row{Role: transcript.RoleUser, ToolResults: []transcript.ToolResult{{ToolUseID: "t"}}}
	assistant := transcript.Row{Role: transcript.RoleAssistant, Text: "hi"}

	It("filters by role, dropping tool-result-only rows from role=user", func() {
		Expect(transcript.Matches(user, "user", "")).To(BeTrue())
		Expect(transcript.Matches(results, "user", "")).To(BeFalse())
		Expect(transcript.Matches(results, "all", "")).To(BeTrue())
		Expect(transcript.Matches(assistant, "user", "")).To(BeFalse())
		Expect(transcript.Matches(assistant, "assistant", "")).To(BeTrue())
	})

	It("matches the substring case-insensitively", func() {
		Expect(transcript.Matches(user, "all", "world")).To(BeTrue())
		Expect(transcript.Matches(user, "all", "mars")).To(BeFalse())
	})
})

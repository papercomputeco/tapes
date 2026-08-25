package derive_test

import (
	"encoding/json"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var _ = Describe("Transcript-only reconstruction", func() {
	mkFile := func(agentID, toolUseID, records string) *derive.TranscriptFile {
		meta, _ := json.Marshal(map[string]any{
			"transcript":  true,
			"agent_id":    agentID,
			"tool_use_id": toolUseID,
		})
		rec := storage.RawTurnRecord{
			Source:           "transcript",
			HarnessID:        "claude",
			HarnessSessionID: "sess-1",
			RawRequest:       json.RawMessage(records),
			Meta:             meta,
		}
		file, err := derive.ParseTranscriptFile(&rec)
		Expect(err).NotTo(HaveOccurred())
		return file
	}

	key := derive.SessionKey{HarnessID: "claude", HarnessSessionID: "sess-1"}

	It("projects a trace with source=transcript from a transcript-only session", func() {
		main := mkFile("", "", `[
			{"uuid":"u1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"write a test"}},
			{"uuid":"u2","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":[{"type":"text","text":"ok"}]}}
		]`)

		dv, err := derive.NewDeriver("")
		Expect(err).NotTo(HaveOccurred())
		for _, t := range derive.TranscriptTurns(main, "", time.Unix(0, 0)) {
			dv.AddTranscriptTurn(key, 7, t)
		}
		set := dv.Finish()
		spans := derive.EmitSpans(set)

		Expect(spans.Turns).To(HaveLen(1))
		turn := spans.Turns[0]
		Expect(turn.Source).To(Equal("transcript"))
		Expect(turn.UserPrompt).To(ContainSubstring("write a test"))

		llmSpans := 0
		for _, s := range turn.Spans {
			if s.Kind == derive.SpanKindLLM {
				llmSpans++
				Expect(s.CallKind).To(Equal(derive.KindMain))
			}
		}
		Expect(llmSpans).To(BeNumerically(">=", 1))
	})

	It("anchors a subagent transcript under its spawning Task tool span", func() {
		main := mkFile("", "", `[
			{"uuid":"u1","timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"spawn a subagent"}},
			{"uuid":"u2","timestamp":"2026-01-01T00:00:01Z","message":{"role":"assistant","content":[
				{"type":"tool_use","id":"toolu_task_1","name":"Task","input":{"prompt":"explore"}}
			]}}
		]`)
		sub := mkFile("abc123", "toolu_task_1", `[
			{"uuid":"s1","timestamp":"2026-01-01T00:00:02Z","message":{"role":"user","content":"explore"}},
			{"uuid":"s2","timestamp":"2026-01-01T00:00:03Z","message":{"role":"assistant","content":[{"type":"text","text":"explored"}]}}
		]`)

		dv, err := derive.NewDeriver("")
		Expect(err).NotTo(HaveOccurred())

		type turnSource struct {
			turn derive.TranscriptTurn
			id   int64
		}
		var turns []turnSource
		for _, t := range derive.TranscriptTurns(main, "", time.Unix(0, 0)) {
			turns = append(turns, turnSource{t, 7})
		}
		for _, t := range derive.TranscriptTurns(sub, "", time.Unix(0, 0)) {
			turns = append(turns, turnSource{t, 8})
		}
		sort.SliceStable(turns, func(i, j int) bool {
			return turns[i].turn.CapturedAt.Before(turns[j].turn.CapturedAt)
		})
		for _, ts := range turns {
			dv.AddTranscriptTurn(key, ts.id, ts.turn)
		}

		set := dv.Finish()
		spans := derive.EmitSpans(set)

		Expect(spans.Turns).To(HaveLen(1))
		var toolSpan, agentSpan *derive.Span
		for _, s := range spans.Turns[0].Spans {
			switch {
			case s.Kind == derive.SpanKindTool && s.Name == "Task":
				toolSpan = s
			case s.Kind == derive.SpanKindAgent && s.ThreadID == "abc123":
				agentSpan = s
			}
		}
		Expect(toolSpan).NotTo(BeNil())
		Expect(agentSpan).NotTo(BeNil())
		Expect(agentSpan.ParentSpanID).To(Equal(toolSpan.SpanID))
	})
})

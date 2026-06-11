package derive_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
)

// The span-model corpus gate — decision 7 re-pinned as span trees. The
// same two live-capture raw layers must re-derive into semantically
// equivalent traces: turn segmentation, span kinds, fork nesting,
// offshoot anchoring, and compaction stitching are all pinned here.
// When the emitter changes intentionally, re-pin and say why in the
// commit message.

// spanIndex collects lookup tables a gate assertion needs.
func spanIndex(spans *derive.SpanSet) (byID map[string]*derive.Span, turnOf map[string]*derive.SpanTurn) {
	byID = map[string]*derive.Span{}
	turnOf = map[string]*derive.SpanTurn{}
	for _, turn := range spans.Turns {
		for _, s := range turn.Spans {
			byID[s.SpanID] = s
			turnOf[s.SpanID] = turn
		}
	}
	return byID, turnOf
}

var _ = Describe("span emit over the corpus (cb9a87e5)", func() {
	It("projects the session into the pinned trace shape", func() {
		set, _ := deriveAdvanced()
		spans := derive.EmitSpans(set)
		r := spans.Report

		// Three user-visible turns: the session opener, the work
		// prompt, the thanks.
		Expect(r.Traces).To(Equal(3))
		Expect(r.Synthetic).To(Equal(0)) // no compaction in this session

		// One llm span per parsed wire call — the call mix is pinned
		// in the node-layer gate; here the same counts appear as span
		// call kinds plus the injected event spans.
		Expect(r.SpanKinds).To(Equal(map[string]int{
			derive.SpanKindAgent: 5, // 3 trace roots + 2 subagents
			derive.SpanKindLLM:   85,
			derive.SpanKindTool:  97,
			// injected context (7) + mid-spine system-role inserts (5)
			derive.SpanKindEvent: 12,
		}))
		Expect(r.CallKinds[derive.KindMain]).To(Equal(56))
		Expect(r.CallKinds[derive.KindCheckStage2]).To(Equal(1))

		// Every tool_use becomes exactly one tool span with one emits
		// link; every consumed result feeds the call that read it.
		Expect(r.LinkKinds[derive.LinkEmits]).To(Equal(97))
		Expect(r.LinkKinds[derive.LinkFeeds]).To(Equal(97))

		// Both subagents nest under their Task tool span and rejoin.
		Expect(r.LinkKinds[derive.LinkRejoin]).To(Equal(2))
		byID, turnOf := spanIndex(spans)
		agents := 0
		for id, s := range byID {
			if s.Kind != derive.SpanKindAgent || s.ThreadID == "" {
				continue
			}
			agents++
			task := byID[s.ParentSpanID]
			Expect(task).NotTo(BeNil(), "subagent %s parent missing", id)
			Expect(task.Kind).To(Equal(derive.SpanKindTool))
			// the spawn tool is "Agent" in this capture's harness build
			Expect(task.Name).To(Equal("Agent"))
			Expect(turnOf[id]).To(BeIdenticalTo(turnOf[task.SpanID]),
				"subagent lives in its Task's trace")
		}
		Expect(agents).To(Equal(2))

		// Anchored shadows hang off the tool span they judge; the
		// orphans are the session-level calls (title-gen, suggestion)
		// plus the subagents' non-tool handback checks.
		Expect(r.LinkKinds[derive.LinkVerdict]).To(Equal(26))
		Expect(r.OrphanShadow).To(Equal(3))

		// No cross-trace causality without compaction.
		Expect(spans.Links).To(BeEmpty())
	})
})

var _ = Describe("span emit over the corpus (9fec0da7 — compaction)", func() {
	It("projects the session into the pinned trace shape", func() {
		set, _ := deriveSuperAdvanced()
		spans := derive.EmitSpans(set)
		r := spans.Report

		// Three turns: the autonomous run, the compaction
		// continuation, the thanks. (The tree model rendered the
		// post-compaction caveat as its own root; on the wire it
		// arrives inside the continuation call, and per-call
		// granularity cannot split a single call into two turns.)
		Expect(r.Traces).To(Equal(3))
		Expect(r.Synthetic).To(Equal(1))

		Expect(r.SpanKinds).To(Equal(map[string]int{
			derive.SpanKindAgent: 7, // 3 trace roots + 4 subagents
			derive.SpanKindLLM:   121,
			derive.SpanKindTool:  129,
			// injected context (10) + mid-spine system-role inserts (9)
			derive.SpanKindEvent: 19,
		}))
		Expect(r.CallKinds[derive.KindMain]).To(Equal(79))
		Expect(r.CallKinds[derive.KindCompaction]).To(Equal(1))

		Expect(r.LinkKinds[derive.LinkEmits]).To(Equal(129))
		Expect(r.LinkKinds[derive.LinkFeeds]).To(Equal(129))
		Expect(r.LinkKinds[derive.LinkRejoin]).To(Equal(4))
		Expect(r.LinkKinds[derive.LinkVerdict]).To(Equal(35))
		Expect(r.OrphanShadow).To(Equal(6))

		// The compaction seam is the one cross-trace edge: the
		// compaction llm span's output seeds the first llm call of the
		// post-compaction trace.
		Expect(r.LinkKinds[derive.LinkCompactionSeam]).To(Equal(1))
		Expect(spans.Links).To(HaveLen(1))
		seam := spans.Links[0]
		Expect(seam.Kind).To(Equal(derive.LinkCompactionSeam))
		Expect(seam.FromTraceID).NotTo(Equal(seam.ToTraceID))

		byID, turnOf := spanIndex(spans)
		from := byID[seam.FromSpanID]
		Expect(from.CallKind).To(Equal(derive.KindCompaction))
		to := byID[seam.ToSpanID]
		Expect(to.Kind).To(Equal(derive.SpanKindLLM))
		Expect(turnOf[seam.ToSpanID].Synthetic).To(Equal("post-compaction"))

		// All four subagents nest under Task tool spans in the main
		// trace.
		for id, s := range byID {
			if s.Kind == derive.SpanKindAgent && s.ThreadID != "" {
				Expect(byID[s.ParentSpanID].Name).To(Equal("Agent"), "agent %s", id)
			}
		}

		// Structural invariants that hold for every emitted set:
		// parents resolve within the trace, links reference real
		// spans, payload dedup means no llm input ever carries a
		// tool_result block.
		for _, turn := range spans.Turns {
			for _, s := range turn.Spans {
				if s.ParentSpanID != "" {
					Expect(turnOf[s.ParentSpanID]).To(BeIdenticalTo(turn),
						"span %s parent crosses traces", s.SpanID)
				}
				for _, b := range s.Input {
					if s.Kind == derive.SpanKindLLM {
						Expect(b.Type).NotTo(Equal("tool_result"),
							"llm span %s input re-carries a tool result", s.SpanID)
					}
				}
			}
			for _, l := range turn.Links {
				Expect(byID).To(HaveKey(l.FromSpanID))
				Expect(byID).To(HaveKey(l.ToSpanID))
			}
		}
	})

	It("mints identical span identity on re-derive", func() {
		a := derive.EmitSpans(mustDerive(deriveSuperAdvanced))
		b := derive.EmitSpans(mustDerive(deriveSuperAdvanced))
		Expect(len(a.Turns)).To(Equal(len(b.Turns)))
		for i, ta := range a.Turns {
			tb := b.Turns[i]
			Expect(tb.TraceID).To(Equal(ta.TraceID))
			Expect(len(tb.Spans)).To(Equal(len(ta.Spans)))
			for j, sa := range ta.Spans {
				sb := tb.Spans[j]
				Expect(sb.SpanID).To(Equal(sa.SpanID))
				Expect(sb.ParentSpanID).To(Equal(sa.ParentSpanID))
				Expect(sb.Kind).To(Equal(sa.Kind))
				Expect(sb.CallKind).To(Equal(sa.CallKind))
				Expect(sb.NodeHash).To(Equal(sa.NodeHash))
			}
		}
	})
})

func mustDerive(fn func() (*derive.DerivedSet, *derive.ReconcileStats)) *derive.DerivedSet {
	set, _ := fn()
	return set
}

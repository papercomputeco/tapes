package derive_test

import (
	"bytes"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// The Codex subagent-nesting corpus gate (PCC-1021). Both corpora are
// LIVE captures from the July-22 codex_skills clearing, re-shaped into
// the post-fix attribution contract paperd emits: every raw turn keyed
// to the ROOT Codex session id, child thread ids in meta.thread_id
// (empty on root turns, whose thread-id equals the session id), and one
// transcript-source spawn-anchor row per child exactly as
// POST /v1/ingest/transcript mints it from the paperd uploader payload
// (see the contract in codex.go).
//
//   - corpus-codex-delta: root 019f8d46-beb1 → child 019f8d46-e663
//     (fork_turns:"all" — its chain grafts onto the root spine's nodes)
//     → grandchild 019f8d47-0473. The depth-2 gate.
//   - corpus-codex-parallel: a seven-turn interleaved window of session
//     019f8bba-0a74 — two fork_turns:"none" children whose byte-identical
//     harness context dedups into ONE shared chain root (the shape that
//     forced per-call anchors), plus a spawn whose child is outside the
//     window (a dangling spawn that must stay agent-less).
//
// When the emitter changes intentionally, re-pin and say why in the
// commit message.

const (
	codexDeltaRoot   = "019f8d46-beb1-7f50-9df4-0cd39ed38d13"
	codexDeltaChild  = "019f8d46-e663-74e1-940c-f82e34c07618"
	codexDeltaGrand  = "019f8d47-0473-7743-a1ed-9e4c0ae92ad8"
	codexDeltaSpawn1 = "call_J7B6r7ZdtqkECtSJV8YDQaL7" // root  → child
	codexDeltaSpawn2 = "call_sQQ4ZV6SPacs8MmDrGYsdLDD" // child → grandchild

	codexParallelRoot   = "019f8bba-0a74-73c3-a2e0-a784624b7fa7"
	codexParallelChild1 = "019f8bd3-de08-70d0-b14e-c4b7ccfa5ffb"
	codexParallelChild2 = "019f8bd3-f86a-72e0-838c-eddb52372b45"
	codexParallelSpawn1 = "call_1ImkdvjnhZ7lGYMzORgqMimo"
	codexParallelSpawn2 = "call_oJbDT1fgSTQoa65JpbMDEsTu"
	// codexParallelSpawn3's child thread is outside the capture window.
	codexParallelSpawn3 = "call_jHcOQ8vNGfZrXFeZweIf1SDY"
)

// deriveCodexRows mirrors the production pipeline over in-memory rows:
// RederiveSession sorts wire rows by capture time before feeding the
// deriver, so the helper does too.
func deriveCodexRows(wire, transcripts []storage.RawTurnRecord) (*derive.DerivedSet, *derive.ReconcileStats, *derive.SpanSet) {
	sorted := make([]storage.RawTurnRecord, len(wire))
	copy(sorted, wire)
	sort.SliceStable(sorted, func(i, j int) bool {
		return derive.CapturedAt(&sorted[i]).Before(derive.CapturedAt(&sorted[j]))
	})
	set, err := derive.BuildDerivedSet(sorted, "")
	Expect(err).NotTo(HaveOccurred())
	files := make([]*derive.TranscriptFile, 0, len(transcripts))
	for i := range transcripts {
		file, err := derive.ParseTranscriptFile(&transcripts[i])
		Expect(err).NotTo(HaveOccurred())
		files = append(files, file)
	}
	stats := derive.ReconcileTranscripts(set, files)
	return set, stats, derive.EmitSpans(set)
}

func loadCodexCorpus(path string, wantWire, wantAnchors int) (wire, transcripts []storage.RawTurnRecord) {
	wire, transcripts = loadCorpus(path)
	Expect(wire).To(HaveLen(wantWire))
	Expect(transcripts).To(HaveLen(wantAnchors))
	return wire, transcripts
}

// spawnInput returns the spawn tool span's normalized tool_input.
func spawnInput(s *derive.Span) map[string]any {
	Expect(s).NotTo(BeNil())
	Expect(s.Input).To(HaveLen(1))
	return s.Input[0].ToolInput
}

// rejoinTargets collects agent-span → tool-span rejoin pairs.
func rejoinTargets(spans *derive.SpanSet) map[string]string {
	out := map[string]string{}
	for _, turn := range spans.Turns {
		for _, l := range turn.Links {
			if l.Kind == derive.LinkRejoin {
				out[l.FromSpanID] = l.ToSpanID
			}
		}
	}
	return out
}

var _ = Describe("codex corpus (delta — depth-2 nesting)", func() {
	load := func() ([]storage.RawTurnRecord, []storage.RawTurnRecord) {
		return loadCodexCorpus(corpusPath("corpus-codex-delta.jsonl.gz"), 13, 2)
	}
	deriveAll := func() (*derive.DerivedSet, *derive.ReconcileStats, *derive.SpanSet) {
		wire, transcripts := load()
		return deriveCodexRows(wire, transcripts)
	}

	It("anchors the spawn-anchor rows through the verbatim transcript identity join", func() {
		_, transcripts := load()
		for i := range transcripts {
			file, err := derive.ParseTranscriptFile(&transcripts[i])
			Expect(err).NotTo(HaveOccurred(), "the Claude transcript parser must accept a Codex rollout-line records array")
			Expect(file.AgentID).NotTo(BeEmpty())
			Expect(file.ToolUseID).To(HavePrefix("call_"))
			Expect(file.Session).To(Equal(derive.SessionKey{HarnessID: "codex", HarnessSessionID: codexDeltaRoot}))
		}
	})

	It("projects the Delta topology into the pinned trace shape", func() {
		set, stats, spans := deriveAll()

		// One session, all turns classified main; root turns carry an
		// empty thread id, children their own.
		Expect(set.Report.CallKinds).To(Equal(map[string]int{derive.KindMain: 13}))
		Expect(set.Sessions).To(ConsistOf(derive.SessionKey{HarnessID: "codex", HarnessSessionID: codexDeltaRoot}))

		// Both threads anchored via the spawn-anchor rows.
		Expect(stats.CodexThreadsAnchored).To(Equal(2))
		Expect(stats.CodexThreadsUnanchored).To(Equal(0))

		r := spans.Report
		Expect(r.Traces).To(Equal(1))
		Expect(r.Synthetic).To(Equal(0))
		Expect(r.SpanKinds).To(Equal(map[string]int{
			derive.SpanKindAgent: 3, // trace root + child + grandchild
			derive.SpanKindLLM:   13,
			derive.SpanKindTool:  10,
		}))
		Expect(r.LinkKinds[derive.LinkEmits]).To(Equal(10))
		Expect(r.LinkKinds[derive.LinkFeeds]).To(Equal(10))
		Expect(r.LinkKinds[derive.LinkRejoin]).To(Equal(2))

		byID, turnOf := spanIndex(spans)

		// Child: agent span named "subagent", SpanID agent_<thread>,
		// parented to the EXACT spawn_agent tool span.
		child := byID["agent_"+codexDeltaChild]
		Expect(child).NotTo(BeNil())
		Expect(child.Kind).To(Equal(derive.SpanKindAgent))
		Expect(child.Name).To(Equal("subagent"))
		Expect(child.ParentSpanID).To(Equal(codexDeltaSpawn1))
		Expect(byID[codexDeltaSpawn1].Kind).To(Equal(derive.SpanKindTool))
		Expect(byID[codexDeltaSpawn1].Name).To(Equal("spawn_agent"))
		Expect(turnOf["agent_"+codexDeltaChild]).To(BeIdenticalTo(turnOf[codexDeltaSpawn1]))

		// Depth-2: the grandchild nests under the LAUNCHER's spawn tool
		// span, whose parent is the child's agent span — not the root.
		grand := byID["agent_"+codexDeltaGrand]
		Expect(grand).NotTo(BeNil())
		Expect(grand.Name).To(Equal("subagent"))
		Expect(grand.ParentSpanID).To(Equal(codexDeltaSpawn2))
		Expect(byID[codexDeltaSpawn2].ParentSpanID).To(Equal(child.SpanID))

		// Rejoin links: each agent's output back to its spawn tool span.
		Expect(rejoinTargets(spans)).To(Equal(map[string]string{
			child.SpanID: codexDeltaSpawn1,
			grand.SpanID: codexDeltaSpawn2,
		}))

		// Child llm spans keep thread id, model, usage, and call_kind
		// "main" — the console's Chat tab coalesces on the literal value.
		var childLLM int
		for _, turn := range spans.Turns {
			for _, s := range turn.Spans {
				if s.Kind == derive.SpanKindLLM && s.ThreadID != "" {
					childLLM++
					Expect(s.CallKind).To(Equal(derive.KindMain))
					Expect(s.Model).To(Equal("gpt-5.6-sol"))
					Expect(s.Usage).NotTo(BeNil())
				}
			}
		}
		Expect(childLLM).To(Equal(8)) // 5 child + 3 grandchild calls

		// Structural invariants: parents resolve within the trace, links
		// reference real spans.
		for _, turn := range spans.Turns {
			for _, s := range turn.Spans {
				if s.ParentSpanID != "" {
					Expect(turnOf[s.ParentSpanID]).To(BeIdenticalTo(turn),
						"span %s parent crosses traces", s.SpanID)
				}
			}
			for _, l := range turn.Links {
				Expect(byID).To(HaveKey(l.FromSpanID))
				Expect(byID).To(HaveKey(l.ToSpanID))
			}
		}
	})

	It("labels the spawn tool spans for the console panel", func() {
		_, _, spans := deriveAll()
		byID, _ := spanIndex(spans)

		// subagent_type = the wire envelope's agent_nickname (richer than
		// the anchor row's path segment); description = the agent_path.
		in := spawnInput(byID[codexDeltaSpawn1])
		Expect(in["subagent_type"]).To(Equal("Volta"))
		Expect(in["description"]).To(Equal("/root/depth2_cli_child"))
		// The original wire arguments survive alongside.
		Expect(in["task_name"]).To(Equal("depth2_cli_child"))

		in = spawnInput(byID[codexDeltaSpawn2])
		Expect(in["subagent_type"]).To(Equal("Euler"))
		Expect(in["description"]).To(Equal("/root/depth2_cli_child/depth2_cli_grandchild"))

		// The normalization is span-facing only: the parent llm span's
		// OUTPUT still carries the wire-verbatim block.
		for _, turn := range spans.Turns {
			for _, s := range turn.Spans {
				if s.Kind != derive.SpanKindLLM {
					continue
				}
				for _, b := range s.Output {
					if b.ToolUseID == codexDeltaSpawn1 {
						Expect(b.ToolInput).NotTo(HaveKey("subagent_type"))
					}
				}
			}
		}
	})

	It("folds child usage into the root session's rollups and leaves root status intact", func() {
		_, _, spans := deriveAll()
		key := derive.SessionKey{HarnessID: "codex", HarnessSessionID: codexDeltaRoot}

		// Every llm call — subagents included — folds into the session's
		// model usage (the rollup basis).
		usage := spans.ModelUsage[key]
		Expect(usage).To(HaveLen(1))
		Expect(usage[0].Model).To(Equal("gpt-5.6-sol"))
		Expect(usage[0].Calls).To(Equal(int64(13)))
		Expect(usage[0].InputTokens).To(BeNumerically(">", 0))

		// The trace's token totals span all threads.
		turn := spans.Turns[0]
		var allThreads int64
		for _, s := range turn.Spans {
			if s.Kind == derive.SpanKindLLM && s.Usage != nil {
				allThreads += int64(s.Usage.PromptTokens)
			}
		}
		Expect(turn.TotalInputTokens).To(Equal(allThreads))
		Expect(spans.KindCounts[key]).To(Equal(map[string]int{derive.KindMain: 13}))

		// Root derived_status folds from the ROOT spine's terminal call:
		// root turns carry an empty thread id, so the status leaf is the
		// closing main-thread response, not a subagent's.
		Expect(spans.Status).To(HaveKey(key))
		Expect(spans.Status[key].DerivedStatus).To(Equal("completed"))
	})

	It("mints identical span identity on re-derive", func() {
		_, _, a := deriveAll()
		_, _, b := deriveAll()
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
				Expect(sb.Seq).To(Equal(sa.Seq))
			}
		}
	})

	It("converges when rows arrive out of order (child before parent spawn result)", func() {
		wire, transcripts := load()

		// Arrival order is not capture order: reverse the rows entirely.
		// The production path (and deriveCodexRows) re-sorts by capture
		// time, so the projection must be byte-identical.
		reversed := make([]storage.RawTurnRecord, 0, len(wire))
		for i := len(wire) - 1; i >= 0; i-- {
			reversed = append(reversed, wire[i])
		}
		setA, _, spansA := deriveCodexRows(wire, transcripts)
		setB, _, spansB := deriveCodexRows(reversed, transcripts)
		Expect(canonicalProjection(setB, spansB)).To(Equal(canonicalProjection(setA, spansA)))
	})

	It("still nests depth-2 when thread calls are fed grandchild-first (capture-order edge)", func() {
		wire, transcripts := load()

		// Feed the deriver an adversarial order directly (no re-sort):
		// every grandchild turn ahead of every child turn. The grandchild's
		// anchor tool span is emitted by the CHILD's calls, so a single
		// capture-order pass would drop the grandchild to the trace root;
		// the thread phase iterates to a fixed point instead.
		threadOf := func(rec *storage.RawTurnRecord) int {
			switch {
			case bytes.Contains(rec.Meta, []byte(codexDeltaGrand)):
				return 0 // first: the adversarial part
			case bytes.Contains(rec.Meta, []byte(codexDeltaChild)):
				return 1
			default:
				return 2
			}
		}
		adversarial := make([]storage.RawTurnRecord, len(wire))
		copy(adversarial, wire)
		sort.SliceStable(adversarial, func(i, j int) bool {
			return threadOf(&adversarial[i]) < threadOf(&adversarial[j])
		})

		set, err := derive.BuildDerivedSet(adversarial, "")
		Expect(err).NotTo(HaveOccurred())
		files := make([]*derive.TranscriptFile, 0, len(transcripts))
		for i := range transcripts {
			f, err := derive.ParseTranscriptFile(&transcripts[i])
			Expect(err).NotTo(HaveOccurred())
			files = append(files, f)
		}
		derive.ReconcileTranscripts(set, files)
		spans := derive.EmitSpans(set)

		byID, _ := spanIndex(spans)
		Expect(byID["agent_"+codexDeltaChild].ParentSpanID).To(Equal(codexDeltaSpawn1))
		Expect(byID["agent_"+codexDeltaGrand].ParentSpanID).To(Equal(codexDeltaSpawn2))
		Expect(byID[codexDeltaSpawn2].ParentSpanID).To(Equal("agent_" + codexDeltaChild))
		Expect(rejoinTargets(spans)).To(HaveLen(2))
	})

	It("anchors through the agent_path fallback when no spawn-anchor rows exist", func() {
		wire, _ := load()
		_, stats, spans := deriveCodexRows(wire, nil)

		// The wire envelope's thread_spawn.agent_path joins against the
		// parent's spawn function_call_output task_name — exact, tested
		// degrade path.
		Expect(stats.CodexThreadsAnchored).To(Equal(2))
		Expect(stats.CodexThreadsUnanchored).To(Equal(0))

		byID, _ := spanIndex(spans)
		Expect(byID["agent_"+codexDeltaChild].ParentSpanID).To(Equal(codexDeltaSpawn1))
		Expect(byID["agent_"+codexDeltaGrand].ParentSpanID).To(Equal(codexDeltaSpawn2))
		Expect(spans.Report.LinkKinds[derive.LinkRejoin]).To(Equal(2))

		// Labels still resolve from the envelope alone.
		in := spawnInput(byID[codexDeltaSpawn1])
		Expect(in["subagent_type"]).To(Equal("Volta"))
		Expect(in["description"]).To(Equal("/root/depth2_cli_child"))
	})

	It("degrades visibly and safely when no spawn evidence exists at all", func() {
		wire, _ := load()

		// Strip the envelope metadata too: no anchor rows, no agent_path.
		stripped := make([]storage.RawTurnRecord, len(wire))
		copy(stripped, wire)
		for i := range stripped {
			stripped[i].SessionEnvelope = nil
		}
		_, stats, spans := deriveCodexRows(stripped, nil)

		// Visible: the unanchored count is the operator signal.
		Expect(stats.CodexThreadsAnchored).To(Equal(0))
		Expect(stats.CodexThreadsUnanchored).To(Equal(2))

		// Safe: both agents land under the TRACE ROOT — in the trace,
		// never attached to a wrong tool span — and mint no rejoin.
		byID, turnOf := spanIndex(spans)
		for _, thread := range []string{codexDeltaChild, codexDeltaGrand} {
			agent := byID["agent_"+thread]
			Expect(agent).NotTo(BeNil())
			root := turnOf[agent.SpanID].Spans[0]
			Expect(agent.ParentSpanID).To(Equal(root.SpanID))
		}
		Expect(spans.Report.LinkKinds[derive.LinkRejoin]).To(BeZero())

		// Labels degrade to the wire task_name.
		in := spawnInput(byID[codexDeltaSpawn1])
		Expect(in["subagent_type"]).To(Equal("depth2_cli_child"))
		Expect(in["description"]).To(Equal("depth2_cli_child"))
	})
})

var _ = Describe("codex corpus (parallel children)", func() {
	load := func() ([]storage.RawTurnRecord, []storage.RawTurnRecord) {
		return loadCodexCorpus(corpusPath("corpus-codex-parallel.jsonl.gz"), 7, 2)
	}
	deriveAll := func() (*derive.DerivedSet, *derive.ReconcileStats, *derive.SpanSet) {
		wire, transcripts := load()
		return deriveCodexRows(wire, transcripts)
	}

	It("keeps interleaved parallel children distinct under their own spawn spans", func() {
		_, stats, spans := deriveAll()
		Expect(stats.CodexThreadsAnchored).To(Equal(2))
		Expect(stats.CodexThreadsUnanchored).To(Equal(0))

		r := spans.Report
		Expect(r.Traces).To(Equal(1))
		Expect(r.SpanKinds).To(Equal(map[string]int{
			derive.SpanKindAgent: 3, // trace root + 2 children
			derive.SpanKindLLM:   7,
			derive.SpanKindTool:  5,
		}))
		Expect(r.LinkKinds[derive.LinkRejoin]).To(Equal(2))

		// Two chains, ONE deduped chain root: both fork_turns:"none"
		// children start from byte-identical harness context, the shape
		// that would collapse a node-stamped join. Each child must still
		// anchor to ITS OWN spawn tool span.
		byID, _ := spanIndex(spans)
		Expect(byID["agent_"+codexParallelChild1].ParentSpanID).To(Equal(codexParallelSpawn1))
		Expect(byID["agent_"+codexParallelChild2].ParentSpanID).To(Equal(codexParallelSpawn2))
		Expect(rejoinTargets(spans)).To(Equal(map[string]string{
			"agent_" + codexParallelChild1: codexParallelSpawn1,
			"agent_" + codexParallelChild2: codexParallelSpawn2,
		}))

		// Labels resolve per spawn.
		Expect(spawnInput(byID[codexParallelSpawn1])["subagent_type"]).To(Equal("Mencius"))
		Expect(spawnInput(byID[codexParallelSpawn2])["subagent_type"]).To(Equal("James"))
		Expect(spawnInput(byID[codexParallelSpawn1])["description"]).To(Equal("/root/verify_citation_1"))

		// The window's third spawn's child is outside the capture: the
		// tool span exists, labels degrade to its task_name, and NOTHING
		// attaches to it.
		dangling := byID[codexParallelSpawn3]
		Expect(dangling).NotTo(BeNil())
		Expect(spawnInput(dangling)["subagent_type"]).To(Equal("verify_citation_3"))
		for _, turn := range spans.Turns {
			for _, s := range turn.Spans {
				Expect(s.ParentSpanID).NotTo(Equal(codexParallelSpawn3))
			}
		}
	})

	It("orders the interleaved capture deterministically by seq", func() {
		_, _, a := deriveAll()
		_, _, b := deriveAll()

		turn := a.Turns[0]
		// Seq is the frozen presentation order: strictly increasing, and
		// time-ordered — the children INTERLEAVE with the root spine
		// rather than being grouped per thread.
		order := make([]string, 0, len(turn.Spans))
		for i, s := range turn.Spans {
			Expect(s.Seq).To(Equal(int64(i)))
			order = append(order, s.SpanID)
		}
		seq := map[string]int64{}
		for _, s := range turn.Spans {
			seq[s.SpanID] = s.Seq
		}
		// vc1's first call lands before the root's third spine call;
		// vc1's LAST call lands after it — a genuine interleave,
		// preserved in presentation order rather than grouped per thread.
		Expect(seq["llm_turn-01784757345979292000-00000219"]).To(BeNumerically("<", seq["llm_turn-01784757352668561000-00000221"]))
		Expect(seq["llm_turn-01784757355730289000-00000223"]).To(BeNumerically(">", seq["llm_turn-01784757352668561000-00000221"]))

		// Deterministic across re-derives, byte for byte.
		orderB := make([]string, 0, len(b.Turns[0].Spans))
		for _, s := range b.Turns[0].Spans {
			orderB = append(orderB, s.SpanID)
		}
		Expect(orderB).To(Equal(order))
	})

	It("refuses to guess when the agent_path fallback is ambiguous", func() {
		wire, _ := load()

		// Rewrite the capture so BOTH children (and both spawn results)
		// claim verify_citation_1's path: two spawn outputs now report
		// the same task_name, so the path→spawn join is ambiguous and
		// must anchor NEITHER thread rather than attach one to the wrong
		// tool span. (No anchor rows — the fallback is the only path.)
		rewrite := func(raw []byte) []byte {
			return bytes.ReplaceAll(raw, []byte("verify_citation_2"), []byte("verify_citation_1"))
		}
		rewritten := make([]storage.RawTurnRecord, len(wire))
		copy(rewritten, wire)
		for i := range rewritten {
			rewritten[i].RawRequest = rewrite(rewritten[i].RawRequest)
			rewritten[i].Response = rewrite(rewritten[i].Response)
			rewritten[i].SessionEnvelope = rewrite(rewritten[i].SessionEnvelope)
		}
		_, stats, spans := deriveCodexRows(rewritten, nil)

		Expect(stats.CodexThreadsAnchored).To(Equal(0))
		Expect(stats.CodexThreadsUnanchored).To(Equal(2))
		Expect(spans.Report.LinkKinds[derive.LinkRejoin]).To(BeZero())
		byID, turnOf := spanIndex(spans)
		for _, thread := range []string{codexParallelChild1, codexParallelChild2} {
			agent := byID["agent_"+thread]
			Expect(agent).NotTo(BeNil())
			Expect(agent.ParentSpanID).To(Equal(turnOf[agent.SpanID].Spans[0].SpanID))
		}
	})
})

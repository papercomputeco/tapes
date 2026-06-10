package derive_test

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// The corpus is a full session captured LIVE through a clearing
// (2026-06-10, exercise-claude-harness-advanced driven by a human):
// 85 wire envelopes exactly as tapes-extproc dispatched them plus the
// session's 3 transcript files (main + 2 subagents). It replaces the
// "golden sessions in a long-lived clearing DB" pattern — the deriver
// is a pure function of these rows, so the whole reconciliation
// pipeline regression-tests here with no database and no clearing.
//
// When the classifier or projection changes intentionally, re-pin the
// numbers below and say why in the commit message. A drop you can't
// explain is a regression, exactly like the old oracle.

type corpusRow struct {
	ID               int64           `json:"id"`
	OrgID            string          `json:"org_id"`
	Source           string          `json:"source"`
	Provider         string          `json:"provider"`
	AgentName        string          `json:"agent_name"`
	HarnessID        string          `json:"harness_id"`
	HarnessSessionID string          `json:"harness_session_id"`
	RequestID        string          `json:"request_id"`
	RawRequest       json.RawMessage `json:"raw_request"`
	Response         json.RawMessage `json:"response"`
	Meta             json.RawMessage `json:"meta"`
	SessionEnvelope  json.RawMessage `json:"session_envelope"`
	ReceivedAt       time.Time       `json:"received_at"`
}

func loadCorpus(path string) (wire []storage.RawTurnRecord, transcripts []storage.RawTurnRecord) {
	f, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred())
	defer f.Close() //nolint:errcheck // read-side close

	gz, err := gzip.NewReader(f)
	Expect(err).NotTo(HaveOccurred())

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var row corpusRow
		Expect(json.Unmarshal(line, &row)).To(Succeed())
		rec := storage.RawTurnRecord{
			ID: row.ID, OrgID: row.OrgID, Source: row.Source,
			Provider: row.Provider, AgentName: row.AgentName,
			HarnessID: row.HarnessID, HarnessSessionID: row.HarnessSessionID,
			RequestID: row.RequestID, RawRequest: row.RawRequest,
			Response: row.Response, Meta: row.Meta,
			SessionEnvelope: row.SessionEnvelope, ReceivedAt: row.ReceivedAt,
		}
		if rec.Source == storage.RawTurnSourceTranscript {
			transcripts = append(transcripts, rec)
		} else {
			wire = append(wire, rec)
		}
	}
	Expect(scanner.Err()).NotTo(HaveOccurred())
	return wire, transcripts
}

func deriveCorpus() (*derive.DerivedSet, *derive.ReconcileStats) {
	wire, transcriptRows := loadCorpus("testdata/corpus-cb9a87e5.jsonl.gz")
	Expect(wire).To(HaveLen(87))
	Expect(transcriptRows).To(HaveLen(3))

	set, err := derive.BuildDerivedSet(wire, "")
	Expect(err).NotTo(HaveOccurred())

	var files []*derive.TranscriptFile
	for i := range transcriptRows {
		file, err := derive.ParseTranscriptFile(&transcriptRows[i])
		Expect(err).NotTo(HaveOccurred())
		files = append(files, file)
	}
	stats := derive.ReconcileTranscripts(set, files)
	return set, stats
}

var _ = Describe("live-capture corpus (cb9a87e5)", func() {
	It("re-derives the session with the pinned reconciliation quality", func() {
		set, stats := deriveCorpus()
		r := set.Report

		// Thread attribution is deterministic from the capture-time
		// agent-id header: both subagents' nodes carry their thread.
		threads := map[string]int{}
		for _, dn := range set.Nodes {
			threads[dn.Node.ThreadID]++
		}
		Expect(threads).To(HaveLen(3)) // main ("") + 2 subagents

		// Every call classifies — a non-zero unknown is either a new
		// harness side-call to catalog or a classifier regression.
		Expect(r.CallKinds).NotTo(HaveKey(derive.KindUnknown))

		// The session's call mix, pinned from the live capture.
		Expect(r.CallKinds).To(Equal(map[string]int{
			derive.KindMain:        56,
			derive.KindCheckStage1: 23,
			derive.KindCheckStage2: 1,
			derive.KindTitleGen:    1,
			derive.KindPlanNameGen: 1,
			derive.KindSuggestion:  1,
			derive.KindWebSummary:  2,
		}))

		// Verdict attach: the two misses are each subagent's non-tool
		// handback event ("subagent has finished and is handing back
		// control…") — byte-identical text that thread scoping
		// correctly counts per thread instead of merging.
		Expect(r.JudgedActions).To(Equal(23))
		Expect(r.AttachedVerdicts).To(Equal(21))
		Expect(r.UnattachedActions).To(HaveLen(2))
		for _, u := range r.UnattachedActions {
			Expect(u).To(ContainSubstring("subagent has finished"))
		}

		// Both subagents fork at their Task tool_use.
		Expect(stats.SubagentForks).To(Equal(2))
		Expect(stats.ForkedChains).To(Equal(2))
		Expect(stats.MainChainsJoined).To(Equal(1))

		// Conversation join ≥90% — the residual is the known
		// structural drift (ExitPlanMode empty input, server-side
		// WebSearch), not projection noise.
		joinPct := float64(stats.ConversationJoined) / float64(stats.ConversationTotal)
		Expect(joinPct).To(BeNumerically(">=", 0.90))

		// 183 after the claude-md fan fix: the shared <user_claude_md>
		// block became a side node, so check chains root at their own
		// transcript message and stage pairs dedup their shared prefix.
		Expect(r.Nodes).To(Equal(183))

		// The plan ties to the ExitPlanMode that accepted it.
		Expect(r.PlansAttached).To(Equal(1))

		// The title-gen call's output folds onto the session.
		Expect(set.SessionTitles).To(HaveKeyWithValue(
			derive.SessionKey{HarnessID: "claude", HarnessSessionID: "cb9a87e5-b0e3-4eb3-a9b1-82ee4d72c29c"},
			"Exercise the Claude harness advanced",
		))
	})

	It("is idempotent — a second derivation is byte-identical", func() {
		a, _ := deriveCorpus()
		b, _ := deriveCorpus()
		Expect(len(a.Nodes)).To(Equal(len(b.Nodes)))
		for i := range a.Nodes {
			Expect(b.Nodes[i].Node.Hash).To(Equal(a.Nodes[i].Node.Hash))
			Expect(b.Nodes[i].Node.Kind).To(Equal(a.Nodes[i].Node.Kind))
			Expect(b.Nodes[i].Node.ParentToolUseID).To(Equal(a.Nodes[i].Node.ParentToolUseID))
		}
	})
})

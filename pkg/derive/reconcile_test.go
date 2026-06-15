package derive_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

var _ = Describe("ReconcileTranscripts", func() {
	mkWire := func(id int64, requestID, body, respBody string) storage.RawTurnRecord {
		return storage.RawTurnRecord{
			ID: id, Source: "wire", Provider: "anthropic",
			HarnessID: "claude", HarnessSessionID: "sess-1", RequestID: requestID,
			RawRequest: json.RawMessage(body), Response: json.RawMessage(respBody),
			Meta: json.RawMessage(`{}`),
		}
	}
	mkTranscript := func(agentID, toolUseID string, records string) storage.RawTurnRecord {
		meta, _ := json.Marshal(map[string]any{
			"transcript": true, "agent_id": agentID, "tool_use_id": toolUseID, "agent_type": "Explore",
		})
		return storage.RawTurnRecord{
			Source: "transcript", HarnessID: "claude", HarnessSessionID: "sess-1",
			RawRequest: json.RawMessage(records), Meta: meta,
		}
	}

	It("stamps the fork edge on the subagent's wire chain root", func() {
		// Two wire conversations in one session: the main thread and a
		// subagent thread (its own root, as captured on the wire).
		mainTurn := mkWire(1, "r1", `{
			"model":"m","max_tokens":32000,"stream":true,"tools":[{"name":"Bash"}],
			"messages":[{"role":"user","content":"main thread opener"}]
		}`, `{
			"model":"m","message":{"role":"assistant","content":[
				{"type":"text","text":"spawning the explorer"},
				{"type":"tool_use","tool_use_id":"toolu_task_1","tool_name":"Task","tool_input":{"prompt":"explore the repo"}}
			]},"stop_reason":"tool_use"
		}`)
		subTurn := mkWire(2, "r2", `{
			"model":"m","max_tokens":32000,"stream":true,"tools":[{"name":"Bash"}],
			"messages":[{"role":"user","content":"explore the repo please, subagent"}]
		}`, `{
			"model":"m","message":{"role":"assistant","content":[{"type":"text","text":"explored: 3 packages"}]},
			"stop_reason":"end_turn"
		}`)

		set, err := derive.BuildDerivedSet([]storage.RawTurnRecord{mainTurn, subTurn}, "")
		Expect(err).NotTo(HaveOccurred())

		// Transcript side: the main file matches the main thread, the
		// subagent file (forked by toolu_task_1) matches the subagent
		// thread.
		mainFileRec := mkTranscript("", "", `[
			{"uuid":"u1","message":{"role":"user","content":"main thread opener"}},
			{"uuid":"u2","parentUuid":"u1","message":{"role":"assistant","content":[
				{"type":"text","text":"spawning the explorer"},
				{"type":"tool_use","id":"toolu_task_1","name":"Task","input":{"prompt":"explore the repo"}}
			]}}
		]`)
		subFileRec := mkTranscript("abc123", "toolu_task_1", `[
			{"uuid":"s1","message":{"role":"user","content":"explore the repo please, subagent"}},
			{"uuid":"s2","parentUuid":"s1","message":{"role":"assistant","content":[
				{"type":"text","text":"explored: 3 packages"}
			]}}
		]`)
		mainFile, err := derive.ParseTranscriptFile(&mainFileRec)
		Expect(err).NotTo(HaveOccurred())
		subFile, err := derive.ParseTranscriptFile(&subFileRec)
		Expect(err).NotTo(HaveOccurred())

		stats := derive.ReconcileTranscripts(set, []*derive.TranscriptFile{mainFile, subFile})
		Expect(stats.SubagentForks).To(Equal(1))
		Expect(stats.ForkedChains).To(Equal(1))
		Expect(stats.MainChainsJoined).To(Equal(1))
		Expect(stats.ConversationJoined).To(BeNumerically(">=", 3))

		// The subagent chain root carries the fork edge; the main root
		// does not.
		var forkRoots, plainRoots int
		for _, dn := range set.Nodes {
			if dn.Node.ParentHash != nil || dn.Node.Kind != derive.KindMain {
				continue
			}
			switch dn.Node.ParentToolUseID {
			case "toolu_task_1":
				forkRoots++
			case "":
				plainRoots++
			}
		}
		Expect(forkRoots).To(Equal(1))
		Expect(plainRoots).To(Equal(1))
	})
})

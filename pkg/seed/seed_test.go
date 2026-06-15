package seed

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

var _ = Describe("loadCorpora", func() {
	It("loads the bundled corpora into wire and transcript rows", func() {
		wire, transcripts, err := loadCorpora()
		Expect(err).NotTo(HaveOccurred())
		Expect(wire).NotTo(BeEmpty())
		Expect(transcripts).NotTo(BeEmpty())

		for _, rec := range wire {
			Expect(rec.Source).To(Equal(storage.RawTurnSourceWire))
			Expect(rec.HarnessSessionID).NotTo(BeEmpty())
		}
		for _, rec := range transcripts {
			Expect(rec.Source).To(Equal(storage.RawTurnSourceTranscript))
		}
	})

	It("derives deterministic session identities from the corpora", func() {
		wire, transcripts, err := loadCorpora()
		Expect(err).NotTo(HaveOccurred())

		keys := sessionKeys(wire, transcripts)
		Expect(keys).To(HaveLen(2))

		// Re-loading yields the same identities in the same order.
		wire2, transcripts2, err := loadCorpora()
		Expect(err).NotTo(HaveOccurred())
		Expect(sessionKeys(wire2, transcripts2)).To(Equal(keys))
	})
})

var _ = Describe("demoEnvelope", func() {
	It("rewrites org identity and marks the session as demo", func() {
		raw := json.RawMessage(`{
			"org_id": "11111111-2222-3333-4444-555555555555",
			"harness_id": "claude",
			"harness_session_id": "abc",
			"cwd": "/work",
			"harness_metadata": {"kind": "interactive"}
		}`)

		out, err := demoEnvelope(raw, "")
		Expect(err).NotTo(HaveOccurred())

		var env map[string]any
		Expect(json.Unmarshal(out, &env)).To(Succeed())
		Expect(env["org_id"]).To(Equal(""))
		Expect(env["harness_id"]).To(Equal("claude"))
		Expect(env["cwd"]).To(Equal("/work"))

		meta, ok := env["harness_metadata"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(meta["demo"]).To(Equal(true))
		Expect(meta["kind"]).To(Equal("interactive"), "existing metadata survives the merge")
	})

	It("builds an envelope from nothing when the row carried none", func() {
		out, err := demoEnvelope(nil, "99999999-9999-9999-9999-999999999999")
		Expect(err).NotTo(HaveOccurred())

		var env map[string]any
		Expect(json.Unmarshal(out, &env)).To(Succeed())
		Expect(env["org_id"]).To(Equal("99999999-9999-9999-9999-999999999999"))
		meta := env["harness_metadata"].(map[string]any)
		Expect(meta["demo"]).To(Equal(true))
	})
})

var _ = Describe("payload reconstruction", func() {
	It("rebuilds a wire turn body verbatim around the rewritten envelope", func() {
		rec := storage.RawTurnRecord{
			Provider:        "anthropic",
			AgentName:       "claude-code",
			RawRequest:      json.RawMessage(`{"model":"m","messages":[]}`),
			Response:        json.RawMessage(`{"message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}`),
			Meta:            json.RawMessage(`{"request_id":"turn-1"}`),
			SessionEnvelope: json.RawMessage(`{"harness_id":"claude","harness_session_id":"s1"}`),
		}

		body, err := wireTurnBody(&rec, "")
		Expect(err).NotTo(HaveOccurred())

		var payload map[string]json.RawMessage
		Expect(json.Unmarshal(body, &payload)).To(Succeed())
		Expect(string(payload["provider"])).To(Equal(`"anthropic"`))
		Expect(string(payload["agent_name"])).To(Equal(`"claude-code"`))
		Expect(payload["request"]).To(MatchJSON(rec.RawRequest))
		Expect(payload["response"]).To(MatchJSON(rec.Response))
		Expect(payload["meta"]).To(MatchJSON(rec.Meta))

		var env map[string]any
		Expect(json.Unmarshal(payload["session"], &env)).To(Succeed())
		Expect(env["harness_session_id"]).To(Equal("s1"))
		Expect(env["harness_metadata"].(map[string]any)["demo"]).To(Equal(true))
	})

	It("rebuilds a transcript body from the stored meta attribution", func() {
		rec := storage.RawTurnRecord{
			Source:          storage.RawTurnSourceTranscript,
			RawRequest:      json.RawMessage(`[{"type":"user","uuid":"u1"}]`),
			Meta:            json.RawMessage(`{"transcript":true,"agent_id":"a1","agent_type":"Plan","description":"d","tool_use_id":"toolu_1","records":1}`),
			SessionEnvelope: json.RawMessage(`{"harness_id":"claude","harness_session_id":"s1"}`),
		}

		body, err := transcriptBody(&rec, "")
		Expect(err).NotTo(HaveOccurred())

		var payload map[string]json.RawMessage
		Expect(json.Unmarshal(body, &payload)).To(Succeed())
		Expect(payload["records"]).To(MatchJSON(rec.RawRequest))
		Expect(string(payload["agent_id"])).To(Equal(`"a1"`))
		Expect(string(payload["agent_type"])).To(Equal(`"Plan"`))
		Expect(string(payload["tool_use_id"])).To(Equal(`"toolu_1"`))
	})
})

var _ = Describe("Run", func() {
	It("refuses drivers without the raw-turn layer", func() {
		_, err := Run(context.Background(), nil, nil, "")
		Expect(err).To(MatchError(ErrUnsupportedDriver))
	})
})

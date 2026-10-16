package sessions_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

var _ = Describe("IngestEnvelope.Validate", func() {
	It("accepts a nil receiver as 'no envelope'", func() {
		var e *sessions.IngestEnvelope
		Expect(e.Validate()).To(Succeed())
	})

	It("accepts a zero-value envelope", func() {
		e := &sessions.IngestEnvelope{}
		Expect(e.Validate()).To(Succeed())
	})

	It("accepts a fully-populated valid envelope", func() {
		e := &sessions.IngestEnvelope{
			OrgID:                  "11111111-1111-1111-1111-111111111111",
			AuthSubject:            "subject",
			HarnessID:              "claude",
			HarnessSessionID:       "hs-1",
			ParentHarnessSessionID: new("parent-1"),
			HarnessMetadata:        json.RawMessage(`{"ai_title":"hello"}`),
		}
		Expect(e.Validate()).To(Succeed())
	})

	It("accepts an empty OrgID", func() {
		e := &sessions.IngestEnvelope{}
		Expect(e.Validate()).To(Succeed())
	})

	It("rejects an OrgID that is not a valid UUID", func() {
		e := &sessions.IngestEnvelope{
			OrgID: "not-a-uuid",
		}
		err := e.Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("org_id"))
	})

	It("rejects ParentHarnessSessionID pointing at an empty string", func() {
		empty := ""
		e := &sessions.IngestEnvelope{
			ParentHarnessSessionID: &empty,
		}
		err := e.Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("parent_harness_session_id"))
	})

	It("rejects ParentHarnessID pointing at an empty string", func() {
		empty := ""
		e := &sessions.IngestEnvelope{
			ParentHarnessSessionID: new("parent-1"),
			ParentHarnessID:        &empty,
		}
		err := e.Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("parent_harness_id"))
	})

	It("accepts a cross-harness parent and a harness outside the known set", func() {
		e := &sessions.IngestEnvelope{
			HarnessID:              "hermes",
			HarnessSessionID:       "hs-1",
			ParentHarnessSessionID: new("parent-1"),
			ParentHarnessID:        new("claude"),
		}
		Expect(e.Validate()).To(Succeed())
	})

	It("accepts a ParentHarnessID without a ParentHarnessSessionID (ignored for linkage)", func() {
		e := &sessions.IngestEnvelope{
			HarnessID:       "codex",
			ParentHarnessID: new("claude"),
		}
		Expect(e.Validate()).To(Succeed())
	})

	It("rejects HarnessMetadata that is a JSON array", func() {
		e := &sessions.IngestEnvelope{
			HarnessMetadata: json.RawMessage(`["a","b"]`),
		}
		err := e.Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("harness_metadata"))
	})

	It("rejects HarnessMetadata that is a JSON scalar", func() {
		e := &sessions.IngestEnvelope{
			HarnessMetadata: json.RawMessage(`42`),
		}
		err := e.Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("harness_metadata"))
	})

	It("rejects malformed HarnessMetadata", func() {
		e := &sessions.IngestEnvelope{
			HarnessMetadata: json.RawMessage(`{not json`),
		}
		err := e.Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("harness_metadata"))
	})

	It("accepts an empty/absent HarnessMetadata", func() {
		e := &sessions.IngestEnvelope{}
		Expect(e.Validate()).To(Succeed())
	})
})

var _ = Describe("IngestEnvelope.ParentHarnessIDOrChild", func() {
	It("uses the explicit parent harness when one is named", func() {
		e := &sessions.IngestEnvelope{HarnessID: "codex", ParentHarnessID: new("claude")}
		Expect(e.ParentHarnessIDOrChild()).To(Equal("claude"))
	})

	It("falls back to the child's harness when none is named", func() {
		e := &sessions.IngestEnvelope{HarnessID: "codex"}
		Expect(e.ParentHarnessIDOrChild()).To(Equal("codex"))
	})

	It("treats an explicit empty parent harness as absent", func() {
		empty := ""
		e := &sessions.IngestEnvelope{HarnessID: "codex", ParentHarnessID: &empty}
		Expect(e.ParentHarnessIDOrChild()).To(Equal("codex"))
	})

	It("normalizes an empty child harness to unknown, like HarnessIDOrUnknown", func() {
		Expect((&sessions.IngestEnvelope{}).ParentHarnessIDOrChild()).To(Equal(sessions.HarnessIDUnknown))
		var nilEnvelope *sessions.IngestEnvelope
		Expect(nilEnvelope.ParentHarnessIDOrChild()).To(Equal(sessions.HarnessIDUnknown))
	})

	It("round-trips parent_harness_id through the stored envelope JSON", func() {
		e := sessions.IngestEnvelope{HarnessID: "codex", ParentHarnessSessionID: new("p"), ParentHarnessID: new("claude")}
		b, err := json.Marshal(e)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(ContainSubstring(`"parent_harness_id":"claude"`))
		var back sessions.IngestEnvelope
		Expect(json.Unmarshal(b, &back)).To(Succeed())
		Expect(back.ParentHarnessID).To(HaveValue(Equal("claude")))

		b, err = json.Marshal(sessions.IngestEnvelope{HarnessID: "claude"})
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).NotTo(ContainSubstring("parent_harness_id"))
	})
})

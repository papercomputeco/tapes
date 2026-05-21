package sessions_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// stringPtr is a tiny helper for building *string fields inline.
func stringPtr(s string) *string { return &s }

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
			ParentHarnessSessionID: stringPtr("parent-1"),
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

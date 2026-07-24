package backfill

// Executable oracle for the shared envelope fixtures (fixtures/envelope/cases).
//
// Those cases pin the X-Tapes-* header -> session-envelope contract for every
// consumer. On their own they are inert JSON: nothing loads them, so they can
// silently drift from the parser they claim to describe. This test is the
// consumer that keeps them honest — it runs each case's header set through the
// real reader (sessionEnvelopeFromHeaders) plus IngestEnvelope.Validate and
// asserts the parsed envelope and validation outcome the case declares.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

type envelopeFixtureCase struct {
	Name      string            `json:"name"`
	Category  string            `json:"category"`
	Direction string            `json:"direction"`
	Headers   map[string]string `json:"headers"`
	Envelope  json.RawMessage   `json:"envelope"`
	Error     *struct {
		Field       string `json:"field"`
		Rule        string `json:"rule"`
		Disposition string `json:"disposition"`
	} `json:"error"`
}

// envelopeEntries loads the fixture cases at tree-construction time. It panics
// on IO/parse errors (there is no running spec yet to hang a Gomega assertion
// on); Ginkgo surfaces the panic as a construction failure.
func envelopeEntries() []TableEntry {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "envelope", "cases")
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		panic(err)
	}
	if len(matches) == 0 {
		panic("no envelope fixture cases found under " + dir)
	}
	sort.Strings(matches)
	entries := make([]TableEntry, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			panic(err)
		}
		var c envelopeFixtureCase
		if err := json.Unmarshal(b, &c); err != nil {
			panic(fmt.Sprintf("%s: %v", m, err))
		}
		entries = append(entries, Entry(c.Name, c))
	}
	return entries
}

// jsonEqual compares two JSON payloads structurally (key order irrelevant),
// treating empty/absent as equal.
func jsonEqual(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

var _ = Describe("envelope fixture corpus (reader oracle)", func() {
	DescribeTable("each case decodes and validates exactly as it claims",
		func(c envelopeFixtureCase) {
			hdr := func(name string) string { return c.Headers[name] }

			// The reader maps only the X-Tapes-* headers. org_id / auth_subject
			// are populated server-side from the trusted X-Paper-Auth-* headers
			// (a verbatim passthrough), so apply that here before comparing and
			// validating the full envelope.
			actual := sessionEnvelopeFromHeaders(hdr)
			if actual == nil {
				actual = &sessions.IngestEnvelope{}
			}
			actual.OrgID = hdr("x-paper-auth-org-id")
			actual.AuthSubject = hdr("x-paper-auth-subject")

			var expected sessions.IngestEnvelope
			Expect(json.Unmarshal(c.Envelope, &expected)).To(Succeed())

			// harness_id is compared through the shared normalization every
			// consumer applies (missing/empty -> "unknown").
			Expect(actual.HarnessIDOrUnknown()).To(Equal(expected.HarnessIDOrUnknown()), "harness_id")
			Expect(actual.HarnessSessionID).To(Equal(expected.HarnessSessionID), "harness_session_id")
			Expect(actual.HarnessVersion).To(Equal(expected.HarnessVersion), "harness_version")
			Expect(actual.Cwd).To(Equal(expected.Cwd), "cwd")
			Expect(actual.Name).To(Equal(expected.Name), "name")
			Expect(actual.OrgID).To(Equal(expected.OrgID), "org_id")
			Expect(actual.AuthSubject).To(Equal(expected.AuthSubject), "auth_subject")

			switch {
			case actual.ParentHarnessSessionID == nil && expected.ParentHarnessSessionID == nil:
			case actual.ParentHarnessSessionID != nil && expected.ParentHarnessSessionID != nil:
				Expect(*actual.ParentHarnessSessionID).To(Equal(*expected.ParentHarnessSessionID), "parent_harness_session_id")
			default:
				Fail("parent_harness_session_id presence mismatch")
			}

			Expect(jsonEqual(actual.HarnessMetadata, expected.HarnessMetadata)).To(BeTrue(),
				"harness_metadata: got %s want %s", actual.HarnessMetadata, expected.HarnessMetadata)

			// Validation outcome: reject-400 cases must fail Validate; every
			// other case (valid, or a non-fatal drop-field) must pass.
			err := actual.Validate()
			if c.Error != nil && c.Error.Disposition == "reject-400" {
				Expect(err).To(HaveOccurred(), "expected validation to reject case %q", c.Name)
			} else {
				Expect(err).NotTo(HaveOccurred(), "expected case %q to validate", c.Name)
			}
		},
		envelopeEntries(),
	)
})

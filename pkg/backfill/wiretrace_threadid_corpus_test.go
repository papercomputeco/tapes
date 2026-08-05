package backfill

// Consumer oracle for the shared thread fixture corpus (fixtures/thread/),
// which pins the harness-native sub-thread header ↔ meta.thread_id contract.
//
// threadIDFromHeaders is one of four independent readers of that vocabulary
// (with extproc/headers.ThreadID, proxy/header.ThreadID, and tapes-harnesses'
// envelope::thread_id). The corpus is what keeps them resolving identical
// bytes identically; the canonical header spellings live in tapes-harnesses
// src/envelope.rs (HARNESS_THREAD_ID_RULES). Schema validation, the DIGEST
// seal, and rule coverage are the authored-home gate's job
// (extproc/headers/thread_corpus_test.go); this file only proves the backfill
// reader agrees.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("threadIDFromHeaders over the shared thread corpus", func() {
	type corpusCase struct {
		Name     string            `json:"name"`
		Headers  map[string]string `json:"headers"`
		ThreadID string            `json:"thread_id"`
	}

	It("resolves every corpus case exactly like the other readers", func() {
		_, file, _, ok := runtime.Caller(0)
		Expect(ok).To(BeTrue())
		dir := filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "thread", "cases")

		matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
		Expect(err).NotTo(HaveOccurred())
		Expect(matches).NotTo(BeEmpty(), "no thread corpus cases under %s", dir)
		sort.Strings(matches)

		for _, m := range matches {
			b, err := os.ReadFile(m)
			Expect(err).NotTo(HaveOccurred())
			var c corpusCase
			Expect(json.Unmarshal(b, &c)).To(Succeed(), m)

			Expect(threadIDFromHeaders(headersFrom(c.Headers))).To(Equal(c.ThreadID),
				"case %s: the backfill reader disagrees with the shared corpus; the canonical vocabulary is tapes-harnesses src/envelope.rs (HARNESS_THREAD_ID_RULES)", c.Name)
		}
	})
})

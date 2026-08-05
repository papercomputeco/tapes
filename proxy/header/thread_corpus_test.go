package header

// Consumer oracle for the shared thread fixture corpus (fixtures/thread/),
// which pins the harness-native sub-thread header ↔ meta.thread_id contract.
//
// This package's ThreadID is one of four independent readers of that
// vocabulary (with extproc/headers.ThreadID, pkg/backfill's
// threadIDFromHeaders, and tapes-harnesses' envelope::thread_id). The corpus
// is what keeps them resolving identical bytes identically; the canonical
// header spellings live in tapes-harnesses src/envelope.rs
// (HARNESS_THREAD_ID_RULES). Schema validation, the DIGEST seal, and rule
// coverage are the authored-home gate's job (extproc/headers/
// thread_corpus_test.go); this file only proves the fiber-transport reader
// agrees.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type threadCorpusCase struct {
	Name     string            `json:"name"`
	Headers  map[string]string `json:"headers"`
	ThreadID string            `json:"thread_id"`
}

func loadThreadCorpus() []threadCorpusCase {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue())
	dir := filepath.Join(filepath.Dir(file), "..", "..", "fixtures", "thread", "cases")

	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	Expect(err).NotTo(HaveOccurred())
	Expect(matches).NotTo(BeEmpty(), "no thread corpus cases under %s", dir)
	sort.Strings(matches)

	out := make([]threadCorpusCase, 0, len(matches))
	for _, m := range matches {
		b, err := os.ReadFile(m)
		Expect(err).NotTo(HaveOccurred())
		var c threadCorpusCase
		Expect(json.Unmarshal(b, &c)).To(Succeed(), m)
		out = append(out, c)
	}
	return out
}

var _ = Describe("ThreadID over the shared thread corpus", func() {
	// A fresh app per case: fiber routes are first-registered-wins, so
	// reusing one app across cases would pin every request to the first
	// case's handler closure.
	resolve := func(hdrs map[string]string) string {
		app := fiber.New()
		defer app.Shutdown()

		var got string
		app.Post("/test", func(c *fiber.Ctx) error {
			got = ThreadID(c)
			return c.SendStatus(fiber.StatusOK)
		})
		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		for k, v := range hdrs {
			req.Header.Set(k, v)
		}
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()
		return got
	}

	It("resolves every corpus case exactly like the other readers", func() {
		for _, c := range loadThreadCorpus() {
			Expect(resolve(c.Headers)).To(Equal(c.ThreadID),
				"case %s: the proxy reader disagrees with the shared corpus; the canonical vocabulary is tapes-harnesses src/envelope.rs (HARNESS_THREAD_ID_RULES)", c.Name)
		}
	})
})

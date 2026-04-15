package backfill_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/backfill"
)

var _ = Describe("LoadSessionStubs", func() {
	It("returns the parsed stubs from ~/.claude/sessions/*.json", func() {
		dir := GinkgoT().TempDir()

		writeFile(dir, "8086.json", `{"pid":8086,"sessionId":"s-aaa","cwd":"/Users/me/proj","startedAt":1776179941698,"kind":"interactive","entrypoint":"cli","bridgeSessionId":null}`)
		writeFile(dir, "8896.json", `{"pid":8896,"sessionId":"s-bbb","cwd":"/Users/me/other","startedAt":1776179942000,"kind":"interactive","entrypoint":"cli","bridgeSessionId":null}`)

		stubs, err := backfill.LoadSessionStubs(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(stubs).To(HaveLen(2))

		bySessionID := map[string]backfill.SessionStub{}
		for _, s := range stubs {
			bySessionID[s.SessionID] = s
		}
		Expect(bySessionID["s-aaa"].CWD).To(Equal("/Users/me/proj"))
		Expect(bySessionID["s-aaa"].PID).To(Equal(8086))
		Expect(bySessionID["s-bbb"].CWD).To(Equal("/Users/me/other"))
	})

	It("returns an empty slice when the directory does not exist", func() {
		dir := filepath.Join(GinkgoT().TempDir(), "missing")

		stubs, err := backfill.LoadSessionStubs(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(stubs).To(BeEmpty())
	})

	It("skips files that are not valid JSON", func() {
		dir := GinkgoT().TempDir()
		writeFile(dir, "good.json", `{"pid":1,"sessionId":"s-1","cwd":"/p"}`)
		writeFile(dir, "garbage.json", `not json at all`)

		stubs, err := backfill.LoadSessionStubs(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(stubs).To(HaveLen(1))
		Expect(stubs[0].SessionID).To(Equal("s-1"))
	})

	It("ignores entries without a sessionId", func() {
		dir := GinkgoT().TempDir()
		writeFile(dir, "stub.json", `{"pid":1,"cwd":"/p"}`)

		stubs, err := backfill.LoadSessionStubs(dir)
		Expect(err).NotTo(HaveOccurred())
		Expect(stubs).To(BeEmpty())
	})
})

var _ = Describe("EncodeCWD", func() {
	It("replaces every non-alphanumeric character with a dash", func() {
		Expect(backfill.EncodeCWD("/Users/me/proj")).To(Equal("-Users-me-proj"))
	})

	It("preserves alphanumerics", func() {
		Expect(backfill.EncodeCWD("/a1/b2/c3")).To(Equal("-a1-b2-c3"))
	})

	It("collapses dots and other punctuation to dashes", func() {
		Expect(backfill.EncodeCWD("/Users/me/code.app/sub_dir")).To(Equal("-Users-me-code-app-sub-dir"))
	})
})

func writeFile(dir, name, content string) string {
	path := filepath.Join(dir, name)
	Expect(os.WriteFile(path, []byte(content), 0o644)).To(Succeed())
	return path
}

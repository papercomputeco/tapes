package backfill

// Unit coverage for threadIDFromHeaders, the backfill-side mirror of
// extproc's headers.ThreadID. The cases here are the ones that separate
// the two harness shapes: Claude Code signals a subagent by the mere
// presence of its agent-id header, while Codex stamps thread-id on every
// call and only signals a sub-thread when thread-id and session-id
// diverge.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("threadIDFromHeaders", func() {
	It("resolves Claude Code's agent-id header", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"x-claude-code-agent-id": "agent-7",
		}))).To(Equal("agent-7"))
	})

	It("prefers the Claude list over the Codex pair", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"x-claude-code-agent-id": "agent-7",
			"thread-id":              "thr-2",
			"session-id":             "sess-1",
		}))).To(Equal("agent-7"))
	})

	It("resolves a Codex sub-thread when the pair diverges", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"thread-id":  "thr-2",
			"session-id": "sess-1",
		}))).To(Equal("thr-2"))
	})

	// The root guard: a non-empty thread_id on a root turn would misroute
	// the root spine into derive's threadCall path, degrading the derived
	// status (terminalMainSpan requires ThreadID=="").
	It("resolves a Codex root turn to empty when the pair matches", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"thread-id":  "sess-1",
			"session-id": "sess-1",
		}))).To(BeEmpty())
	})

	It("ignores a lone thread-id with no session-id counterpart", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"thread-id": "thr-2",
		}))).To(BeEmpty())
	})

	It("ignores a lone session-id with no thread-id", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"session-id": "sess-1",
		}))).To(BeEmpty())
	})

	It("resolves to empty when no thread headers are present", func() {
		Expect(threadIDFromHeaders(headersFrom(map[string]string{
			"x-tapes-harness-id": "codex",
		}))).To(BeEmpty())
	})
})

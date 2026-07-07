package sessions_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

var _ = Describe("DetectToolOutcomes", func() {
	Describe("the gh CLI matcher family", func() {
		It("detects a pull request from gh pr create stdout", func() {
			outcomes := sessions.DetectToolOutcomes(
				"Bash",
				map[string]any{"command": `gh pr create --title "fix" --body "..."`},
				"Creating pull request for fix into main\nhttps://github.com/papercomputeco/paper/pull/94\n",
			)
			Expect(outcomes).To(HaveLen(1))
			Expect(outcomes[0].Kind).To(Equal(sessions.OutcomeKindPullRequest))
			Expect(outcomes[0].URL).To(Equal("https://github.com/papercomputeco/paper/pull/94"))
			Expect(outcomes[0].Repo).To(Equal("papercomputeco/paper"))
			Expect(outcomes[0].DetectedBy).To(Equal(sessions.OutcomeDetectedByGhCli))
		})

		It("detects a repo from gh repo create stdout", func() {
			outcomes := sessions.DetectToolOutcomes(
				"Bash",
				map[string]any{"command": "gh repo create papercomputeco/outcome-detector --private"},
				"✓ Created repository papercomputeco/outcome-detector on GitHub\nhttps://github.com/papercomputeco/outcome-detector\n",
			)
			Expect(outcomes).To(HaveLen(1))
			Expect(outcomes[0].Kind).To(Equal(sessions.OutcomeKindRepo))
			Expect(outcomes[0].URL).To(Equal("https://github.com/papercomputeco/outcome-detector"))
			Expect(outcomes[0].Repo).To(Equal("papercomputeco/outcome-detector"))
		})

		It("detects an issue from gh issue create stdout", func() {
			outcomes := sessions.DetectToolOutcomes(
				"Bash",
				map[string]any{"command": `gh issue create --title "bug"`},
				"https://github.com/papercomputeco/tapes/issues/212\n",
			)
			Expect(outcomes).To(HaveLen(1))
			Expect(outcomes[0].Kind).To(Equal(sessions.OutcomeKindIssue))
			Expect(outcomes[0].Repo).To(Equal("papercomputeco/tapes"))
		})

		It("matches the command case-insensitively but keeps the URL verbatim", func() {
			outcomes := sessions.DetectToolOutcomes(
				"Bash",
				map[string]any{"command": "GH PR CREATE --fill"},
				"https://github.com/papercomputeco/paper/pull/95",
			)
			Expect(outcomes).To(HaveLen(1))
			Expect(outcomes[0].URL).To(Equal("https://github.com/papercomputeco/paper/pull/95"))
		})

		It("yields nothing when the create printed no URL (errored run)", func() {
			outcomes := sessions.DetectToolOutcomes(
				"Bash",
				map[string]any{"command": "gh pr create --fill"},
				"pull request create failed: GraphQL: was submitted too quickly",
			)
			Expect(outcomes).To(BeEmpty())
		})

		It("yields nothing for unrelated shell commands that merely print a PR URL", func() {
			outcomes := sessions.DetectToolOutcomes(
				"Bash",
				map[string]any{"command": "gh pr view 94"},
				"https://github.com/papercomputeco/paper/pull/94",
			)
			Expect(outcomes).To(BeEmpty())
		})

		It("yields nothing for a Bash call without a command string", func() {
			Expect(sessions.DetectToolOutcomes("Bash", map[string]any{}, "output")).To(BeEmpty())
		})
	})

	Describe("the MCP matcher family", func() {
		It("detects a Linear issue from a save_issue result payload", func() {
			outcomes := sessions.DetectToolOutcomes(
				"mcp__linear-server__save_issue",
				map[string]any{"team": "Engineering"},
				`{"id":"PCC-837","title":"Explore: Outcomes as a feature","url":"https://linear.app/paper-compute-co/issue/PCC-837/explore-outcomes"}`,
			)
			Expect(outcomes).To(HaveLen(1))
			Expect(outcomes[0].Kind).To(Equal(sessions.OutcomeKindLinearIssue))
			Expect(outcomes[0].URL).To(Equal("https://linear.app/paper-compute-co/issue/PCC-837/explore-outcomes"))
			Expect(outcomes[0].Title).To(Equal("Explore: Outcomes as a feature"))
			Expect(outcomes[0].Repo).To(Equal("paper-compute-co"))
			Expect(outcomes[0].DetectedBy).To(Equal(sessions.OutcomeDetectedByMCP))
		})

		It("falls back to a bare URL scan when the result is not JSON", func() {
			outcomes := sessions.DetectToolOutcomes(
				"mcp__claude_ai_Linear__create_issue",
				nil,
				"Created https://linear.app/paper-compute-co/issue/PCC-845 for you.",
			)
			Expect(outcomes).To(HaveLen(1))
			Expect(outcomes[0].URL).To(Equal("https://linear.app/paper-compute-co/issue/PCC-845"))
		})

		It("yields nothing for non-issue Linear tools", func() {
			outcomes := sessions.DetectToolOutcomes(
				"mcp__linear-server__list_issues",
				nil,
				`[{"url":"https://linear.app/paper-compute-co/issue/PCC-1"}]`,
			)
			Expect(outcomes).To(BeEmpty())
		})

		It("yields nothing when the result carries no linear issue URL", func() {
			outcomes := sessions.DetectToolOutcomes(
				"mcp__linear-server__save_issue",
				nil,
				`{"error":"unauthorized"}`,
			)
			Expect(outcomes).To(BeEmpty())
		})
	})

	It("yields nothing for empty output or unmatched tools", func() {
		Expect(sessions.DetectToolOutcomes("Bash", map[string]any{"command": "gh pr create"}, "")).To(BeEmpty())
		Expect(sessions.DetectToolOutcomes("Edit", map[string]any{"file_path": "x"}, "ok")).To(BeEmpty())
	})
})

var _ = Describe("DedupeOutcomes", func() {
	It("drops repeat URLs keeping the first occurrence", func() {
		first := sessions.Outcome{Kind: sessions.OutcomeKindPullRequest, URL: "https://github.com/o/r/pull/1", SpanID: "tool_1"}
		repeat := sessions.Outcome{Kind: sessions.OutcomeKindPullRequest, URL: "https://github.com/o/r/pull/1", SpanID: "tool_2"}
		other := sessions.Outcome{Kind: sessions.OutcomeKindIssue, URL: "https://github.com/o/r/issues/2"}

		deduped := sessions.DedupeOutcomes([]sessions.Outcome{first, repeat, other})

		Expect(deduped).To(HaveLen(2))
		Expect(deduped[0].SpanID).To(Equal("tool_1"))
		Expect(deduped[1].URL).To(Equal("https://github.com/o/r/issues/2"))
	})

	It("passes short slices through untouched", func() {
		one := []sessions.Outcome{{URL: "u"}}
		Expect(sessions.DedupeOutcomes(one)).To(Equal(one))
		Expect(sessions.DedupeOutcomes(nil)).To(BeNil())
	})
})

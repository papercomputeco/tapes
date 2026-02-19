package deck

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
)

var _ = Describe("determineStatus", func() {
	leafNode := func(role, stopReason string) *ent.Node {
		return &ent.Node{Role: role, StopReason: stopReason}
	}

	DescribeTable("without git activity",
		func(role, stopReason string, hasToolError bool, expected string) {
			node := leafNode(role, stopReason)
			Expect(determineStatus(node, hasToolError, false)).To(Equal(expected))
		},
		Entry("assistant stop → completed", "assistant", "stop", false, StatusCompleted),
		Entry("assistant end_turn → completed", "assistant", "end_turn", false, StatusCompleted),
		Entry("assistant end-turn → completed", "assistant", "end-turn", false, StatusCompleted),
		Entry("assistant eos → completed", "assistant", "eos", false, StatusCompleted),
		Entry("assistant length → failed", "assistant", "length", false, StatusFailed),
		Entry("assistant max_tokens → failed", "assistant", "max_tokens", false, StatusFailed),
		Entry("assistant content_filter → failed", "assistant", "content_filter", false, StatusFailed),
		Entry("assistant tool_use → failed", "assistant", "tool_use", false, StatusFailed),
		Entry("assistant tool_use_response → failed", "assistant", "tool_use_response", false, StatusFailed),
		Entry("assistant error reason → failed", "assistant", "server_error", false, StatusFailed),
		Entry("assistant empty reason → unknown", "assistant", "", false, StatusUnknown),
		Entry("assistant unrecognized reason → unknown", "assistant", "something_else", false, StatusUnknown),
		Entry("user last message → abandoned", "user", "", false, StatusAbandoned),
		Entry("tool error overrides everything → failed", "assistant", "stop", true, StatusFailed),
		Entry("tool error + user message → failed", "user", "", true, StatusFailed),
	)

	DescribeTable("with git activity",
		func(role, stopReason string, hasToolError bool, expected string) {
			node := leafNode(role, stopReason)
			Expect(determineStatus(node, hasToolError, true)).To(Equal(expected))
		},
		Entry("user last message + git activity → completed", "user", "", false, StatusCompleted),
		Entry("assistant unknown stop + git activity → completed", "assistant", "something_else", false, StatusCompleted),
		Entry("assistant empty stop + git activity → completed", "assistant", "", false, StatusCompleted),
		Entry("tool error still wins over git activity → failed", "assistant", "stop", true, StatusFailed),
	)
})

var _ = Describe("blocksHaveGitActivity", func() {
	bashBlock := func(command string) llm.ContentBlock {
		return llm.ContentBlock{
			Type:      "tool_use",
			ToolName:  "Bash",
			ToolInput: map[string]any{"command": command},
		}
	}

	It("detects git commit", func() {
		blocks := []llm.ContentBlock{
			bashBlock("git commit -m 'fix bug'"),
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeTrue())
	})

	It("detects git push", func() {
		blocks := []llm.ContentBlock{
			bashBlock("git push origin main"),
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeTrue())
	})

	It("detects git commit in chained commands", func() {
		blocks := []llm.ContentBlock{
			bashBlock("git add . && git commit -m 'update' && git push"),
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeTrue())
	})

	It("is case insensitive", func() {
		blocks := []llm.ContentBlock{
			bashBlock("GIT PUSH origin feature"),
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeTrue())
	})

	It("returns false for non-git bash commands", func() {
		blocks := []llm.ContentBlock{
			bashBlock("go test ./..."),
			bashBlock("make build"),
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeFalse())
	})

	It("returns false for git commands that are not commit/push", func() {
		blocks := []llm.ContentBlock{
			bashBlock("git status"),
			bashBlock("git diff"),
			bashBlock("git log --oneline"),
			bashBlock("git checkout main"),
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeFalse())
	})

	It("ignores non-Bash tool use blocks", func() {
		blocks := []llm.ContentBlock{
			{
				Type:      "tool_use",
				ToolName:  "Read",
				ToolInput: map[string]any{"path": "git commit log"},
			},
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeFalse())
	})

	It("ignores text blocks", func() {
		blocks := []llm.ContentBlock{
			{Type: "text", Text: "I ran git commit and git push"},
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeFalse())
	})

	It("ignores Bash blocks with empty command", func() {
		blocks := []llm.ContentBlock{
			{
				Type:      "tool_use",
				ToolName:  "Bash",
				ToolInput: map[string]any{},
			},
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeFalse())
	})

	It("returns false for empty blocks", func() {
		Expect(blocksHaveGitActivity(nil)).To(BeFalse())
		Expect(blocksHaveGitActivity([]llm.ContentBlock{})).To(BeFalse())
	})

	It("finds git activity among many blocks", func() {
		blocks := []llm.ContentBlock{
			{Type: "text", Text: "Let me fix the tests"},
			bashBlock("go test ./..."),
			bashBlock("make lint"),
			bashBlock("git add -A && git commit -m 'fix tests'"),
			{Type: "text", Text: "All done."},
		}
		Expect(blocksHaveGitActivity(blocks)).To(BeTrue())
	})
})

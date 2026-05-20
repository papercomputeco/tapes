package merkle_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// userBucket builds the kind of user-role bucket the worker pool
// hashes when storing a turn — identical shape for every test below so
// the only inputs that vary across hashes are the content blocks.
func userBucket(content []llm.ContentBlock) merkle.Bucket {
	return merkle.Bucket{
		Type:      "message",
		Role:      "user",
		Content:   content,
		Model:     "claude-opus-4-7",
		Provider:  "anthropic",
		AgentName: "claude",
	}
}

var _ = Describe("ProjectContent", func() {
	It("strips a <system-reminder> span and leaves the prose intact", func() {
		blocks := []llm.ContentBlock{{
			Type: "text",
			Text: "<system-reminder>\n# currentDate\nToday's date is 2026-05-20.\n</system-reminder>\n\nHey Claude!",
		}}

		projected := merkle.ProjectContent(blocks)

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].Text).To(Equal("Hey Claude!"))
	})

	It("drops a block that is entirely harness content", func() {
		blocks := []llm.ContentBlock{
			{Type: "text", Text: "<system-reminder>MCP inventory</system-reminder>"},
			{Type: "text", Text: "<system-reminder>skill catalogue</system-reminder>"},
			{Type: "text", Text: "Hey Claude!"},
		}

		projected := merkle.ProjectContent(blocks)

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].Text).To(Equal("Hey Claude!"))
	})

	It("strips every harness tag the spec lists", func() {
		// Each tag wraps a marker so we can prove the strip happened.
		tags := []string{
			"system-reminder",
			"command-name", "command-message", "command-args",
			"local-command-stdout", "local-command-stderr", "local-command-caveat",
		}
		var body string
		for _, t := range tags {
			body += "<" + t + ">drop-" + t + "</" + t + ">\n"
		}
		body += "keep"

		projected := merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: body}})

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].Text).To(Equal("keep"))
	})

	It("collapses a blank line inserted between paragraphs (PCC-562 57a58 case)", func() {
		original := "line A\nline B\nline C"
		drift := "line A\nline B\n\nline C"

		Expect(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: original}})).
			To(Equal(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: drift}})))
	})

	It("ignores trailing whitespace and CRLF drift", func() {
		original := "line A\nline B"
		drift := "line A   \r\nline B  "

		Expect(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: original}})).
			To(Equal(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: drift}})))
	})

	It("preserves non-text blocks verbatim", func() {
		blocks := []llm.ContentBlock{
			{Type: "tool_use", ToolName: "Bash", ToolUseID: "abc", ToolInput: map[string]any{"cmd": "ls"}},
			{Type: "image", ImageURL: "https://example.com/x.png", MediaType: "image/png"},
		}

		projected := merkle.ProjectContent(blocks)

		Expect(projected).To(Equal(blocks))
	})

	It("does not mutate the input slice or its blocks", func() {
		input := []llm.ContentBlock{
			{Type: "text", Text: "<system-reminder>drop</system-reminder>keep"},
		}
		_ = merkle.ProjectContent(input)

		Expect(input[0].Text).To(Equal("<system-reminder>drop</system-reminder>keep"))
	})

	It("swallows an unterminated harness tag to end-of-text", func() {
		blocks := []llm.ContentBlock{{
			Type: "text",
			Text: "keep this\n<system-reminder>and then drift never closes",
		}}

		projected := merkle.ProjectContent(blocks)

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].Text).To(Equal("keep this"))
	})
})

var _ = Describe("Node.Hash with harness drift", func() {
	prose := "Hey Claude! I'd like to exercise-claude-harness-basic, as defined in our skills directory."

	It("hashes a pure-prose user turn the same as a drift-fork of that same turn (PCC-562)", func() {
		// Matches the actual capture pair observed in the dev corpus:
		// one root has a single prose text block; the drift fork has the
		// same prose preceded by three <system-reminder> blocks.
		named := merkle.NewNode(userBucket([]llm.ContentBlock{
			{Type: "text", Text: prose},
		}), nil)

		drifted := merkle.NewNode(userBucket([]llm.ContentBlock{
			{Type: "text", Text: "<system-reminder>\n# MCP Server Instructions\n…\n</system-reminder>"},
			{Type: "text", Text: "<system-reminder>\nThe following skills are available…\n</system-reminder>"},
			{Type: "text", Text: "<system-reminder>\nAs you answer the user's questions…\n</system-reminder>"},
			{Type: "text", Text: prose},
		}), nil)

		Expect(named.Hash).To(Equal(drifted.Hash))
	})

	It("hashes the same when a stray blank line drifts inside the prose", func() {
		original := "Para 1\nPara 2\nPara 3"
		drifted := "Para 1\nPara 2\n\nPara 3"

		a := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: original}}), nil)
		b := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: drifted}}), nil)

		Expect(a.Hash).To(Equal(b.Hash))
	})

	It("still produces a different hash when real user content changes", func() {
		a := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: "Hello world"}}), nil)
		b := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: "Hello there"}}), nil)

		Expect(a.Hash).NotTo(Equal(b.Hash))
	})

	It("still distinguishes nodes whose parents differ", func() {
		root := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: "root"}}), nil)
		other := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: "other"}}), nil)

		child1 := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: "same"}}), root)
		child2 := merkle.NewNode(userBucket([]llm.ContentBlock{{Type: "text", Text: "same"}}), other)

		Expect(child1.Hash).NotTo(Equal(child2.Hash))
	})

	It("hashes the same when the routing model changes (Claude Code haiku→opus, PCC-562)", func() {
		// Same observed in the live trace: the named root captured with
		// model=claude-haiku-4-5-* must dedup against the same content
		// captured later with model=claude-opus-4-7. Provider and
		// AgentName are also routing-only.
		content := []llm.ContentBlock{{Type: "text", Text: "Hey Claude!"}}

		haiku := merkle.NewNode(merkle.Bucket{
			Type: "message", Role: "user", Content: content,
			Model: "claude-haiku-4-5-20251001", Provider: "anthropic", AgentName: "claude",
		}, nil)
		opus := merkle.NewNode(merkle.Bucket{
			Type: "message", Role: "user", Content: content,
			Model: "claude-opus-4-7", Provider: "anthropic", AgentName: "claude",
		}, nil)
		acrossProviders := merkle.NewNode(merkle.Bucket{
			Type: "message", Role: "user", Content: content,
			Model: "gpt-5", Provider: "openai", AgentName: "codex",
		}, nil)

		Expect(haiku.Hash).To(Equal(opus.Hash))
		Expect(haiku.Hash).To(Equal(acrossProviders.Hash))
	})

	It("hashes role independently — user 'Hi' and assistant 'Hi' must not collide", func() {
		content := []llm.ContentBlock{{Type: "text", Text: "Hi"}}
		user := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: content}, nil)
		assistant := merkle.NewNode(merkle.Bucket{Type: "message", Role: "assistant", Content: content}, nil)

		Expect(user.Hash).NotTo(Equal(assistant.Hash))
	})
})

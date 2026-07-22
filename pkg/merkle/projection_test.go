package merkle_test

import (
	"strings"

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
		// Iterate merkle.HarnessTags directly so this stays a canonical
		// "every catalogued tag gets stripped" guard: new entries (the
		// Phase-2 wrappers, environment_context, anything added later) are
		// covered automatically instead of drifting from a hardcoded subset.
		var body strings.Builder
		for _, t := range merkle.HarnessTags {
			body.WriteString("<" + t + ">drop-" + t + "</" + t + ">\n")
		}
		body.WriteString("keep")

		projected := merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: body.String()}})

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].Text).To(Equal("keep"))
	})

	It("strips Codex's <environment_context> wrapper with its nested volatile children", func() {
		// A Codex session prepends an environment-framing block whose
		// values (current_date, timezone, cwd) drift between turns. The
		// outer strip must swallow the whole nested block so none of it
		// reaches the projection.
		env := strings.Join([]string{
			"<environment_context>",
			"  <cwd>/home/fedora/git/paper-forest/groves</cwd>",
			"  <shell>zsh</shell>",
			"  <current_date>2026-07-22</current_date>",
			"  <timezone>UTC</timezone>",
			"  <filesystem><workspace_roots><root>/home/x</root></workspace_roots>" +
				"<permission_profile type=\"managed\">full</permission_profile></filesystem>",
			"</environment_context>",
			"",
			"Hey Codex!",
		}, "\n")

		projected := merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: env}})

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].Text).To(Equal("Hey Codex!"))
	})

	It("hashes two Codex turns identically when only the environment_context values drift", func() {
		day1 := "<environment_context><current_date>2026-07-22</current_date>" +
			"<timezone>UTC</timezone></environment_context>\n\nsame ask"
		day2 := "<environment_context><current_date>2026-07-23</current_date>" +
			"<timezone>PST</timezone></environment_context>\n\nsame ask"

		Expect(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: day1}})).
			To(Equal(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: day2}})))
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

	It("intentionally hashes two prose turns identically when they differ only by a blank-line run", func() {
		// PCC-562 accepted-tradeoff guard. The harness occasionally
		// re-serializes a turn and inserts or removes a blank line
		// (see the "57a58 >" case in the ticket evidence). To survive
		// that drift we collapse runs of newlines, which means a
		// future reader of two captures whose ONLY difference is a
		// blank-line count will see them dedup. This is intended.
		// A real user authoring two genuinely-different turns that
		// differ only by whitespace is implausible enough that we
		// accept the tradeoff. If this assertion ever needs to flip,
		// PCC-562's guarantees go with it — tighten the regex
		// somewhere else, not here.
		paragraphed := "Explain A\n\nExplain B"
		flattened := "Explain A\nExplain B"

		Expect(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: paragraphed}})).
			To(Equal(merkle.ProjectContent([]llm.ContentBlock{{Type: "text", Text: flattened}})))
	})

	It("preserves a tool_use block when nothing in its input is a zero value", func() {
		blocks := []llm.ContentBlock{
			{Type: "tool_use", ToolName: "Bash", ToolUseID: "abc", ToolInput: map[string]any{"cmd": "ls"}},
			{Type: "image", ImageURL: "https://example.com/x.png", MediaType: "image/png"},
		}

		projected := merkle.ProjectContent(blocks)

		Expect(projected).To(Equal(blocks))
	})

	It("drops zero-valued keys from tool_use.ToolInput (Edit replace_all=false)", func() {
		// Mirrors the live capture pair (7067d494 vs b8d68493): same
		// tool_use_id, same file_path / new_string / old_string, the
		// streamed assistant turn includes "replace_all": false while
		// the re-sent history omits it.
		streamed := []llm.ContentBlock{{
			Type:      "tool_use",
			ToolUseID: "toolu_01abc",
			ToolName:  "Edit",
			ToolInput: map[string]any{
				"file_path":   "/tmp/x.md",
				"new_string":  "next",
				"old_string":  "prev",
				"replace_all": false,
			},
		}}
		resent := []llm.ContentBlock{{
			Type:      "tool_use",
			ToolUseID: "toolu_01abc",
			ToolName:  "Edit",
			ToolInput: map[string]any{
				"file_path":  "/tmp/x.md",
				"new_string": "next",
				"old_string": "prev",
			},
		}}

		Expect(merkle.ProjectContent(streamed)).To(Equal(merkle.ProjectContent(resent)))
	})

	It("keeps tool_input values that are not the zero value", func() {
		blocks := []llm.ContentBlock{{
			Type:      "tool_use",
			ToolUseID: "toolu_keep",
			ToolName:  "Edit",
			ToolInput: map[string]any{
				"file_path":   "/tmp/y.md",
				"replace_all": true,
				"count":       int64(3),
				"tags":        []any{"a", "b"},
				"nested":      map[string]any{"on": true, "off": false},
			},
		}}

		projected := merkle.ProjectContent(blocks)

		Expect(projected).To(HaveLen(1))
		Expect(projected[0].ToolInput).To(Equal(map[string]any{
			"file_path":   "/tmp/y.md",
			"replace_all": true,
			"count":       int64(3),
			"tags":        []any{"a", "b"},
			"nested":      map[string]any{"on": true},
		}))
	})

	It("drops the thinking signature so streamed and re-sent thinking blocks dedup", func() {
		streamed := []llm.ContentBlock{{
			Type:              "thinking",
			Thinking:          "let me check…",
			ThinkingSignature: "EosCCmMIDRgC…",
		}}
		resent := []llm.ContentBlock{{
			Type:     "thinking",
			Thinking: "let me check…",
		}}

		Expect(merkle.ProjectContent(streamed)).To(Equal(merkle.ProjectContent(resent)))
	})

	It("preserves an image block verbatim", func() {
		blocks := []llm.ContentBlock{{Type: "image", ImageURL: "https://example.com/x.png", MediaType: "image/png"}}

		Expect(merkle.ProjectContent(blocks)).To(Equal(blocks))
	})

	It("does not mutate the input slice or its blocks", func() {
		input := []llm.ContentBlock{
			{Type: "text", Text: "<system-reminder>drop</system-reminder>keep"},
			{
				Type:              "thinking",
				Thinking:          "x",
				ThinkingSignature: "sig",
			},
			{
				Type:      "tool_use",
				ToolName:  "Edit",
				ToolInput: map[string]any{"replace_all": false, "p": "/x"},
			},
		}
		_ = merkle.ProjectContent(input)

		Expect(input[0].Text).To(Equal("<system-reminder>drop</system-reminder>keep"))
		Expect(input[1].ThinkingSignature).To(Equal("sig"))
		Expect(input[2].ToolInput).To(HaveKey("replace_all"))
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

	It("strips harness tags from tool_result output, not just text", func() {
		// The harness concatenates volatile blocks INTO a tool's output
		// when re-sending it as history: the live capture and the
		// re-sent history of the same result must hash together.
		live := []llm.ContentBlock{{
			Type: "tool_result", ToolResultID: "toolu_1",
			ToolOutput: "total 8\ndrwxr-xr-x  2 u  staff",
		}}
		resent := []llm.ContentBlock{{
			Type: "tool_result", ToolResultID: "toolu_1",
			ToolOutput: "total 8\ndrwxr-xr-x  2 u  staff\n<system-reminder>\nclock ticked, skills changed\n</system-reminder>",
		}}
		a := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: live}, nil)
		b := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: resent}, nil)
		Expect(a.Hash).To(Equal(b.Hash))
	})

	It("projects volatile wrapper tags so their drift cannot fork the chain", func() {
		v1 := []llm.ContentBlock{{Type: "text", Text: "judge this\n<new-diagnostics>\nfile.go:1 unused var\n</new-diagnostics>"}}
		v2 := []llm.ContentBlock{{Type: "text", Text: "judge this\n<new-diagnostics>\nfile.go:9 other finding\n</new-diagnostics>"}}
		a := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: v1}, nil)
		b := merkle.NewNode(merkle.Bucket{Type: "message", Role: "user", Content: v2}, nil)
		Expect(a.Hash).To(Equal(b.Hash))
	})
})

var _ = Describe("PreviewText", func() {
	It("strips volatile harness spans whole", func() {
		Expect(merkle.PreviewText(
			"<system-reminder># claudeMd boilerplate</system-reminder>\nActual human question")).
			To(Equal("Actual human question"))
	})

	It("unwraps content-bearing wrappers, keeping the human's words", func() {
		Expect(merkle.PreviewText(
			"<command-name>/goal</command-name><command-args>Ship it tonight.</command-args>")).
			To(Equal("Ship it tonight."))
		Expect(merkle.PreviewText("<session>Resume the migration.</session>")).
			To(Equal("Resume the migration."))
	})

	It("strips noise nested inside an unwrapped wrapper", func() {
		Expect(merkle.PreviewText(
			"<session>Opener<task-notification>bg event</task-notification> continues</session>")).
			To(Equal("Opener continues"))
	})

	It("returns empty when a turn is nothing but volatile scaffolding", func() {
		Expect(merkle.PreviewText(
			"<system-reminder>x</system-reminder><new-diagnostics>y</new-diagnostics>")).
			To(BeEmpty())
	})

	It("keeps the inner text of an unterminated wrapper open tag", func() {
		Expect(merkle.PreviewText("<command-args>truncated goal text")).
			To(Equal("truncated goal text"))
	})
})

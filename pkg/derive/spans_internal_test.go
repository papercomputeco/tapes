package derive

import (
	"strings"
	"time"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

var _ = Describe("joinTextBlocks truncation", func() {
	It("truncates on a rune boundary so previews stay valid UTF-8", func() {
		// 279 ASCII bytes followed by a 3-byte rune: a byte-indexed cut
		// at 280 would split the rune and Postgres would reject the
		// preview (SQLSTATE 22021).
		text := strings.Repeat("a", maxPreviewText-1) + "✓✓✓"
		blocks := []llm.ContentBlock{{Type: blockText, Text: text}}

		got := joinTextBlocks(blocks, false)
		Expect(len(got)).To(BeNumerically("<=", maxPreviewText))
		Expect(utf8.ValidString(got)).To(BeTrue())
		Expect(got).To(Equal(strings.Repeat("a", maxPreviewText-1)))
	})

	It("keeps short previews untouched", func() {
		blocks := []llm.ContentBlock{{Type: blockText, Text: "hello ✓"}}
		Expect(joinTextBlocks(blocks, false)).To(Equal("hello ✓"))
	})
})

var _ = Describe("outcomes fold", func() {
	key := SessionKey{HarnessID: "claude-code", HarnessSessionID: "sess-1"}

	// A minimal turn: root agent span plus one completed Bash tool span
	// whose output printed the created PR's URL.
	makeTurn := func(command, output string) *SpanTurn {
		root := &Span{SpanID: "agent_1", Kind: SpanKindAgent, StartedAt: time.Unix(100, 0)}
		tool := &Span{
			SpanID:    "tool_1",
			Kind:      SpanKindTool,
			Name:      "Bash",
			StartedAt: time.Unix(101, 0),
			Input: []llm.ContentBlock{{
				Type:      blockToolUse,
				ToolName:  "Bash",
				ToolInput: map[string]any{"command": command},
			}},
			Output: []llm.ContentBlock{{
				Type:       blockToolResult,
				ToolOutput: output,
			}},
		}
		return &SpanTurn{
			TraceID:   "trc_test1",
			Session:   key,
			StartedAt: time.Unix(100, 0),
			Spans:     []*Span{root, tool},
		}
	}

	It("stamps provenance and folds detected outcomes per session", func() {
		em := &spanEmitter{set: &SpanSet{Report: SpanReport{
			SpanKinds: map[string]int{},
			CallKinds: map[string]int{},
			LinkKinds: map[string]int{},
		}}}
		em.set.Turns = []*SpanTurn{makeTurn(
			"gh pr create --fill",
			"https://github.com/papercomputeco/paper/pull/94\n",
		)}

		em.finish()

		outcomes := em.set.Outcomes[key]
		Expect(outcomes).To(HaveLen(1))
		Expect(outcomes[0].Kind).To(Equal(sessions.OutcomeKindPullRequest))
		Expect(outcomes[0].URL).To(Equal("https://github.com/papercomputeco/paper/pull/94"))
		Expect(outcomes[0].TraceID).To(Equal("trc_test1"))
		Expect(outcomes[0].SpanID).To(Equal("tool_1"))
		// The detecting span's start time, never the wall clock, so a
		// re-derive reproduces the fold.
		Expect(outcomes[0].DetectedAt).To(Equal(time.Unix(101, 0)))
	})

	It("dedupes repeat detections of the same URL across turns", func() {
		em := &spanEmitter{set: &SpanSet{Report: SpanReport{
			SpanKinds: map[string]int{},
			CallKinds: map[string]int{},
			LinkKinds: map[string]int{},
		}}}
		first := makeTurn("gh pr create --fill", "https://github.com/o/r/pull/1")
		repeat := makeTurn("gh pr create --fill", "https://github.com/o/r/pull/1")
		repeat.TraceID = "trc_test2"
		repeat.Spans[1].SpanID = "tool_2"
		em.set.Turns = []*SpanTurn{first, repeat}

		em.finish()

		outcomes := em.set.Outcomes[key]
		Expect(outcomes).To(HaveLen(1))
		Expect(outcomes[0].SpanID).To(Equal("tool_1"))
	})

	It("emits no fold at all when nothing was detected", func() {
		em := &spanEmitter{set: &SpanSet{Report: SpanReport{
			SpanKinds: map[string]int{},
			CallKinds: map[string]int{},
			LinkKinds: map[string]int{},
		}}}
		em.set.Turns = []*SpanTurn{makeTurn("ls -la", "total 0")}

		em.finish()

		Expect(em.set.Outcomes).To(BeNil())
	})
})

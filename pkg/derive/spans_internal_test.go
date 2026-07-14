package derive

import (
	"strings"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

var _ = Describe("preview truncation", func() {
	It("truncates on a rune boundary so previews stay valid UTF-8", func() {
		// 279 ASCII bytes followed by a 3-byte rune: a byte-indexed cut
		// at 280 would split the rune and Postgres would reject the
		// preview (SQLSTATE 22021).
		text := strings.Repeat("a", maxPreviewText-1) + "✓✓✓"
		blocks := []llm.ContentBlock{{Type: blockText, Text: text}}

		got := joinTextBlocks(blocks)
		Expect(len(got)).To(BeNumerically("<=", maxPreviewText))
		Expect(utf8.ValidString(got)).To(BeTrue())
		Expect(got).To(Equal(strings.Repeat("a", maxPreviewText-1)))
	})

	It("keeps short previews untouched", func() {
		blocks := []llm.ContentBlock{{Type: blockText, Text: "hello ✓"}}
		Expect(joinTextBlocks(blocks)).To(Equal("hello ✓"))
	})
})

var _ = Describe("promptText harness-scaffolding fold", func() {
	node := func(texts ...string) *merkle.Node {
		blocks := make([]llm.ContentBlock, len(texts))
		for i, t := range texts {
			blocks[i] = llm.ContentBlock{Type: blockText, Text: t}
		}
		return &merkle.Node{Bucket: merkle.Bucket{Content: blocks}}
	}

	It("previews the human's slash-command args when every block is harness-tagged", func() {
		// The live regression shape (019f4f79 / 019f4e6b): a turn whose
		// blocks are ALL harness scaffolding. The previous fold skipped
		// every block, then rejoined the RAW text as a fallback and
		// previewed the <system-reminder> claudeMd boilerplate. The fix
		// unwraps the content-bearing wrapper (<command-args>) and drops
		// the boilerplate.
		got := promptText(node(
			"<system-reminder>\n# claudeMd\nProject instructions the harness injects every turn.\n</system-reminder>",
			"<command-name>/goal</command-name>\n<command-args>Ship the span-model cleanup tonight.</command-args>",
		))
		Expect(got).To(Equal("Ship the span-model cleanup tonight."))
		Expect(got).NotTo(ContainSubstring("claudeMd"))
		Expect(got).NotTo(ContainSubstring("system-reminder"))
	})

	It("keeps a genuine trailing block that follows harness scaffolding in the same block", func() {
		// A single block that LEADS with a harness tag but carries the
		// human's real words after it. The previous prefix-based,
		// whole-block skip dropped the human text with the scaffolding.
		got := promptText(node(
			"<system-reminder>volatile injected context</system-reminder>\nWhat changed in the deriver since yesterday?",
		))
		Expect(got).To(Equal("What changed in the deriver since yesterday?"))
	})

	It("does not drop a genuine block that follows a harness-only block", func() {
		got := promptText(node(
			"<system-reminder>boilerplate</system-reminder>",
			"Please summarize the open PRs.",
		))
		Expect(got).To(Equal("Please summarize the open PRs."))
	})

	It("leaves an ordinary human prompt untouched", func() {
		Expect(promptText(node("How does the derive worker debounce ingest?"))).
			To(Equal("How does the derive worker debounce ingest?"))
	})
})

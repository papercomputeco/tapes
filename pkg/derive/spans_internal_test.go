package derive

import (
	"strings"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
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

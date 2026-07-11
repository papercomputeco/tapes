package derive_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
)

func blocks(texts ...string) []llm.ContentBlock {
	out := make([]llm.ContentBlock, 0, len(texts))
	for _, t := range texts {
		out = append(out, llm.ContentBlock{Type: "text", Text: t})
	}
	return out
}

var _ = Describe("ClassifyVerdict", func() {
	It("returns nil for spans that are not permission checks", func() {
		Expect(derive.ClassifyVerdict("main", blocks("<block>yes"))).To(BeNil())
		Expect(derive.ClassifyVerdict("offshoot:title-gen", blocks("<block>yes"))).To(BeNil())
	})

	It("returns nil for a permission check with no <block> marker", func() {
		Expect(derive.ClassifyVerdict(derive.KindCheckStage1, blocks("no decision here"))).To(BeNil())
	})

	It("reads BLOCK from <block>yes and ALLOW from <block>no", func() {
		block := derive.ClassifyVerdict(derive.KindCheckStage1, blocks("<block>yes"))
		Expect(block).NotTo(BeNil())
		Expect(block.Disposition).To(Equal("BLOCK"))
		Expect(block.Stage).To(Equal(1))
		Expect(block.Reasoned).To(BeFalse())

		allow := derive.ClassifyVerdict(derive.KindCheckStage1, blocks("<block> no"))
		Expect(allow.Disposition).To(Equal("ALLOW"))
	})

	It("marks stage 2 and reasoned from the stage2 kind and <thinking>", func() {
		v := derive.ClassifyVerdict(derive.KindCheckStage2, blocks("<thinking>weighing it</thinking>\n<block>no"))
		Expect(v.Stage).To(Equal(2))
		Expect(v.Reasoned).To(BeTrue())
		Expect(v.Disposition).To(Equal("ALLOW"))
	})

	It("concatenates multiple text blocks before matching", func() {
		v := derive.ClassifyVerdict(derive.KindCheckStage1, blocks("preamble\n", "<block>yes"))
		Expect(v).NotTo(BeNil())
		Expect(v.Disposition).To(Equal("BLOCK"))
	})
})

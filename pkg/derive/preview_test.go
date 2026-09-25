package derive_test

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("PreviewBlocks", func() {
	// long is deliberately multi-byte: the bound is counted in runes, so
	// a byte-counting regression would cut it short.
	long := strings.Repeat("é", derive.PreviewRunes+100)

	decode := func(raw json.RawMessage) []llm.ContentBlock {
		var blocks []llm.ContentBlock
		Expect(json.Unmarshal(raw, &blocks)).To(Succeed())
		return blocks
	}

	It("pins an empty or null payload to an empty array", func() {
		Expect(string(derive.PreviewBlocks(nil))).To(Equal("[]"))
		Expect(string(derive.PreviewBlocks(json.RawMessage("null")))).To(Equal("[]"))
	})

	It("passes a blob that is not a block array through whole", func() {
		Expect(string(derive.PreviewBlocks(json.RawMessage(`{not json`)))).To(Equal(`{not json`))
		Expect(string(derive.PreviewBlocks(json.RawMessage(`{"a": 1}`)))).To(Equal(`{"a":1}`))
	})

	It("truncates text, thinking, and tool output to the rune bound", func() {
		raw, err := json.Marshal([]llm.ContentBlock{
			{Type: "text", Text: long},
			{Type: "thinking", Thinking: long, ThinkingSignature: "sig"},
			{Type: "tool_result", ToolResultID: "tu_1", ToolOutput: long},
		})
		Expect(err).NotTo(HaveOccurred())

		blocks := decode(derive.PreviewBlocks(raw))
		Expect(blocks).To(HaveLen(3))
		for _, s := range []string{blocks[0].Text, blocks[1].Thinking, blocks[2].ToolOutput} {
			Expect(utf8.RuneCountInString(s)).To(Equal(derive.PreviewRunes + 1))
			Expect(s).To(HaveSuffix("…"))
			Expect(s).To(HavePrefix(strings.Repeat("é", derive.PreviewRunes)))
		}
		Expect(blocks[1].ThinkingSignature).To(Equal("sig"))
		Expect(blocks[2].ToolResultID).To(Equal("tu_1"))
	})

	It("leaves a string at the bound untouched", func() {
		exact := strings.Repeat("x", derive.PreviewRunes)
		raw, err := json.Marshal([]llm.ContentBlock{{Type: "text", Text: exact}})
		Expect(err).NotTo(HaveOccurred())
		Expect(decode(derive.PreviewBlocks(raw))[0].Text).To(Equal(exact))
	})

	It("bounds a server tool result's inline content", func() {
		// web_search_tool_result and its kin carry the provider's result
		// verbatim in `content`; a preview keeps its shape but not its size.
		long := strings.Repeat("r", derive.PreviewRunes*3)
		raw := json.RawMessage(`[{"type":"web_search_tool_result","tool_result_id":"srvtoolu_1","content":[{"type":"web_search_result","title":"t","encrypted_content":"` + long + `"}]}]`)
		blocks := decode(derive.PreviewBlocks(raw))
		Expect(blocks).To(HaveLen(1))
		var content []map[string]any
		Expect(json.Unmarshal(blocks[0].Content, &content)).To(Succeed())
		Expect(content).To(HaveLen(1))
		Expect(content[0]["title"]).To(Equal("t"))
		got, _ := content[0]["encrypted_content"].(string)
		Expect(utf8.RuneCountInString(got)).To(Equal(derive.PreviewRunes + 1))
		Expect(strings.HasSuffix(got, "…")).To(BeTrue())
	})

	It("strips image bytes", func() {
		raw, err := json.Marshal([]llm.ContentBlock{
			{Type: "text", Text: "look at this"},
			{Type: "image", MediaType: "image/png", ImageBase64: strings.Repeat("A", 4096)},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).To(ContainSubstring("image_base64"))

		out := derive.PreviewBlocks(raw)
		Expect(string(out)).NotTo(ContainSubstring("image_base64"))
		Expect(string(out)).NotTo(ContainSubstring("AAAA"))

		blocks := decode(out)
		Expect(blocks).To(HaveLen(2))
		Expect(blocks[1].Type).To(Equal("image"))
		Expect(blocks[1].MediaType).To(Equal("image/png"), "the block survives; only its bytes go")
		Expect(blocks[1].ImageBase64).To(BeEmpty())
	})

	It("truncates nested tool arguments", func() {
		raw, err := json.Marshal([]llm.ContentBlock{{
			Type: "tool_use", ToolUseID: "tu_1", ToolName: "Edit",
			ToolInput: map[string]any{
				"file_path":  "/tmp/x",
				"old_string": long,
				"nested": map[string]any{
					"deeper": []any{long, 7, true, map[string]any{"leaf": long}},
				},
			},
		}})
		Expect(err).NotTo(HaveOccurred())

		blocks := decode(derive.PreviewBlocks(raw))
		Expect(blocks).To(HaveLen(1))
		in := blocks[0].ToolInput
		Expect(in["file_path"]).To(Equal("/tmp/x"))
		Expect(utf8.RuneCountInString(in["old_string"].(string))).To(Equal(derive.PreviewRunes + 1))

		deeper := in["nested"].(map[string]any)["deeper"].([]any)
		Expect(deeper).To(HaveLen(4), "structure is preserved")
		Expect(utf8.RuneCountInString(deeper[0].(string))).To(Equal(derive.PreviewRunes + 1))
		Expect(deeper[1]).To(BeEquivalentTo(7))
		Expect(deeper[2]).To(Equal(true))
		leaf := deeper[3].(map[string]any)["leaf"].(string)
		Expect(utf8.RuneCountInString(leaf)).To(Equal(derive.PreviewRunes + 1))
		Expect(leaf).To(HaveSuffix("…"))
	})
})

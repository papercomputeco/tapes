package llm_test

import (
	"encoding/json"
	"strings"
	"unsafe"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// aliases reports whether two strings share a backing array — the
// condition that keeps a parsed sub-slice pinning the whole raw buffer.
// A clone must produce equal bytes that do NOT alias.
func aliases(a, b string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return unsafe.StringData(a) == unsafe.StringData(b)
}

// bytesAlias reports whether two byte slices share a backing array.
func bytesAlias(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return unsafe.SliceData(a) == unsafe.SliceData(b)
}

var _ = Describe("ContentBlock.Clone", func() {
	// buf stands in for a raw request buffer; every field below is a
	// sub-slice of it, exactly as zero-copy provider parsing produces.
	var buf string
	var block llm.ContentBlock

	BeforeEach(func() {
		buf = "TYPEtool_use" + "TEXThello world" + "IDtu_123" + "NAMEBash" +
			"OUTresult-bytes" + "THINKdeep" + "SIGsig-bytes" + "RAW{\"k\":\"v\"}"
		sub := func(marker, val string) string {
			i := strings.Index(buf, marker) + len(marker)
			return buf[i : i+len(val)]
		}
		block = llm.ContentBlock{
			Type:              sub("TYPE", "tool_use"),
			Text:              sub("TEXT", "hello world"),
			ToolUseID:         sub("ID", "tu_123"),
			ToolName:          sub("NAME", "Bash"),
			ToolOutput:        sub("OUT", "result-bytes"),
			Thinking:          sub("THINK", "deep"),
			ThinkingSignature: sub("SIG", "sig-bytes"),
			Content:           json.RawMessage(sub("RAW", `{"k":"v"}`)),
			ToolInput: map[string]any{
				sub("NAME", "Bash"): sub("TEXT", "hello world"),
				"nested":            map[string]any{"deep": sub("THINK", "deep")},
				"list":              []any{sub("ID", "tu_123"), float64(7)},
				"num":               json.Number("42"),
			},
		}
	})

	It("preserves every byte", func() {
		c := block.Clone()
		Expect(c.Type).To(Equal(block.Type))
		Expect(c.Text).To(Equal(block.Text))
		Expect(c.ToolUseID).To(Equal(block.ToolUseID))
		Expect(c.ToolName).To(Equal(block.ToolName))
		Expect(c.ToolOutput).To(Equal(block.ToolOutput))
		Expect(c.Thinking).To(Equal(block.Thinking))
		Expect(c.ThinkingSignature).To(Equal(block.ThinkingSignature))
		Expect([]byte(c.Content)).To(Equal([]byte(block.Content)))
		Expect(c.ToolInput).To(Equal(block.ToolInput))
	})

	It("breaks every alias to the source buffer", func() {
		c := block.Clone()
		Expect(aliases(c.Type, buf)).To(BeFalse())
		Expect(aliases(c.Text, buf)).To(BeFalse())
		Expect(aliases(c.ToolUseID, buf)).To(BeFalse())
		Expect(aliases(c.ToolName, buf)).To(BeFalse())
		Expect(aliases(c.ToolOutput, buf)).To(BeFalse())
		Expect(aliases(c.Thinking, buf)).To(BeFalse())
		Expect(aliases(c.ThinkingSignature, buf)).To(BeFalse())
		// json.RawMessage is a []byte sub-slice of the same buffer.
		Expect(bytesAlias(c.Content, block.Content)).To(BeFalse())
		// ToolInput map values (and keys) must be reallocated too.
		Expect(aliases(c.ToolInput["nested"].(map[string]any)["deep"].(string), buf)).To(BeFalse())
		Expect(aliases(c.ToolInput["list"].([]any)[0].(string), buf)).To(BeFalse())
	})

	It("rebuilds the ToolInput map (mutating the clone leaves the original intact)", func() {
		c := block.Clone()
		c.ToolInput["new"] = "x"
		Expect(block.ToolInput).NotTo(HaveKey("new"))
		c.ToolInput["nested"].(map[string]any)["deep"] = "changed"
		Expect(block.ToolInput["nested"].(map[string]any)["deep"]).NotTo(Equal("changed"))
	})

	It("leaves nil/empty fields nil/empty", func() {
		c := llm.ContentBlock{Type: "text"}.Clone()
		Expect(c.Content).To(BeNil())
		Expect(c.ToolInput).To(BeNil())
		Expect(c.Text).To(BeEmpty())
	})
})

var _ = Describe("RequestParams.Clone", func() {
	It("clones System and re-points every scalar to fresh storage", func() {
		buf := "SYSyou are helpful"
		mx, temp, stream, tools := 4096, 0.7, true, 12
		rp := &llm.RequestParams{
			System: buf[3:], MaxTokens: &mx, Temperature: &temp, Stream: &stream, ToolCount: &tools,
		}
		c := rp.Clone()
		Expect(c.System).To(Equal(rp.System))
		Expect(aliases(c.System, buf)).To(BeFalse())

		// Values preserved...
		Expect(*c.MaxTokens).To(Equal(*rp.MaxTokens))
		Expect(*c.Temperature).To(Equal(*rp.Temperature))
		Expect(*c.Stream).To(Equal(*rp.Stream))
		Expect(*c.ToolCount).To(Equal(*rp.ToolCount))

		// ...but every pointer addresses NEW storage. A parsed scalar can
		// live inside the arena that backs the raw request buffer, so a
		// shared pointer would pin the whole buffer — the clone must not.
		Expect(c.MaxTokens).NotTo(BeIdenticalTo(rp.MaxTokens))
		Expect(c.Temperature).NotTo(BeIdenticalTo(rp.Temperature))
		Expect(c.Stream).NotTo(BeIdenticalTo(rp.Stream))
		Expect(c.ToolCount).NotTo(BeIdenticalTo(rp.ToolCount))

		// Mutating the clone never touches the original.
		*c.MaxTokens = 1
		Expect(*rp.MaxTokens).To(Equal(4096))
	})

	It("preserves nil scalar pointers", func() {
		rp := &llm.RequestParams{System: "x"}
		c := rp.Clone()
		Expect(c.MaxTokens).To(BeNil())
		Expect(c.Temperature).To(BeNil())
		Expect(c.Stream).To(BeNil())
		Expect(c.ToolCount).To(BeNil())
	})

	It("returns nil for a nil receiver", func() {
		var rp *llm.RequestParams
		Expect(rp.Clone()).To(BeNil())
	})
})

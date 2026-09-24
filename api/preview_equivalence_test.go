package api

// The stored preview (derive.PreviewBlocks, written by the deriver) must be
// byte-identical to the preview the API computes on the way out
// (contentArray in preview mode): once reads switch to the stored column,
// nothing on the wire may change.

import (
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("stored preview equivalence", func() {
	long := strings.Repeat("ü", previewPayloadRunes*3)

	mustBlocks := func(blocks []llm.ContentBlock) json.RawMessage {
		raw, err := json.Marshal(blocks)
		Expect(err).NotTo(HaveOccurred())
		return raw
	}

	corpus := func() map[string]json.RawMessage {
		return map[string]json.RawMessage{
			"empty":     nil,
			"null":      json.RawMessage("null"),
			"null pad":  json.RawMessage(" null "),
			"[]":        json.RawMessage("[]"),
			"malformed": json.RawMessage(`[{"type":"text","text":`),
			"not array": json.RawMessage(`{"type": "text", "text": "<b>&</b>"}`),
			"scalar":    json.RawMessage(`"just a string"`),
			"plain text": mustBlocks([]llm.ContentBlock{
				{Type: "text", Text: "short & <sweet>"},
			}),
			"long text": mustBlocks([]llm.ContentBlock{
				{Type: "text", Text: long},
			}),
			"thinking": mustBlocks([]llm.ContentBlock{
				{Type: "thinking", Thinking: long, ThinkingSignature: "sig-1"},
			}),
			"tool_use nested": mustBlocks([]llm.ContentBlock{{
				Type: "tool_use", ToolUseID: "tu_1", ToolName: "Bash",
				ToolInput: map[string]any{
					"command": long,
					"env":     map[string]any{"PATH": long, "n": 3.5, "ok": false, "nil": nil},
					"args":    []any{"a", long, []any{long}},
				},
			}}),
			"tool_result long": mustBlocks([]llm.ContentBlock{
				{Type: "tool_result", ToolResultID: "tu_1", ToolOutput: long, IsError: true},
			}),
			"image": mustBlocks([]llm.ContentBlock{
				{Type: "image", MediaType: "image/png", ImageBase64: strings.Repeat("Zm9v", 2048)},
			}),
			"mixed": mustBlocks([]llm.ContentBlock{
				{Type: "text", Text: long},
				{Type: "image", ImageURL: "https://example.test/i.png", ImageBase64: "AAAA"},
				{Type: "tool_use", ToolUseID: "tu_2", ToolName: "Read", ToolInput: map[string]any{"file_path": "/x"}},
			}),
			"pretty printed": json.RawMessage("[\n  {\n    \"type\": \"text\",\n    \"text\": \"" + long + "\"\n  }\n]"),
		}
	}

	It("preview bytes unchanged", func() {
		for name, raw := range corpus() {
			Expect(string(derive.PreviewBlocks(raw))).To(Equal(string(contentArray(raw, PayloadPreview))),
				"corpus entry %q", name)
		}
	})

	It("shares the rune bound with the API", func() {
		Expect(derive.PreviewRunes).To(Equal(previewPayloadRunes))
	})
})

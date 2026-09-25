package derive

import (
	"encoding/json"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// PreviewRunes bounds every string carried by a stored span preview.
// Long enough to read, short enough that a whole session of previews
// stays smaller than one full tool result.
const PreviewRunes = 512

// PreviewBlocks renders the bounded preview of a stored content-block
// array. It is the derive-time projection the API used to compute on
// every preview read, moved to the writer so a preview read never has to
// detoast the payload it summarizes.
//
// The output is pinned to `[]` for an empty or JSON-null payload. A blob
// that does not decode as content blocks passes through whole rather
// than silently vanishing: re-encoded (compacted) when it is valid JSON,
// verbatim when it is not. Otherwise every text, thinking, and tool
// output string is truncated to PreviewRunes with an ellipsis, image
// bytes are dropped, and tool input is truncated recursively so nested
// arguments keep their structure.
func PreviewBlocks(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("[]")
	}
	var blocks []llm.ContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil || blocks == nil {
		out, err := json.Marshal(raw)
		if err != nil {
			return raw
		}
		return out
	}
	for i := range blocks {
		b := &blocks[i]
		b.Text = previewString(b.Text)
		b.Thinking = previewString(b.Thinking)
		b.ToolOutput = previewString(b.ToolOutput)
		// Previews never carry image bytes.
		b.ImageBase64 = ""
		if b.ToolInput != nil {
			b.ToolInput = previewValue(b.ToolInput).(map[string]any)
		}
		b.Content = previewRaw(b.Content)
	}
	out, err := json.Marshal(blocks)
	if err != nil {
		return raw
	}
	return out
}

// previewRaw bounds a server-tool result's inline content, the one block
// field captured verbatim as JSON: every string inside it is truncated
// with structure kept, and a blob that is not JSON is truncated as text.
func previewRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		out, err := json.Marshal(previewString(string(raw)))
		if err != nil {
			return raw
		}
		return out
	}
	out, err := json.Marshal(previewValue(v))
	if err != nil {
		return raw
	}
	return out
}

// previewString truncates one payload string to the preview bound.
func previewString(s string) string {
	r := []rune(s)
	if len(r) <= PreviewRunes {
		return s
	}
	return string(r[:PreviewRunes]) + "…"
}

// previewValue truncates every string reachable in a decoded JSON
// value, preserving structure (tool arguments nest arbitrarily).
func previewValue(v any) any {
	switch t := v.(type) {
	case string:
		return previewString(t)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = previewValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = previewValue(val)
		}
		return out
	default:
		return v
	}
}

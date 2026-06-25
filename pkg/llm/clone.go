package llm

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Clone returns a deep copy of the content block in which every string
// and byte field is reallocated.
//
// Provider request parsing is zero-copy (jsonv2): the parsed strings are
// sub-slices that alias the raw request buffer. Go frees a backing array
// only when no sub-slice still references it, so a single retained
// ContentBlock would otherwise pin the entire multi-MB request buffer
// alive for the life of the derived node. Cloning breaks every alias, so
// the raw buffer can be collected once its turn is processed.
//
// Cloning is all-or-nothing: any one field left aliasing keeps the whole
// buffer pinned, so EVERY string/byte field must be copied here. The copy
// is byte-for-byte identical to the original — a node hashed from a cloned
// bucket hashes the same.
func (b ContentBlock) Clone() ContentBlock {
	b.Type = strings.Clone(b.Type)
	b.Text = strings.Clone(b.Text)
	b.ImageURL = strings.Clone(b.ImageURL)
	b.ImageBase64 = strings.Clone(b.ImageBase64)
	b.MediaType = strings.Clone(b.MediaType)
	b.ToolUseID = strings.Clone(b.ToolUseID)
	b.ToolName = strings.Clone(b.ToolName)
	b.ToolResultID = strings.Clone(b.ToolResultID)
	b.ToolOutput = strings.Clone(b.ToolOutput)
	b.Thinking = strings.Clone(b.Thinking)
	b.ThinkingSignature = strings.Clone(b.ThinkingSignature)
	if b.Content != nil {
		b.Content = bytes.Clone(b.Content)
	}
	b.ToolInput = cloneAnyMap(b.ToolInput)
	return b
}

// cloneAnyMap deep-copies a decoded JSON object, reallocating every key
// and every string value so none aliases the raw buffer. Nested objects
// and arrays are copied recursively; numbers, bools, and nil are value
// types with no backing buffer to pin.
func cloneAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[strings.Clone(k)] = cloneAnyValue(v)
	}
	return out
}

func cloneAnyValue(v any) any {
	switch t := v.(type) {
	case string:
		return strings.Clone(t)
	case map[string]any:
		return cloneAnyMap(t)
	case []any:
		s := make([]any, len(t))
		for i, e := range t {
			s[i] = cloneAnyValue(e)
		}
		return s
	case json.Number:
		// Defensive: the jsonv2 decoder yields float64 for numbers in a
		// map[string]any, so this fires only if a caller decodes ToolInput
		// with a UseNumber-style option. json.Number is a string under the
		// hood, so it WOULD alias the raw buffer and must be cloned then.
		return json.Number(strings.Clone(string(t)))
	default:
		// float64, bool, nil — value types, nothing to pin.
		return v
	}
}

// Clone returns a copy of the request params that shares no backing
// storage with the raw request buffer it was parsed from. System is
// reallocated, and each scalar pointer (MaxTokens, Temperature, Stream,
// ToolCount) is repointed at a freshly allocated value: a value parsed by
// jsonv2 may be allocated inside the arena that also backs the raw buffer,
// so copying the pointer by value would keep the whole multi-MB buffer
// alive. Returns nil for a nil receiver.
func (r *RequestParams) Clone() *RequestParams {
	if r == nil {
		return nil
	}
	c := *r
	c.System = strings.Clone(c.System)
	c.MaxTokens = clonePtr(r.MaxTokens)
	c.Temperature = clonePtr(r.Temperature)
	c.Stream = clonePtr(r.Stream)
	c.ToolCount = clonePtr(r.ToolCount)
	return &c
}

// clonePtr returns a pointer to a fresh copy of *p (nil for nil), so the
// result addresses standalone storage rather than a slot inside a parse
// arena that may pin the raw request buffer.
func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

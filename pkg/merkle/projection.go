package merkle

import (
	"regexp"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// HarnessTags lists the XML-like tags emitted by agent harnesses (e.g.
// Claude Code) that wrap mutable metadata into user-role content blocks.
// Text wrapped in these tags drifts between turns of the same
// conversation — the clock ticks, a skill loads, an MCP inventory
// changes — yet none of it is part of the user's intent. Letting it
// participate in the content-addressed hash fractures otherwise
// continuous conversations into multiple disconnected roots. See
// PCC-562.
//
// Tags are matched verbatim, so "system-reminder" matches
// <system-reminder>…</system-reminder> but does NOT match
// <local-command-stdout>; each suffixed variant must be listed
// explicitly.
var HarnessTags = []string{
	"system-reminder",
	"command-name",
	"command-message",
	"command-args",
	"local-command-stdout",
	"local-command-stderr",
	"local-command-caveat",
	// Second wave (reconciled conversation tree, Phase 2): wrappers and
	// volatile blocks measured fracturing the golden sessions' chains.
	// <session> wraps the opener in harness side-calls; <conversation>
	// wraps plan re-injections; the rest are volatile per-turn noise
	// (LSP diagnostics, background-task notifications, harness event
	// framing) that drifts between a live capture and the re-sent
	// history of the same turn.
	"session",
	"conversation",
	"new-diagnostics",
	"task-notification",
	"status",
	"summary",
	"transcript",
	"event",
	"tool-use-id",
	"output-file",
	"task-id",
	// Codex's opening environment-framing wrapper. A Codex session
	// prepends a text block wrapping its cwd/shell/current_date/timezone/
	// filesystem framing in <environment_context>…</environment_context>.
	// Those values (clock, timezone, cwd) drift between turns; stripping
	// the outer tag removes the whole nested block so they cannot ride
	// into the chain hash or the embedding render.
	"environment_context",
}

// ProjectContent returns the projection of content blocks used when
// computing a node's chain hash. The projection:
//
//  1. Strips every <tag>…</tag> span whose tag is in HarnessTags from
//     each block's Text field.
//  2. Normalizes whitespace in what remains so blank-line drift inside
//     the surviving prose cannot fork the chain (see PCC-562's
//     "57a58 >" case).
//  3. Drops any block that became empty after stripping.
//  4. For tool_use blocks, drops zero-valued keys from ToolInput. The
//     streamed assistant capture sees every key the model emitted
//     including optional defaults like Edit's "replace_all": false;
//     when that same turn is re-sent as history the harness strips
//     those defaults. Without this step the two captures hash to
//     different nodes and the chain branches.
//  5. For thinking blocks, drops ThinkingSignature. Anthropic emits
//     the signature on the live stream but the harness omits it on
//     re-send; same shape as (4).
//
// The input slice and its blocks are not mutated; the returned slice is
// independent. Bucket.Content itself stays unprojected so display,
// labels, and search continue to see the raw text the model received.
func ProjectContent(blocks []llm.ContentBlock) []llm.ContentBlock {
	out := make([]llm.ContentBlock, 0, len(blocks))
	for _, b := range blocks {
		if b.Text != "" {
			projected := normalizeWhitespace(StripHarnessTags(b.Text))
			if projected == "" {
				continue
			}
			b.Text = projected
		}
		if len(b.ToolInput) > 0 {
			b.ToolInput = pruneZeroValues(b.ToolInput)
		}
		if b.ThinkingSignature != "" {
			b.ThinkingSignature = ""
		}
		// Tool results need the same treatment as text: the harness
		// concatenates volatile blocks (<system-reminder>, …) INTO a
		// tool's output when re-sending it as history, so a live
		// capture and the re-sent history of the same tool result
		// otherwise hash apart and fork the chain. This was the bulk
		// of the measured join-residual on the golden sessions.
		if b.ToolOutput != "" {
			b.ToolOutput = normalizeWhitespace(StripHarnessTags(b.ToolOutput))
		}
		out = append(out, b)
	}
	return out
}

// pruneZeroValues returns a copy of m with zero-valued entries removed
// and nested maps recursively pruned. A nested map that becomes empty
// after pruning is itself dropped. Slices and other complex values are
// passed through unchanged — extend the recursion if drift surfaces
// inside them later.
func pruneZeroValues(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if nested, ok := v.(map[string]any); ok {
			pruned := pruneZeroValues(nested)
			if len(pruned) == 0 {
				continue
			}
			out[k] = pruned
			continue
		}
		if isZeroValue(v) {
			continue
		}
		out[k] = v
	}
	return out
}

// isZeroValue reports whether v is the zero value for its JSON kind.
// JSON numbers decode to float64 through encoding/json, but tool input
// can also reach us via direct map literals, so int / int64 are covered
// for completeness.
func isZeroValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case bool:
		return !x
	case string:
		return x == ""
	case float64:
		return x == 0
	case float32:
		return x == 0
	case int:
		return x == 0
	case int64:
		return x == 0
	case []any:
		return len(x) == 0
	default:
		return false
	}
}

// StripHarnessTags removes every <tag>…</tag> span whose tag is in
// HarnessTags from text. Exported for consumers that need the same
// harness-noise removal outside chain-hash projection — e.g. the span
// embedding render, where a several-KB <system-reminder> block would
// otherwise dominate the vector and flatten semantic search ranking.
func StripHarnessTags(text string) string {
	for _, tag := range HarnessTags {
		text = stripTaggedSpan(text, tag)
	}
	return text
}

// previewWrapperTags are the harness wrappers whose INNER text is the
// human's own words: the opener a harness side-call re-sends (<session>),
// a re-injected plan (<conversation>), and the arguments typed to a slash
// command (<command-args>). For a human-facing turn preview these are
// UNWRAPPED (inner text kept) rather than stripped whole, so a turn made
// entirely of harness scaffolding still previews what the user actually
// wrote instead of the <system-reminder> boilerplate. Every other
// HarnessTag is volatile per-turn noise and is stripped whole.
var previewWrapperTags = map[string]bool{
	"session":      true,
	"conversation": true,
	"command-args": true,
}

// PreviewText projects one content block's Text for a human-facing turn
// preview: unwrap the content-bearing harness wrappers (keep their inner
// text) and strip every other harness span whole, then normalize
// whitespace. Unlike ProjectContent — which strips ALL harness spans for
// content identity — this preserves the human's words carried inside a
// wrapper, so the preview reads as prose rather than as harness framing.
// Returns "" when nothing human survives (the caller drops the block
// rather than falling back to the raw, un-projected text).
//
// Tags are processed in HarnessTags order; because stripTaggedSpan and
// unwrapTaggedSpan each rewrite the whole string, one pass handles noise
// nested inside a wrapper and wrappers nested inside wrappers alike.
func PreviewText(text string) string {
	for _, tag := range HarnessTags {
		if previewWrapperTags[tag] {
			text = unwrapTaggedSpan(text, tag)
		} else {
			text = stripTaggedSpan(text, tag)
		}
	}
	return normalizeWhitespace(text)
}

// unwrapTaggedSpan replaces every <tag>…</tag> span with its inner text,
// dropping only the tag markers. An unterminated open tag keeps the text
// after the marker (mirroring, in reverse, stripTaggedSpan's rule that an
// unterminated open tag swallows the rest of the string).
func unwrapTaggedSpan(text, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"
	var out strings.Builder
	for {
		start := strings.Index(text, openTag)
		if start == -1 {
			out.WriteString(text)
			return out.String()
		}
		out.WriteString(text[:start])
		rest := text[start+len(openTag):]
		inner, after, ok := strings.Cut(rest, closeTag)
		if !ok {
			out.WriteString(rest) // unterminated: keep inner, drop the open marker
			return out.String()
		}
		out.WriteString(inner)
		text = after
	}
}

// NormalizeForEmbed strips harness-tag spans and folds away whitespace
// drift, producing the canonical prose used for span embedding. It
// composes StripHarnessTags with the same whitespace normalization
// ProjectContent applies to chain-hash text, so embed render and chain
// hash treat harness noise and whitespace identically.
func NormalizeForEmbed(text string) string {
	return normalizeWhitespace(StripHarnessTags(text))
}

// stripTaggedSpan removes every <tag>…</tag> span from text. An
// unterminated open tag is treated as swallowing the rest of the
// string, matching the behavior of sessions.StripTaggedSection.
func stripTaggedSpan(text, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"
	for {
		start := strings.Index(text, openTag)
		if start == -1 {
			return text
		}
		rest := text[start:]
		_, after, ok := strings.Cut(rest, closeTag)
		if !ok {
			return text[:start]
		}
		text = text[:start] + after
	}
}

var (
	trailingLineSpace   = regexp.MustCompile(`[ \t]+\n`)
	consecutiveNewlines = regexp.MustCompile(`\n{2,}`)
)

// normalizeWhitespace folds away whitespace-only drift so two captures
// of the same logical prose hash to the same value. CRLF is canonicalized
// to LF, trailing horizontal whitespace on each line is dropped, runs of
// two or more newlines collapse to a single newline, and the result is
// stripped on both ends. The single-newline collapse is what catches the
// 1-char "blank line inserted around line 58" diff documented in
// PCC-562.
func normalizeWhitespace(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = trailingLineSpace.ReplaceAllString(text, "\n")
	text = consecutiveNewlines.ReplaceAllString(text, "\n")
	return strings.TrimSpace(text)
}

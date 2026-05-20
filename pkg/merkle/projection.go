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
//
// The input slice and its blocks are not mutated; the returned slice is
// independent. Bucket.Content itself stays unprojected so display,
// labels, and search continue to see the raw text the model received.
func ProjectContent(blocks []llm.ContentBlock) []llm.ContentBlock {
	out := make([]llm.ContentBlock, 0, len(blocks))
	for _, b := range blocks {
		if b.Text != "" {
			projected := normalizeWhitespace(stripHarnessTags(b.Text))
			if projected == "" {
				continue
			}
			b.Text = projected
		}
		out = append(out, b)
	}
	return out
}

func stripHarnessTags(text string) string {
	for _, tag := range HarnessTags {
		text = stripTaggedSpan(text, tag)
	}
	return text
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
		end := strings.Index(rest, closeTag)
		if end == -1 {
			return text[:start]
		}
		text = text[:start] + rest[end+len(closeTag):]
	}
}

var (
	trailingLineSpace = regexp.MustCompile(`[ \t]+\n`)
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

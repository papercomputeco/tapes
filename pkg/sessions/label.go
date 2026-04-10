package sessions

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

const (
	labelLimit   = 36
	labelPrompts = 3
)

// BuildLabel derives a short human-readable label for the session from the
// most recent user-role prompts in the chain. Falls back to a truncated form
// of the leaf hash if no usable prompts are found.
func BuildLabel(nodes []*merkle.Node) string {
	if len(nodes) == 0 {
		return ""
	}

	labelLines := make([]string, 0, labelPrompts)
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		if n.Bucket.Role != roleUser {
			continue
		}
		text := strings.TrimSpace(ExtractLabelText(n.Bucket.Content))
		if text == "" {
			continue
		}
		line := firstLabelLine(text)
		if line == "" {
			continue
		}
		labelLines = append(labelLines, line)
		if len(labelLines) >= labelPrompts {
			break
		}
	}

	if len(labelLines) == 0 {
		return truncate(nodes[len(nodes)-1].Hash, 12)
	}

	for i, j := 0, len(labelLines)-1; i < j; i, j = i+1, j-1 {
		labelLines[i], labelLines[j] = labelLines[j], labelLines[i]
	}

	label := strings.Join(labelLines, " / ")
	return truncate(label, labelLimit)
}

// ExtractLabelText concatenates human-visible text from content blocks for
// label-building purposes, stripping tagged meta sections like
// <system-reminder>...</system-reminder>.
func ExtractLabelText(blocks []llm.ContentBlock) string {
	texts := []string{}
	for _, block := range blocks {
		if block.Text == "" {
			continue
		}
		texts = append(texts, block.Text)
	}

	text := strings.TrimSpace(strings.Join(texts, "\n"))
	text = StripTaggedSection(text, "system-reminder")
	text = StripTaggedSection(text, "local-command")
	return strings.TrimSpace(text)
}

// StripTaggedSection removes all occurrences of a given XML-like tagged
// section (e.g. <system-reminder>…</system-reminder>) from text.
func StripTaggedSection(text, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"

	for {
		start := strings.Index(text, openTag)
		if start == -1 {
			break
		}
		end := strings.Index(text[start:], closeTag)
		if end == -1 {
			text = strings.TrimSpace(text[:start])
			break
		}
		end = start + end + len(closeTag)
		text = strings.TrimSpace(text[:start] + text[end:])
	}

	return strings.TrimSpace(text)
}

func firstLabelLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || isTagLine(line) || isCommandLine(line) {
			continue
		}
		return line
	}
	return ""
}

func isCommandLine(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	return strings.HasPrefix(value, "command:")
}

func isTagLine(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">")
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}

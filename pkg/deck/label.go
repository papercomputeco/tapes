package deck

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage/ent"
)

type userLabelCandidate struct {
	text string
}

func buildLabel(nodes []*ent.Node) string {
	const labelLimit = 36
	const labelPrompts = 3

	labelLines := make([]string, 0, labelPrompts)
	for i := len(nodes) - 1; i >= 0; i-- {
		node := nodes[i]
		if node.Role != roleUser {
			continue
		}
		blocks, _ := parseContentBlocks(node.Content)
		text := strings.TrimSpace(extractLabelText(blocks))
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
		return truncate(nodes[len(nodes)-1].ID, 12)
	}

	for i, j := 0, len(labelLines)-1; i < j; i, j = i+1, j-1 {
		labelLines[i], labelLines[j] = labelLines[j], labelLines[i]
	}

	label := strings.Join(labelLines, " / ")
	return truncate(label, labelLimit)
}

// buildLabelFromCandidates builds a label from pre-extracted user prompt texts
// (collected in forward order). This avoids re-parsing content blocks.
func buildLabelFromCandidates(candidates []userLabelCandidate, fallbackID string) string {
	const labelLimit = 36
	const labelPrompts = 3

	labelLines := make([]string, 0, labelPrompts)
	for i := len(candidates) - 1; i >= 0; i-- {
		line := firstLabelLine(candidates[i].text)
		if line == "" {
			continue
		}
		labelLines = append(labelLines, line)
		if len(labelLines) >= labelPrompts {
			break
		}
	}

	if len(labelLines) == 0 {
		return truncate(fallbackID, 12)
	}

	for i, j := 0, len(labelLines)-1; i < j; i, j = i+1, j-1 {
		labelLines[i], labelLines[j] = labelLines[j], labelLines[i]
	}

	label := strings.Join(labelLines, " / ")
	return truncate(label, labelLimit)
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

func extractLabelText(blocks []llm.ContentBlock) string {
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

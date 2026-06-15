package sessions

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// ExtractText concatenates visible text across text, tool_output, and
// tool_use blocks, joined by newlines.
func ExtractText(blocks []llm.ContentBlock) string {
	texts := []string{}
	for _, block := range blocks {
		switch {
		case block.Text != "":
			texts = append(texts, block.Text)
		case block.ToolOutput != "":
			texts = append(texts, block.ToolOutput)
		case block.ToolName != "":
			texts = append(texts, "tool call: "+block.ToolName)
		}
	}
	return strings.Join(texts, "\n")
}

// ExtractToolCalls returns the names of all tool_use blocks.
func ExtractToolCalls(blocks []llm.ContentBlock) []string {
	tools := []string{}
	for _, block := range blocks {
		if block.Type == blockTypeToolUse && block.ToolName != "" {
			tools = append(tools, block.ToolName)
		}
	}
	return tools
}

// CountToolCalls returns how many tool_use blocks are in the slice.
func CountToolCalls(blocks []llm.ContentBlock) int {
	count := 0
	for _, block := range blocks {
		if block.Type == blockTypeToolUse {
			count++
		}
	}
	return count
}

// BlocksHaveToolError reports whether any tool_result block is marked as an error.
func BlocksHaveToolError(blocks []llm.ContentBlock) bool {
	for _, block := range blocks {
		if block.Type == blockTypeToolResult && block.IsError {
			return true
		}
	}
	return false
}

// CountToolResults returns how many tool_result blocks are in the slice.
func CountToolResults(blocks []llm.ContentBlock) int {
	count := 0
	for _, block := range blocks {
		if block.Type == blockTypeToolResult {
			count++
		}
	}
	return count
}

// CountToolResultErrors returns how many tool_result blocks are marked errors.
func CountToolResultErrors(blocks []llm.ContentBlock) int {
	count := 0
	for _, block := range blocks {
		if block.Type == blockTypeToolResult && block.IsError {
			count++
		}
	}
	return count
}

// gitCommandPatterns matches common git commit and push invocations inside
// shell command strings captured from Bash tool calls.
var gitCommandPatterns = []string{
	"git commit",
	"git push",
}

// BlocksHaveGitActivity reports whether the blocks contain a Bash tool call
// whose command invokes `git commit` or `git push`.
func BlocksHaveGitActivity(blocks []llm.ContentBlock) bool {
	for _, block := range blocks {
		if block.Type != blockTypeToolUse || block.ToolName != "Bash" {
			continue
		}
		cmd, _ := block.ToolInput["command"].(string)
		if cmd == "" {
			continue
		}
		lower := strings.ToLower(cmd)
		for _, pattern := range gitCommandPatterns {
			if strings.Contains(lower, pattern) {
				return true
			}
		}
	}
	return false
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

package deck

import (
	"encoding/json"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

func parseContentBlocks(raw []map[string]any) ([]llm.ContentBlock, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	var blocks []llm.ContentBlock
	if err := json.Unmarshal(data, &blocks); err != nil {
		return nil, err
	}

	return blocks, nil
}

func extractToolCalls(blocks []llm.ContentBlock) []string {
	tools := []string{}
	for _, block := range blocks {
		if block.Type == blockTypeToolUse && block.ToolName != "" {
			tools = append(tools, block.ToolName)
		}
	}
	return tools
}

func countToolCalls(blocks []llm.ContentBlock) int {
	count := 0
	for _, block := range blocks {
		if block.Type == blockTypeToolUse {
			count++
		}
	}
	return count
}

func blocksHaveToolError(blocks []llm.ContentBlock) bool {
	for _, block := range blocks {
		if block.Type == "tool_result" && block.IsError {
			return true
		}
	}
	return false
}

// gitCommandPattern matches common git commit and push invocations inside
// shell command strings captured from Bash tool calls.
var gitCommandPatterns = []string{
	"git commit",
	"git push",
}

func blocksHaveGitActivity(blocks []llm.ContentBlock) bool {
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

func extractText(blocks []llm.ContentBlock) string {
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

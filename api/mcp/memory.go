package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/papercomputeco/tapes/pkg/memory"
)

var (
	memoryRecallToolName    = "memory_recall"
	memoryRecallDescription = "Recall facts from the tapes memory layer. Given a node hash (a position in the conversation DAG), returns extracted facts that are relevant to that position. Use this to retrieve persistent knowledge from past conversations."
)

// MemoryRecallInput represents the input arguments for the MCP memory_recall tool.
type MemoryRecallInput struct {
	Hash string `json:"hash" jsonschema:"the node hash identifying a position in the conversation DAG to recall facts for"`
}

// MemoryRecallOutput represents the structured output of a memory recall.
type MemoryRecallOutput struct {
	Facts []memory.Fact `json:"facts"`
}

// handleMemoryRecall processes a memory recall request via MCP.
func (s *Server) handleMemoryRecall(ctx context.Context, _ *mcp.CallToolRequest, input MemoryRecallInput) (*mcp.CallToolResult, MemoryRecallOutput, error) {
	if input.Hash == "" {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: "hash is required"},
			},
		}, MemoryRecallOutput{}, nil
	}

	facts, err := s.config.MemoryDriver.Recall(ctx, input.Hash)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Memory recall failed: %v", err)},
			},
		}, MemoryRecallOutput{}, nil
	}

	if facts == nil {
		facts = []memory.Fact{}
	}

	output := MemoryRecallOutput{Facts: facts}

	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Failed to serialize results: %v", err)},
			},
		}, MemoryRecallOutput{}, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(jsonBytes)},
		},
	}, output, nil
}

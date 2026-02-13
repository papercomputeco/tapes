package storage

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/agenttrace"
)

// AgentTraceStore defines the interface for persisting and retrieving agent traces.
type AgentTraceStore interface {
	CreateAgentTrace(ctx context.Context, trace *agenttrace.AgentTrace) (*agenttrace.AgentTrace, error)
	GetAgentTrace(ctx context.Context, id string) (*agenttrace.AgentTrace, error)
	QueryAgentTraces(ctx context.Context, query AgentTraceQuery) ([]*agenttrace.AgentTrace, error)
	Close() error
}

// AgentTraceQuery defines query parameters for filtering agent traces.
type AgentTraceQuery struct {
	FilePath string
	Revision string
	ToolName string
	Limit    int
	Offset   int
}

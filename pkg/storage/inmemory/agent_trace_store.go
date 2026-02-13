package inmemory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/papercomputeco/tapes/pkg/agenttrace"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// AgentTraceStore implements storage.AgentTraceStore using an in-memory map.
type AgentTraceStore struct {
	mu     sync.RWMutex
	traces map[string]*agenttrace.AgentTrace
}

// NewAgentTraceStore creates a new in-memory agent trace store.
func NewAgentTraceStore() *AgentTraceStore {
	return &AgentTraceStore{
		traces: make(map[string]*agenttrace.AgentTrace),
	}
}

// CreateAgentTrace stores an agent trace.
func (s *AgentTraceStore) CreateAgentTrace(_ context.Context, trace *agenttrace.AgentTrace) (*agenttrace.AgentTrace, error) {
	if trace == nil {
		return nil, errors.New("cannot store nil agent trace")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.traces[trace.ID] = trace
	return trace, nil
}

// GetAgentTrace retrieves an agent trace by ID.
func (s *AgentTraceStore) GetAgentTrace(_ context.Context, id string) (*agenttrace.AgentTrace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trace, ok := s.traces[id]
	if !ok {
		return nil, fmt.Errorf("agent trace not found: %s", id)
	}
	return trace, nil
}

// QueryAgentTraces queries agent traces with filtering.
func (s *AgentTraceStore) QueryAgentTraces(_ context.Context, query storage.AgentTraceQuery) ([]*agenttrace.AgentTrace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*agenttrace.AgentTrace

	for _, trace := range s.traces {
		if !matchesQuery(trace, query) {
			continue
		}
		results = append(results, trace)
	}

	// Apply offset
	if query.Offset > 0 && query.Offset < len(results) {
		results = results[query.Offset:]
	} else if query.Offset >= len(results) {
		return []*agenttrace.AgentTrace{}, nil
	}

	// Apply limit
	if query.Limit > 0 && query.Limit < len(results) {
		results = results[:query.Limit]
	}

	return results, nil
}

// Close is a no-op for the in-memory store.
func (s *AgentTraceStore) Close() error {
	return nil
}

func matchesQuery(trace *agenttrace.AgentTrace, query storage.AgentTraceQuery) bool {
	if query.FilePath != "" {
		found := false
		for _, f := range trace.Files {
			if strings.EqualFold(f.Path, query.FilePath) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if query.Revision != "" {
		if trace.VCS == nil || trace.VCS.Revision != query.Revision {
			return false
		}
	}

	if query.ToolName != "" {
		if trace.Tool == nil || trace.Tool.Name != query.ToolName {
			return false
		}
	}

	return true
}

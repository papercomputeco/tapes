package provider

import (
	"errors"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// ErrStreamingNotImplemented is returned by ParseStreamChunk when a provider
// does not yet support streaming parsing.
var ErrStreamingNotImplemented = errors.New("streaming not implemented for this provider")

// Provider defines the interface for LLM API parsing.
// Each provider implementation knows how to parse its specific
// API format into the internal representation.
type Provider interface {
	// Name returns the canonical provider name (e.g., "anthropic", "openai", "ollama", "besteffort")
	Name() string

	// ParseRequest converts a provider-specific request into the internal format.
	// Returns an error if the payload cannot be parsed.
	ParseRequest(payload []byte) (*llm.ChatRequest, error)

	// ParseResponse converts a provider-specific response into the internal format.
	// Returns an error if the payload cannot be parsed.
	ParseResponse(payload []byte) (*llm.ChatResponse, error)

	// ParseStreamChunk converts a single streaming chunk into the internal format.
	// Returns ErrStreamingNotImplemented if the provider doesn't support streaming yet.
	// Returns (nil, nil) if the chunk should be skipped (e.g., keep-alive, comments).
	ParseStreamChunk(payload []byte) (*llm.StreamChunk, error)
}

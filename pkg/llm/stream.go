package llm

import "time"

// StreamChunk represents a single chunk in a streaming response.
// This is the internal representation used by the proxy after parsing
// provider-specific streaming formats.
type StreamChunk struct {
	// Model that generated the chunk
	Model string `json:"model"`

	// Chunk timestamp
	CreatedAt time.Time `json:"created_at,omitempty"`

	// The content of this chunk (typically a partial message)
	Message Message `json:"message"`

	// Whether this is the final chunk
	Done bool `json:"done"`

	// Index for providers that support multiple parallel completions
	Index int `json:"index,omitempty"`

	// Stop reason (only present on final chunk)
	StopReason string `json:"stop_reason,omitempty"`

	// Usage metrics (typically only present on final chunk)
	Usage *Usage `json:"usage,omitempty"`
}

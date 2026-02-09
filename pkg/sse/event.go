// Package sse provides a minimal, purpose-built SSE (Server-Sent Events)
// tee-reader for use in the tapes proxy. It is designed to parse SSE events from
// an upstream LLM provider while simultaneously forwarding the raw bytes
// verbatim to a downstream client in a tee pipe fashion.
//
// This package intentionally does NOT provide SSE writer or server
// capabilities.
//
// See the SSE specification:
// https://html.spec.whatwg.org/multipage/server-sent-events.html
package sse

// Event represents a single parsed SSE event, delimited by a blank line
// in the upstream byte stream.
type Event struct {
	// Type is the SSE event type from the "event:" field.
	// An empty string means the default "message" type per the SSE spec.
	Type string

	// Data is the concatenated contents of all "data:" lines for this event,
	// joined with "\n" (per the SSE spec, multiple data fields are joined
	// with a single newline).
	Data string

	// ID is the last event ID from the "id:" field, if present.
	ID string
}

package sse

import (
	"bufio"
	"io"
	"strings"
)

// TeeReader reads SSE events from a source io.Reader while simultaneously
// writing all raw bytes verbatim to a destination io.writer.
// This effectively enables "tee" shaped reading where TeeReader.Next
// returns the Event for consumption while writing to a separate destination.
//
// ┌──────────────────┐
// │ source io.Reader │
// └──────────────────┘
// │
// ▼
// ┌──────────────────┐   ┌───────────────────────┐
// │ TeeReader.Next() │──▶│ destination io.Writer │
// └──────────────────┘   └───────────────────────┘
// │
// ▼
// ┌──────────────────┐
// │      Event       │
// └──────────────────┘
//
// An SSE client io.Writer receives the exact copy of the
// stream, while the caller can inspect parsed events.
type TeeReader struct {
	scanner *bufio.Scanner
	dest    io.Writer

	// current accumulates fields for the event being built in the current scan.
	current *Event
	hasData bool
}

// NewTeeReader returns a Reader that parses SSE events from the src io.Reader
// and writes all raw bytes through to dest.
// The dest writer typically backs an io.Pipe connected to the downstream HTTP
// response.
func NewTeeReader(src io.Reader, dest io.Writer) *TeeReader {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	return &TeeReader{
		scanner: scanner,
		dest:    dest,
		current: &Event{},
	}
}

// Next returns the next parsed SSE event from the scanner. It blocks until a
// complete event is available (terminated by a blank line in the stream).
// Next returns nil, nil when the source is exhausted.
//
// Next also tees all bytes to the destination writer supplied
// to NewTeeReader. This ensures the downstream client receivers can consume
// SSE stream bytes verbatim.
func (r *TeeReader) Next() (*Event, error) {
	for r.scanner.Scan() {
		raw := r.scanner.Text()

		// Write the raw line content and newline to the destination.
		// bufio.Scanner strips the newline from the Scan() so we reinsert it here.
		_, err := io.WriteString(r.dest, raw+"\n")
		if err != nil {
			return nil, err
		}

		// A blank line signals the end of the current event.
		if raw == "" {
			if r.hasData {
				currentEvent := r.current
				r.reset()
				return currentEvent, nil
			}

			// Blank line with no accumulated fields — skip (e.g. leading
			// blank lines or keep-alive newlines).
			continue
		}

		// Lines starting with ':' are comments. Skip them in Event parsing.
		if strings.HasPrefix(raw, ":") {
			continue
		}

		r.parseLine(raw)
	}

	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	// Source exhausted and no error from scanner.
	// If there is an in-progress event (stream ended without a trailing blank
	// line), yield it.
	if r.hasData {
		ev := r.current
		r.reset()
		return ev, nil
	}

	return nil, nil
}

// parseLine processes a single non-empty, non-comment SSE line and
// accumulates the field into the current event.
//
// Per the SSE spec, a line has the form "field:value" where the first
// space after the colon is optional and stripped if present.
func (r *TeeReader) parseLine(line string) {
	var field, value string

	if before, after, ok := strings.Cut(line, ":"); ok {
		field = before
		value = after
		// Strip a single leading space after the colon, per spec.
		value = strings.TrimPrefix(value, " ")
	} else {
		// Line with no colon: the entire line is the field name with
		// an empty value.
		field = line
	}

	switch field {
	case "data":
		if r.hasData && r.current.Data != "" {
			// Multiple data fields are joined with "\n".
			r.current.Data += "\n"
		}
		r.current.Data += value
		r.hasData = true
	case "event":
		r.current.Type = value
		r.hasData = true
	case "id":
		r.current.ID = value
		r.hasData = true
	default:
		// * "retry" is intentionally ignored — not relevant for proxy use.
		// * Other unknown fields are ignored per the SSE spec.
	}
}

// reset clears the accumulated event state for the next event.
func (r *TeeReader) reset() {
	r.current = &Event{}
	r.hasData = false
}

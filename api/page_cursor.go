package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// Every paged read on this surface mints its cursor the same way: the
// keyset boundary of the last row served plus the scope it was minted
// under, rendered as base64url JSON so it is opaque on the wire. The
// codec below is that convention once; each pager keeps only its
// boundary type and the required-field check that makes a hand-crafted
// or truncated token a 400 rather than a boundary it never minted.

// encodeCursor renders a page cursor for the wire.
func encodeCursor[T any](v T) string {
	b, err := json.Marshal(v)
	if err != nil {
		// json.Marshal cannot fail for the cursor struct shapes.
		panic(fmt.Sprintf("encoding page cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor reads a cursor off the wire. It checks only that the
// token is well-formed; whether the decoded value names a boundary is
// the caller's check, since only it knows which fields a minted cursor
// always carries.
func decodeCursor[T any](raw string) (T, error) {
	var zero T
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return zero, fmt.Errorf("invalid cursor: %w", err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return zero, fmt.Errorf("invalid cursor: %w", err)
	}
	return v, nil
}

// parseLimit reads a page's `limit` query: empty means def, anything
// that is not a positive integer is rejected rather than defaulted, and
// a value past upper is clamped to it.
func parseLimit(raw string, def, upper int) (int, error) {
	if raw == "" {
		return def, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	return min(parsed, upper), nil
}

// tracesPageCursor is the decoded GET /v1/sessions/:id/traces pagination
// cursor: the boundary of the last trace the previous page served, in the
// order the composite emits traces (started_at, trace_id). It carries the
// session it was minted under so a token replayed against another session
// is rejected rather than reinterpreted as a boundary that session never
// produced.
type tracesPageCursor struct {
	Session   string    `json:"session"`
	TraceID   string    `json:"trace_id"`
	StartedAt time.Time `json:"started_at"`
}

func encodeTracesPageCursor(c tracesPageCursor) string {
	return encodeCursor(c)
}

func decodeTracesPageCursor(token string) (tracesPageCursor, error) {
	c, err := decodeCursor[tracesPageCursor](token)
	if err != nil {
		return tracesPageCursor{}, err
	}
	// Every cursor we mint names its session and a trace boundary; a token
	// missing either is malformed or hand-crafted, not a legacy client.
	if c.Session == "" || c.TraceID == "" {
		return tracesPageCursor{}, errors.New("invalid cursor: missing trace boundary")
	}
	return c, nil
}

// covers reports whether turn is at or before the cursor boundary — i.e.
// a trace an earlier page already served. The comparison is the keyset
// (started_at, trace_id) the composite orders traces by, so a boundary
// trace that was re-derived away in between neither repeats nor skips its
// neighbours.
func (c tracesPageCursor) covers(turn storage.SpanTurnRecord) bool {
	if turn.StartedAt.Before(c.StartedAt) {
		return true
	}
	if turn.StartedAt.Equal(c.StartedAt) {
		return turn.TraceID <= c.TraceID
	}
	return false
}

// tracePageCursor is the decoded GET /v1/traces/:trace_id pagination
// cursor: the key of the last span the previous page served, in the
// order a trace streams spans (seq, started_at, span_id) — a
// storage.SpanCursor, bound to the trace it was minted under so a token
// replayed against another trace is rejected rather than reinterpreted.
type tracePageCursor struct {
	TraceID   string    `json:"trace_id"`
	Seq       int64     `json:"seq"`
	StartedAt time.Time `json:"started_at"`
	SpanID    string    `json:"span_id"`
}

func encodeTracePageCursor(c tracePageCursor) string {
	return encodeCursor(c)
}

func decodeTracePageCursor(token string) (tracePageCursor, error) {
	c, err := decodeCursor[tracePageCursor](token)
	if err != nil {
		return tracePageCursor{}, err
	}
	// Every cursor we mint names its trace and a span; a token missing
	// either is malformed or hand-crafted, not a legacy client.
	if c.TraceID == "" || c.SpanID == "" {
		return tracePageCursor{}, errors.New("invalid cursor: missing span boundary")
	}
	return c, nil
}

// spanCursor is the storage resumption point the cursor names.
func (c tracePageCursor) spanCursor() storage.SpanCursor {
	return storage.SpanCursor{TraceID: c.TraceID, Seq: c.Seq, StartedAt: c.StartedAt, SpanID: c.SpanID}
}

// rawTurnsPageCursor is the decoded GET /v1/sessions/:id/raw_turns
// pagination cursor: the id of the last raw turn the previous page
// served, in the order the wire log lists them (id ascending), bound to
// the session it was minted under.
type rawTurnsPageCursor struct {
	Session string `json:"session"`
	ID      int64  `json:"id"`
}

func encodeRawTurnsPageCursor(c rawTurnsPageCursor) string {
	return encodeCursor(c)
}

func decodeRawTurnsPageCursor(token string) (rawTurnsPageCursor, error) {
	c, err := decodeCursor[rawTurnsPageCursor](token)
	if err != nil {
		return rawTurnsPageCursor{}, err
	}
	// Every cursor we mint names its session and a raw turn; ids start
	// at 1, so a zero or negative one is no boundary we produced.
	if c.Session == "" || c.ID <= 0 {
		return rawTurnsPageCursor{}, errors.New("invalid cursor: missing raw turn boundary")
	}
	return c, nil
}

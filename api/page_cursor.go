package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// tracesPageCursor is the decoded GET /v1/sessions/:id/traces pagination
// cursor: the boundary of the last trace the previous page served, in the
// order the composite emits traces (started_at, trace_id). It carries the
// session it was minted under so a token replayed against another session
// is rejected rather than reinterpreted as a boundary that session never
// produced. Opaque on the wire — base64url JSON, the same convention as
// the /v1/sessions cursor.
type tracesPageCursor struct {
	Session   string    `json:"session"`
	TraceID   string    `json:"trace_id"`
	StartedAt time.Time `json:"started_at"`
}

func encodeTracesPageCursor(c tracesPageCursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		// json.Marshal cannot fail for this struct shape.
		panic(fmt.Sprintf("encoding traces page cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeTracesPageCursor(token string) (tracesPageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return tracesPageCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c tracesPageCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return tracesPageCursor{}, fmt.Errorf("invalid cursor: %w", err)
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
// On the wire it follows the same convention as the composite's cursor:
// base64url JSON, opaque to clients.
type tracePageCursor struct {
	TraceID   string    `json:"trace_id"`
	Seq       int64     `json:"seq"`
	StartedAt time.Time `json:"started_at"`
	SpanID    string    `json:"span_id"`
}

func encodeTracePageCursor(c tracePageCursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		// json.Marshal cannot fail for this struct shape.
		panic(fmt.Sprintf("encoding trace page cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeTracePageCursor(token string) (tracePageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return tracePageCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c tracePageCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return tracePageCursor{}, fmt.Errorf("invalid cursor: %w", err)
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

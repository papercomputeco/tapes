package api

import (
	"bufio"
	"context"
	"encoding/json"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// One page of GET /v1/sessions/:id/traces, written straight into the
// response stream. The composite used to be assembled whole and handed to
// c.JSON, which made a request's peak memory the whole session's payload
// three times over (rows, wire items, one JSON byte slice). Here the only
// things resident for the length of a page are the payload-free turn
// headers and links, the one span being encoded, and the stream writer's
// buffer; every span is read, rendered, written and dropped before the
// next row is scanned.
//
// The document is framed by hand so it stays exactly the JSON
// json.Marshal(SessionTracesResponse) would produce — same shape, same
// field order, same [] pins — which is what keeps BuildSessionTraces the
// reference (and `tapes dev trace-fixtures` byte-identical to the wire).
// It is one JSON document per page, not NDJSON: every consumer parses the
// body as a document, and a page keeps all of them working unchanged.

const (
	defaultTracesLimit = 50
	maxTracesLimit     = 200
)

// tracesPageByteBudget is the emitted-bytes bound on one page, measured
// before compression. Item counts alone do not bound a page — a span
// carries whole tool results and base64 images — so once a trace closes
// past this many bytes the page closes with it and the rest continues
// behind next_cursor. A page therefore may hold one trace, and a client
// must not read len(traces) < limit as "no more". It is a variable rather
// than a constant so a spec can shrink it to something a fixture can
// cross.
var tracesPageByteBudget int64 = 8 << 20

// tracesPage is the state one page needs: what was loaded before the
// status was committed (session, turn headers past the cursor, links) and
// the reader the spans stream from.
type tracesPage struct {
	sessionID string
	orgID     string
	session   SessionItem
	// turns are the session's turn headers not yet served, in emit order
	// (started_at, trace_id). Trace order is the turn order, which is not
	// the order spans sort in — so spans are read one trace at a time
	// rather than in one session-wide pass.
	turns  []storage.TraceSummaryRecord
	links  []storage.SpanLinkRecord
	mode   PayloadMode
	limit  int
	budget int64
	spans  spanModelReader
}

// write encodes the page into w. It stops at the first sign the reader
// is gone — a cancelled ctx or a failed write — and returns the reason;
// the status is long committed by then, so a cut stream is truncated
// output, not an error response. A page that closed cleanly returns nil.
func (p *tracesPage) write(ctx context.Context, w *bufio.Writer) error {
	out := &pageWriter{w: w}
	out.writeString(`{"schema":"` + ProjectionSchema + `","session":`)
	out.marshal(p.session)
	out.writeString(`,"traces":[`)

	var next *tracesPageCursor
	for i, turn := range p.turns {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i > 0 {
			out.writeString(",")
		}
		if err := p.writeTrace(ctx, out, turn); err != nil {
			return err
		}
		// A trace boundary is where the client can make use of what it
		// has, so push it through; the flush is also what surfaces a
		// hung-up client before another trace is read for nothing.
		if err := out.flush(); err != nil {
			return err
		}
		served := i + 1
		if served >= p.limit || out.n > p.budget {
			if served < len(p.turns) {
				next = &tracesPageCursor{
					Session:   p.sessionID,
					TraceID:   turn.TraceID,
					StartedAt: turn.StartedAt,
				}
			}
			break
		}
	}

	// Links stay session-scoped and whole on every page: they are the
	// payload-free half of the model, and an edge may touch a trace on
	// another page.
	links := make([]SpanLinkItem, 0, len(p.links))
	for _, l := range p.links {
		links = append(links, spanLinkItem(l))
	}
	out.writeString(`],"links":`)
	out.marshal(links)
	if next != nil {
		out.writeString(`,"next_cursor":`)
		out.marshal(encodeTracesPageCursor(*next))
	}
	out.writeString("}")
	return out.err
}

// writeTrace encodes one TraceDetail — {"trace":…,"spans":[…]} — reading
// the trace's spans one row at a time. Links are omitted, as they are on
// every embedded TraceDetail. Returning from inside the range closes the
// underlying rows.
func (p *tracesPage) writeTrace(ctx context.Context, out *pageWriter, turn storage.TraceSummaryRecord) error {
	out.writeString(`{"trace":`)
	out.marshal(traceItemFromTurn(turn.SpanTurnRecord, turn.SpanCount))
	out.writeString(`,"spans":[`)
	first := true
	for sp, err := range p.spans.IterateTraceSpans(ctx, p.orgID, turn.TraceID, storage.SpanCursor{}, p.mode) {
		if err != nil {
			return err
		}
		// Stop pulling rows the moment a write has failed; there is no
		// one left to encode them for.
		if out.err != nil {
			return out.err
		}
		if !first {
			out.writeString(",")
		}
		first = false
		out.marshal(spanItemFromRecord(sp, p.mode))
	}
	out.writeString("]}")
	return out.err
}

// pageWriter counts what it emits and keeps the first failure, so the
// encoder can close a page on bytes actually written and stop as soon as
// the client stops taking them. After an error every call is a no-op.
type pageWriter struct {
	w   *bufio.Writer
	n   int64
	err error
}

func (pw *pageWriter) writeString(s string) {
	if pw.err != nil {
		return
	}
	n, err := pw.w.WriteString(s)
	pw.n += int64(n)
	pw.err = err
}

// marshal encodes v with the same encoder c.JSON would have used on the
// whole document, so each item is byte-identical to its materialized
// form. An unencodable item ends the page: the document is already
// half-written, and a hole would be worse than a cut.
func (pw *pageWriter) marshal(v any) {
	if pw.err != nil {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		pw.err = err
		return
	}
	n, werr := pw.w.Write(b)
	pw.n += int64(n)
	pw.err = werr
}

func (pw *pageWriter) flush() error {
	if pw.err != nil {
		return pw.err
	}
	pw.err = pw.w.Flush()
	return pw.err
}

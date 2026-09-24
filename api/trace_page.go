package api

import (
	"bufio"
	"context"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// One page of GET /v1/traces/:trace_id, streamed. A trace is bounded in
// turns but not in spans — a long agent loop is thousands of them, each
// carrying whole tool results — so the standalone lookup pages spans the
// way the composite pages traces: the payload-free header and the
// trace's links are loaded whole, the status is committed, and then each
// span is read, rendered, written and dropped before the next row is
// scanned. Peak memory is one span, whatever the trace's size.
//
// The document is framed by hand in exactly the field order
// json.Marshal(StandaloneTraceDetail) produces, so a page that holds the
// whole trace is byte-identical to the materialized form BuildTraceDetail
// renders (which `tapes dev trace-fixtures` still emits). One JSON
// document per page, not NDJSON, for the same reason the composite is.

const (
	defaultTraceSpansLimit = 200
	maxTraceSpansLimit     = 1000
)

// tracePage is the state one page needs: what was loaded before the
// status was committed (the turn header with its span count, the links
// touching the trace) and the reader the spans stream from.
type tracePage struct {
	orgID   string
	traceID string
	turn    storage.TraceSummaryRecord
	links   []storage.SpanLinkRecord
	mode    PayloadMode
	after   storage.SpanCursor
	limit   int
	budget  int64
	spans   spanModelReader
}

// write encodes the page into w. It stops at the first sign the reader
// is gone — a cancelled ctx or a failed write — and returns the reason;
// the status is long committed by then, so a cut stream is truncated
// output, not an error response. A page that closed cleanly returns nil.
func (p *tracePage) write(ctx context.Context, w *bufio.Writer) error {
	out := &pageWriter{w: w}
	out.writeString(`{"session_id":`)
	out.marshal(p.turn.SessionID)
	out.writeString(`,"schema":"` + ProjectionSchema + `","trace":`)
	// span_count is the trace's whole count, as the materialized detail
	// reports it, not the page's.
	out.marshal(traceItemFromTurn(p.turn.SpanTurnRecord, p.turn.SpanCount))
	out.writeString(`,"spans":[`)

	var (
		served int
		last   storage.SpanCursor
		closed bool
		more   bool
	)
	for sp, err := range p.spans.IterateTraceSpans(ctx, p.orgID, p.traceID, p.after, p.mode) {
		if err != nil {
			return err
		}
		// Stop pulling rows the moment a write has failed; there is no
		// one left to encode them for.
		if out.err != nil {
			return out.err
		}
		if closed {
			// The page is full; this row exists, so there is a next page.
			// Reading one row past the boundary is what tells the two
			// apart, and it is the one row this page does not render.
			more = true
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if served > 0 {
			out.writeString(",")
		}
		out.marshal(spanItemFromRecord(sp, p.mode))
		served++
		last = storage.SpanCursor{TraceID: p.traceID, Seq: sp.Seq, StartedAt: sp.StartedAt, SpanID: sp.SpanID}
		// The budget is checked at span boundaries, so a span always lands
		// whole and the page closes right after the one that crossed it.
		if served >= p.limit || out.n > p.budget {
			closed = true
		}
	}
	out.writeString("]")

	// Links stay trace-scoped and whole on every page: they are the
	// payload-free half of the model, and an edge may touch a span on
	// another page. Omitted when empty, as TraceDetail marshals them.
	if len(p.links) > 0 {
		links := make([]SpanLinkItem, 0, len(p.links))
		for _, l := range p.links {
			links = append(links, spanLinkItem(l))
		}
		out.writeString(`,"links":`)
		out.marshal(links)
	}
	if more {
		out.writeString(`,"next_cursor":`)
		out.marshal(encodeTracePageCursor(tracePageCursor{
			TraceID:   p.traceID,
			Seq:       last.Seq,
			StartedAt: last.StartedAt,
			SpanID:    last.SpanID,
		}))
	}
	out.writeString("}")
	return out.err
}

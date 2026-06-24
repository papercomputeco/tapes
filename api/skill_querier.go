package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/papercomputeco/tapes/pkg/skill"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// errSkillSessionNotFound marks a source session the caller's org cannot
// see (wrong tenant or absent). It survives the transcript builder's
// %w-wrapping, so the generate handler maps it to a 404 rather than a 500.
var errSkillSessionNotFound = errors.New("source session not found")

// skillTraceQuerier is the in-process, org-scoped implementation of
// skill.Querier the generator reads transcripts through. It replaces an
// HTTP loopback self-call: that hop carried no X-Tapes-Org-Id, so in
// multi-tenant deployments trace reads fell back to the nil-org sentinel
// and a real tenant's generate found zero turns and produced an empty
// skill. Binding the org at construction and calling the driver directly
// keeps every read scoped to the inbound tenant (and drops the spurious
// in-pod round trip, with its JWT and connection overhead).
type skillTraceQuerier struct {
	sessions sessionsReader
	spans    spanModelReader
	orgID    string
}

var _ skill.Querier = (*skillTraceQuerier)(nil)

// skillTraceQuerier builds the org-scoped querier, or returns false when
// the driver lacks the session/span read surface (mirrors the 501 guard
// the trace handlers apply). orgID is bound here so every read across the
// generation stays scoped to the one inbound tenant.
func (s *Server) skillTraceQuerier(orgID string) (*skillTraceQuerier, bool) {
	sessions, ok := s.driver.(sessionsReader)
	if !ok {
		return nil, false
	}
	spans, ok := s.driver.(spanModelReader)
	if !ok {
		return nil, false
	}
	return &skillTraceQuerier{sessions: sessions, spans: spans, orgID: orgID}, true
}

// TraceSummaries returns a session's user-visible turn headers, scoped to
// the bound org. The session is first looked up under the org — the same
// tenancy gate GET /v1/traces?session_id= applies — so a tenant cannot
// generate a skill from a session UUID it does not own.
func (q *skillTraceQuerier) TraceSummaries(ctx context.Context, sessionID string) ([]skill.TraceSummary, error) {
	sess, err := q.sessions.GetSessionRecord(ctx, q.orgID, sessionID)
	if err != nil {
		return nil, fmt.Errorf("load session %s: %w", sessionID, err)
	}
	if sess == nil {
		return nil, fmt.Errorf("%w: %s", errSkillSessionNotFound, sessionID)
	}

	rows, err := q.spans.ListTraceSummaries(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list trace summaries for session %s: %w", sessionID, err)
	}

	out := make([]skill.TraceSummary, 0, len(rows))
	for _, row := range rows {
		out = append(out, traceSummaryFromRecord(row.SpanTurnRecord))
	}
	return out, nil
}

// Trace returns one turn's spans with payloads, scoped to the bound org.
// A trace absent for the org yields (nil, nil): the transcript builder
// then falls back to the turn's derive-time response preview, exactly as
// it does when the HTTP client 404s.
func (q *skillTraceQuerier) Trace(ctx context.Context, traceID string) (*skill.Trace, error) {
	turn, spans, _, err := q.spans.GetTraceDetail(ctx, q.orgID, traceID)
	if err != nil {
		return nil, fmt.Errorf("get trace %s: %w", traceID, err)
	}
	if turn == nil {
		return nil, nil
	}

	trace := &skill.Trace{TraceID: turn.TraceID, Spans: make([]skill.Span, 0, len(spans))}
	for _, sp := range spans {
		trace.Spans = append(trace.Spans, skill.Span{
			SpanID:       sp.SpanID,
			ParentSpanID: sp.ParentSpanID,
			Kind:         sp.Kind,
			Name:         sp.Name,
			Seq:          sp.Seq,
			CallKind:     sp.CallKind,
			ThreadID:     sp.ThreadID,
			// llm/event spans store a content-block array; tool spans
			// carry their tool_result here, which the transcript builder
			// ignores in favour of the span Name. A failed decode is an
			// empty output, same as the wire path.
			Output: decodeBlocks(sp.Output),
		})
	}
	return trace, nil
}

// traceSummaryFromRecord maps a stored turn header to the generator's
// turn shape. Only the fields the transcript builder reads are carried;
// token counts ride along for parity with the wire client.
func traceSummaryFromRecord(rec storage.SpanTurnRecord) skill.TraceSummary {
	return skill.TraceSummary{
		TraceID:           rec.TraceID,
		UserPrompt:        rec.UserPrompt,
		ResponsePreview:   rec.ResponsePreview,
		Synthetic:         rec.Synthetic,
		StartedAt:         rec.StartedAt,
		TotalInputTokens:  rec.TotalInputTokens,
		TotalOutputTokens: rec.TotalOutputTokens,
		MainInputTokens:   rec.MainInputTokens,
		MainOutputTokens:  rec.MainOutputTokens,
	}
}

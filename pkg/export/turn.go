// Package export provides the shared conversation-export primitive:
// one turn's structured record plus a session-level JSONL writer. It is
// used by the `tapes checkout` CLI command and by the tapes API's
// session-export HTTP endpoints, so both surfaces render the exact same
// grain from the same code path.
package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/papercomputeco/tapes/pkg/skill"
)

// Turn is one turn (trace) in the structured (jsonl) export. With span
// detail it carries its derived span tree under Spans, mirroring the
// API's session → traces → spans shape.
type Turn struct {
	TraceID           string `json:"trace_id"`
	SessionID         string `json:"session_id"`
	UserPrompt        string `json:"user_prompt,omitempty"`
	Response          string `json:"response,omitempty"`
	StartedAt         string `json:"started_at,omitempty"`
	TotalInputTokens  int64  `json:"total_input_tokens"`
	TotalOutputTokens int64  `json:"total_output_tokens"`
	MainInputTokens   int64  `json:"main_input_tokens"`
	MainOutputTokens  int64  `json:"main_output_tokens"`
	Spans             []Span `json:"spans,omitempty"`
}

// Span is one span in a turn's derived span tree, emitted nested under
// the turn record in the jsonl export.
type Span struct {
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	Kind         string `json:"kind"` // llm | tool | agent | event
	Name         string `json:"name,omitempty"`
	Seq          int64  `json:"seq"`
	CallKind     string `json:"call_kind,omitempty"` // main | offshoot:… | injected:…
	ThreadID     string `json:"thread_id,omitempty"`
}

// Option configures a session export.
type Option func(*exportConfig)

type exportConfig struct {
	transcriptOpts []skill.TranscriptOption
	includeSpans   bool
}

// WithTranscriptOptions forwards transcript-shaping options (e.g. a
// single-trace filter) to the underlying turn walk.
func WithTranscriptOptions(opts ...skill.TranscriptOption) Option {
	return func(c *exportConfig) {
		c.transcriptOpts = append(c.transcriptOpts, opts...)
	}
}

// WithSpanTrees nests each turn's derived span tree under its record's
// "spans" field (kind, parent_span_id, seq, call-kind taxonomy).
func WithSpanTrees() Option {
	return func(c *exportConfig) {
		c.includeSpans = true
	}
}

// SessionJSONL writes one JSON object per non-synthetic turn in the
// session to w. The response is the rendered spine transcript for the
// turn (assistant + tool lines, prompt excluded since it has its own
// field). This is the generalized body of the CLI's former exportJSONL,
// widened from *strings.Builder to io.Writer so it can be reused for
// streaming HTTP responses.
func SessionJSONL(ctx context.Context, query skill.Querier, sessionID string, w io.Writer, opts ...Option) error {
	var cfg exportConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	turns, err := skill.SessionTurns(ctx, query, sessionID, cfg.transcriptOpts...)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	for _, turn := range turns {
		rec := Turn{
			TraceID:           turn.TraceID,
			SessionID:         sessionID,
			UserPrompt:        turn.UserPrompt,
			Response:          turnResponse(ctx, query, turn),
			TotalInputTokens:  turn.TotalInputTokens,
			TotalOutputTokens: turn.TotalOutputTokens,
			MainInputTokens:   turn.MainInputTokens,
			MainOutputTokens:  turn.MainOutputTokens,
		}
		if !turn.StartedAt.IsZero() {
			rec.StartedAt = turn.StartedAt.Format("2006-01-02T15:04:05Z07:00")
		}
		if cfg.includeSpans {
			spans, err := turnSpans(ctx, query, turn.TraceID)
			if err != nil {
				return err
			}
			rec.Spans = spans
		}
		if err := enc.Encode(rec); err != nil {
			return fmt.Errorf("encoding turn %s: %w", turn.TraceID, err)
		}
	}
	return nil
}

// turnSpans fetches one turn's derived spans as nested export records.
func turnSpans(ctx context.Context, query skill.Querier, traceID string) ([]Span, error) {
	trace, err := query.Trace(ctx, traceID)
	if err != nil {
		return nil, fmt.Errorf("load spans for trace %s: %w", traceID, err)
	}
	if trace == nil {
		return nil, nil
	}
	spans := make([]Span, 0, len(trace.Spans))
	for _, s := range trace.Spans {
		spans = append(spans, Span{
			SpanID:       s.SpanID,
			ParentSpanID: s.ParentSpanID,
			Kind:         s.Kind,
			Name:         s.Name,
			Seq:          s.Seq,
			CallKind:     s.CallKind,
			ThreadID:     s.ThreadID,
		})
	}
	return spans, nil
}

// turnResponse renders just the assistant/tool half of a turn by
// stripping the leading [user] line from the shared turn transcript.
func turnResponse(ctx context.Context, query skill.Querier, turn skill.TraceSummary) string {
	transcript := skill.TurnTranscript(ctx, query, turn)
	var lines []string
	for line := range strings.SplitSeq(transcript, "\n") {
		if strings.HasPrefix(line, "[user] ") {
			continue
		}
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

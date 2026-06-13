package skill

import (
	"context"
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// transcriptConfig is the resolved option set for BuildSessionTranscript.
type transcriptConfig struct {
	timeFilter *GenerateOptions
	traceID    string // when non-empty, render only this turn
}

// TranscriptOption configures BuildSessionTranscript.
type TranscriptOption func(*transcriptConfig)

// WithTimeFilter applies the --since/--until turn window. A nil opts is a
// no-op, mirroring skill generation's filtering.
func WithTimeFilter(opts *GenerateOptions) TranscriptOption {
	return func(c *transcriptConfig) { c.timeFilter = opts }
}

// WithTraceFilter restricts the transcript to a single turn (the --trace
// case). Other turns in the session are dropped before rendering.
func WithTraceFilter(traceID string) TranscriptOption {
	return func(c *transcriptConfig) { c.traceID = traceID }
}

// BuildSessionTranscript renders the turn-grain transcript for one
// product session (a /v1/sessions UUID). It walks the trace surface:
// TraceSummaries for the session's user-visible turns, then Trace for
// each turn's spans. Per turn it emits the user prompt, then the
// main-thread conversation-spine ("main" call-kind, no sub-thread) llm
// responses in span order with tool usage summarized between them.
// Thinking blocks are dropped; when a turn carries no spine text the
// derive-time response preview stands in. Synthetic turns (compaction,
// resume replay) and turns outside the time window are filtered out.
//
// This is the single transcript code path shared by skill generation and
// the checkout export.
func BuildSessionTranscript(ctx context.Context, query Querier, sessionID string, opts ...TranscriptOption) (string, error) {
	turns, err := SessionTurns(ctx, query, sessionID, opts...)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	for _, turn := range turns {
		writeTurn(ctx, &b, query, turn)
	}
	return b.String(), nil
}

// SessionTurns returns the filtered, user-visible turns of a session
// after applying any --trace and time-window options. It is the shared
// turn-resolution step behind BuildSessionTranscript and the structured
// (jsonl) export. Returns an error when no turns remain.
func SessionTurns(ctx context.Context, query Querier, sessionID string, opts ...TranscriptOption) ([]TraceSummary, error) {
	cfg := &transcriptConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	turns, err := query.TraceSummaries(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("load session %s: %w", sessionID, err)
	}

	turns = filterTurns(turns, cfg.timeFilter)

	if cfg.traceID != "" {
		var matched []TraceSummary
		for _, t := range turns {
			if t.TraceID == cfg.traceID {
				matched = append(matched, t)
			}
		}
		turns = matched
		if len(turns) == 0 {
			return nil, fmt.Errorf("trace %s not found in session %s", cfg.traceID, sessionID)
		}
		return turns, nil
	}

	if len(turns) == 0 {
		return nil, fmt.Errorf("no turns in session %s after applying filters", sessionID)
	}
	return turns, nil
}

// TurnTranscript renders the [user]/[assistant]/[tools] lines for a
// single resolved turn, used when exporting one turn at a time.
func TurnTranscript(ctx context.Context, query Querier, turn TraceSummary) string {
	var b strings.Builder
	writeTurn(ctx, &b, query, turn)
	return b.String()
}

// writeTurn renders one turn's prompt and spine responses into b. When a
// turn's span detail is unavailable (or carries no spine text) the
// derive-time response preview stands in, so the transcript always has
// both halves of the exchange.
func writeTurn(ctx context.Context, b *strings.Builder, query Querier, turn TraceSummary) {
	if turn.UserPrompt != "" {
		fmt.Fprintf(b, "[user] %s\n", turn.UserPrompt)
	}

	trace, err := query.Trace(ctx, turn.TraceID)
	if err != nil || trace == nil {
		if turn.ResponsePreview != "" {
			fmt.Fprintf(b, "[assistant] %s\n", turn.ResponsePreview)
		}
		return
	}

	if !writeSpineResponses(b, trace.Spans) && turn.ResponsePreview != "" {
		fmt.Fprintf(b, "[assistant] %s\n", turn.ResponsePreview)
	}
}

// filterTurns drops synthetic turns (compaction seams, resume replays —
// no user intent to extract from) and applies the --since/--until
// window at turn grain.
func filterTurns(turns []TraceSummary, opts *GenerateOptions) []TraceSummary {
	var filtered []TraceSummary
	for _, turn := range turns {
		if turn.Synthetic != "" {
			continue
		}
		if opts != nil {
			if opts.Since != nil && turn.StartedAt.Before(*opts.Since) {
				continue
			}
			if opts.Until != nil && turn.StartedAt.After(*opts.Until) {
				continue
			}
		}
		filtered = append(filtered, turn)
	}
	return filtered
}

// writeSpineResponses walks one turn's spans in presentation order,
// emitting an [assistant] line per conversation-spine llm span with
// text and a [tools] summary line for the tool calls in between.
// Offshoot and injected call kinds, and subagent threads, are skipped.
// Reports whether any assistant text was written.
func writeSpineResponses(b *strings.Builder, spans []Span) bool {
	wrote := false
	pendingTools := map[string]int{}
	var pendingOrder []string

	flushTools := func() {
		if len(pendingOrder) == 0 {
			return
		}
		parts := make([]string, 0, len(pendingOrder))
		for _, name := range pendingOrder {
			if count := pendingTools[name]; count > 1 {
				parts = append(parts, fmt.Sprintf("%s ×%d", name, count))
			} else {
				parts = append(parts, name)
			}
		}
		fmt.Fprintf(b, "[tools] %s\n", strings.Join(parts, ", "))
		pendingTools = map[string]int{}
		pendingOrder = nil
	}

	for _, sp := range spans {
		switch sp.Kind {
		case "tool":
			if sp.ThreadID != "" {
				continue
			}
			if _, seen := pendingTools[sp.Name]; !seen {
				pendingOrder = append(pendingOrder, sp.Name)
			}
			pendingTools[sp.Name]++
		case "llm":
			if sp.CallKind != "main" || sp.ThreadID != "" {
				continue
			}
			text := blocksText(sp.Output)
			if text == "" {
				continue
			}
			flushTools()
			fmt.Fprintf(b, "[assistant] %s\n", text)
			wrote = true
		}
	}
	flushTools()
	return wrote
}

// blocksText joins the visible text blocks of an llm span's output.
// Thinking blocks are intentionally excluded: they are model-internal
// and bloat the prompt without adding workflow signal.
func blocksText(blocks []llm.ContentBlock) string {
	var texts []string
	for _, block := range blocks {
		if block.Text != "" {
			texts = append(texts, block.Text)
		}
	}
	return strings.Join(texts, "\n")
}

// Package export provides the shared JSONL conversation-export primitive:
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

// Turn is one turn's structured (jsonl) export record.
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
}

// SessionJSONL writes one JSON object per non-synthetic turn in the
// session to w. The response is the rendered spine transcript for the
// turn (assistant + tool lines, prompt excluded since it has its own
// field). This is the generalized body of the CLI's former exportJSONL,
// widened from *strings.Builder to io.Writer so it can be reused for
// streaming HTTP responses.
func SessionJSONL(ctx context.Context, query skill.Querier, sessionID string, w io.Writer, opts ...skill.TranscriptOption) error {
	turns, err := skill.SessionTurns(ctx, query, sessionID, opts...)
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
		if err := enc.Encode(rec); err != nil {
			return fmt.Errorf("encoding turn %s: %w", turn.TraceID, err)
		}
	}
	return nil
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

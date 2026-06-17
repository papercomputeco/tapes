// Package checkoutcmder provides the checkout subcommand: a conversation
// export primitive. It renders a captured session (or a single turn) from
// the trace surface as Markdown or JSONL, to stdout or a file.
package checkoutcmder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/inprocessapi"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/skill"
)

type checkoutCommander struct {
	flags config.FlagSet

	sessionID string
	traceID   string
	format    string
	output    string
	apiTarget string
	postgres  string
}

const (
	formatMarkdown = "md"
	formatJSONL    = "jsonl"
	formatSpans    = "spans"
)

var checkoutFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", Shorthand: "a", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const checkoutLongDesc string = `Export a captured conversation from a tapes session.

Reads a session's derived turn/span projection from the trace surface and
renders it as a transcript. The whole session is exported by default; pass
--trace to export a single turn.

Formats:
  md     human-readable Markdown transcript ([user]/[assistant]/[tools] lines)
  jsonl  one JSON object per turn (prompt, response, token counts)
  spans  one JSON object per span (the derived span tree: kind, nesting,
         call-kind taxonomy) — the span-level view behind the transcript

The session must already be captured; checkout owns no state and only reads
the existing /v1/traces surface. Connect to a running API with --api-target,
or run an in-process API over a database with --postgres.

Examples:
  tapes checkout 0196fdb1-93f4-7c41-a53d-0fbe2c5e1f23 --api-target http://127.0.0.1:8081
  tapes checkout <session-id> --trace <trace-id> -o turn.md
  tapes checkout <session-id> --format jsonl -o session.jsonl`

const checkoutShortDesc string = "Export a captured conversation"

// NewCheckoutCmd builds the checkout export command.
func NewCheckoutCmd() *cobra.Command {
	cmder := &checkoutCommander{
		flags: checkoutFlags,
	}

	cmd := &cobra.Command{
		Use:   "checkout <session-id>",
		Short: checkoutShortDesc,
		Long:  checkoutLongDesc,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to default
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagAPITarget,
			})

			cmder.apiTarget = v.GetString("client.api_target")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cmder.sessionID = args[0]
			return cmder.run(cmd)
		},
	}

	cmd.Flags().StringVar(&cmder.traceID, "trace", "", "Export only this trace (turn) instead of the whole session")
	cmd.Flags().StringVar(&cmder.format, "format", formatMarkdown, "Output format: md|jsonl|spans (spans = one JSON object per span)")
	cmd.Flags().StringVarP(&cmder.output, "output", "o", "", "Write to this path instead of stdout")
	cmd.Flags().StringVar(&cmder.postgres, "postgres", "", "PostgreSQL connection string for a local in-process API")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *checkoutCommander) run(cmd *cobra.Command) error {
	if c.format != formatMarkdown && c.format != formatJSONL && c.format != formatSpans {
		return fmt.Errorf("invalid --format %q; valid formats: %s, %s, %s", c.format, formatMarkdown, formatJSONL, formatSpans)
	}

	client, closeFn, err := c.connect(cmd.Context())
	if err != nil {
		return err
	}
	defer closeFn()

	sessionID, err := resolveSessionID(cmd.Context(), client, c.sessionID)
	if err != nil {
		return err
	}

	rendered, err := Export(cmd.Context(), client, ExportOptions{
		SessionID: sessionID,
		TraceID:   c.traceID,
		Format:    c.format,
	})
	if err != nil {
		return err
	}

	return c.write(rendered)
}

// ExportOptions configures a conversation export.
type ExportOptions struct {
	SessionID string
	TraceID   string // when non-empty, export only this turn
	Format    string // formatMarkdown ("md") or formatJSONL ("jsonl")
}

// Export renders a captured session (or single turn) from the trace
// surface in the requested format. It is the testable seam behind the
// checkout command: callers supply any skill.Querier.
func Export(ctx context.Context, query skill.Querier, opts ExportOptions) (string, error) {
	var transcriptOpts []skill.TranscriptOption
	if opts.TraceID != "" {
		transcriptOpts = append(transcriptOpts, skill.WithTraceFilter(opts.TraceID))
	}

	switch opts.Format {
	case formatJSONL:
		return exportJSONL(ctx, query, opts.SessionID, transcriptOpts)
	case formatSpans:
		return exportSpans(ctx, query, opts.SessionID, transcriptOpts)
	case formatMarkdown, "":
		return skill.BuildSessionTranscript(ctx, query, opts.SessionID, transcriptOpts...)
	default:
		return "", fmt.Errorf("invalid format %q; valid formats: %s, %s, %s", opts.Format, formatMarkdown, formatJSONL, formatSpans)
	}
}

// exportSpan is one span's structured (jsonl) export record — the span
// tree the deriver built for the session, flattened in trace + seq order.
type exportSpan struct {
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
	Kind         string `json:"kind"` // llm | tool | agent | event
	Name         string `json:"name,omitempty"`
	Seq          int64  `json:"seq"`
	CallKind     string `json:"call_kind,omitempty"` // main | offshoot:… | injected:…
	ThreadID     string `json:"thread_id,omitempty"`
}

// exportSpans emits one JSON object per span across the session's turns, so
// the span structure (kinds, nesting via parent_span_id, call-kind taxonomy)
// is visible from the CLI rather than only in Deck.
func exportSpans(ctx context.Context, query skill.Querier, sessionID string, opts []skill.TranscriptOption) (string, error) {
	turns, err := skill.SessionTurns(ctx, query, sessionID, opts...)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, turn := range turns {
		trace, err := query.Trace(ctx, turn.TraceID)
		if err != nil {
			return "", fmt.Errorf("load spans for trace %s: %w", turn.TraceID, err)
		}
		for _, s := range trace.Spans {
			rec := exportSpan{
				TraceID:      turn.TraceID,
				SpanID:       s.SpanID,
				ParentSpanID: s.ParentSpanID,
				Kind:         s.Kind,
				Name:         s.Name,
				Seq:          s.Seq,
				CallKind:     s.CallKind,
				ThreadID:     s.ThreadID,
			}
			if err := enc.Encode(rec); err != nil {
				return "", fmt.Errorf("encoding span %s: %w", s.SpanID, err)
			}
		}
	}
	return b.String(), nil
}

// connect resolves an APIClient against either the remote API
// (--api-target) or an in-process API over --postgres.
func (c *checkoutCommander) connect(ctx context.Context) (*skill.APIClient, func(), error) {
	if strings.TrimSpace(c.apiTarget) != "" {
		return skill.NewAPIClient(c.apiTarget), func() {}, nil
	}
	if strings.TrimSpace(c.postgres) == "" {
		return nil, nil, errors.New("no API target configured: pass --api-target or --postgres")
	}
	target, stop, startErr := inprocessapi.Start(ctx, c.postgres, nil)
	if startErr != nil {
		return nil, nil, startErr
	}
	return skill.NewAPIClient(target), stop, nil
}

// resolveSessionID returns the argument unchanged when it is already a full
// UUID; otherwise it treats it as an id prefix and resolves it against the
// session list, requiring a unique match. This lets users paste the short
// id Deck shows instead of hunting for the full UUID.
func resolveSessionID(ctx context.Context, client *skill.APIClient, raw string) (string, error) {
	id := strings.TrimSpace(raw)
	if _, err := uuid.Parse(id); err == nil {
		return id, nil
	}

	sessions, err := client.Sessions(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving session id %q: %w", id, err)
	}

	var matches []string
	for _, s := range sessions {
		if strings.HasPrefix(s.ID, id) {
			matches = append(matches, s.ID)
		}
	}

	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", fmt.Errorf("no session matches id %q — run `tapes sessions` to list ids", id)
	default:
		return "", fmt.Errorf("session id prefix %q is ambiguous (%d matches) — use more characters", id, len(matches))
	}
}

// exportTurn is one turn's structured (jsonl) export record.
type exportTurn struct {
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

// exportJSONL emits one JSON object per turn. The response is the
// rendered spine transcript for the turn (assistant + tool lines, prompt
// excluded since it has its own field).
func exportJSONL(ctx context.Context, query skill.Querier, sessionID string, opts []skill.TranscriptOption) (string, error) {
	turns, err := skill.SessionTurns(ctx, query, sessionID, opts...)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, turn := range turns {
		rec := exportTurn{
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
			return "", fmt.Errorf("encoding turn %s: %w", turn.TraceID, err)
		}
	}
	return b.String(), nil
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

// write sends the rendered export to stdout or the -o path.
func (c *checkoutCommander) write(content string) error {
	if strings.TrimSpace(c.output) == "" {
		_, err := io.WriteString(os.Stdout, content)
		return err
	}
	if err := os.WriteFile(c.output, []byte(content), 0o644); err != nil { //nolint:gosec // export artifact, not a secret
		return fmt.Errorf("writing %s: %w", c.output, err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", c.output)
	return nil
}

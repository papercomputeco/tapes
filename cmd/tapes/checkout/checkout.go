// Package checkoutcmder provides the checkout subcommand: a conversation
// export primitive. It renders a captured session (or a single turn) from
// the trace surface as Markdown or JSONL, to stdout or a file.
package checkoutcmder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/inprocessapi"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/export"
	"github.com/papercomputeco/tapes/pkg/skill"
)

type checkoutCommander struct {
	flags config.FlagSet

	sessionID    string
	traceID      string
	format       string
	includeSpans bool
	output       string
	apiTarget    string
	postgres     string
}

const (
	formatMarkdown = "md"
	formatJSONL    = "jsonl"
)

var checkoutFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", Shorthand: "a", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const checkoutLongDesc string = `Export a captured conversation from a tapes session.

Reads a session's derived projection from the trace surface and renders it the
way the API and console model it — a session is traces (turns) composed of
spans. The whole session is exported by default; pass --trace for a single turn.

Formats:
  md     human-readable Markdown transcript
  jsonl  one JSON object per turn (trace): prompt, response, token counts

Span detail is included by default for both formats: md adds [tools] lines and
jsonl nests each turn's span tree under "spans" (kind, parent_span_id, seq,
call-kind taxonomy). Pass --spans=false for just the [user]/[assistant]
conversation.

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
	cmd.Flags().StringVar(&cmder.format, "format", formatMarkdown, "Output format: md|jsonl")
	cmd.Flags().BoolVar(&cmder.includeSpans, "spans", true, "Include the derived span tree (on by default; --spans=false for just the conversation)")
	cmd.Flags().StringVarP(&cmder.output, "output", "o", "", "Write to this path instead of stdout")
	cmd.Flags().StringVar(&cmder.postgres, "postgres", "", "PostgreSQL connection string for a local in-process API")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *checkoutCommander) run(cmd *cobra.Command) error {
	if c.format != formatMarkdown && c.format != formatJSONL {
		return fmt.Errorf("invalid --format %q; valid formats: %s, %s", c.format, formatMarkdown, formatJSONL)
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
		SessionID:    sessionID,
		TraceID:      c.traceID,
		Format:       c.format,
		IncludeSpans: c.includeSpans,
	})
	if err != nil {
		return err
	}

	return c.write(rendered)
}

// ExportOptions configures a conversation export.
type ExportOptions struct {
	SessionID    string
	TraceID      string // when non-empty, export only this turn
	Format       string // formatMarkdown ("md") or formatJSONL ("jsonl")
	IncludeSpans bool   // include the derived span tree (on by default)
}

// Export renders a captured session (or single turn) from the trace
// surface in the requested format. It is the testable seam behind the
// checkout command: callers supply any skill.Querier. The export mirrors
// the API/console model — a session is traces (turns) composed of spans —
// and IncludeSpans toggles the span detail for either format.
func Export(ctx context.Context, query skill.Querier, opts ExportOptions) (string, error) {
	var transcriptOpts []skill.TranscriptOption
	if opts.TraceID != "" {
		transcriptOpts = append(transcriptOpts, skill.WithTraceFilter(opts.TraceID))
	}

	switch opts.Format {
	case formatJSONL:
		exportOpts := []export.Option{export.WithTranscriptOptions(transcriptOpts...)}
		if opts.IncludeSpans {
			exportOpts = append(exportOpts, export.WithSpanTrees())
		}
		var b strings.Builder
		if err := export.SessionJSONL(ctx, query, opts.SessionID, &b, exportOpts...); err != nil {
			return "", err
		}
		return b.String(), nil
	case formatMarkdown, "":
		if !opts.IncludeSpans {
			transcriptOpts = append(transcriptOpts, skill.WithoutSpanDetail())
		}
		return skill.BuildSessionTranscript(ctx, query, opts.SessionID, transcriptOpts...)
	default:
		return "", fmt.Errorf("invalid format %q; valid formats: %s, %s", opts.Format, formatMarkdown, formatJSONL)
	}
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

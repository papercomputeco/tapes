// Package exportcmd provides the export subcommand: it downloads a captured
// session as JSONL from the running tapes API and writes it to stdout or a
// file. The output is the API's session→traces→spans projection verbatim —
// export is a thin client of GET /v1/sessions/{id}/export, not a second
// renderer, so the CLI and the API can never drift.
package exportcmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/skill"
)

type exportCommander struct {
	flags config.FlagSet

	sessionID string
	detail    string
	output    string
	apiTarget string
}

var exportFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", Shorthand: "a", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const exportShortDesc string = "Export a captured session as JSONL"

const exportLongDesc string = `Export a captured session as JSONL.

Downloads the session's derived projection from the running tapes API and
writes it verbatim — the same session→traces→spans shape the API and console
serve (GET /v1/sessions/{id}/export). export owns no state and no second
renderer; it streams exactly what the API returns.

Detail:
  spans   (default) traces with their full span trees
  traces  turn headers only (no spans or links)

export calls the API, so a running server is required — pass --api-target (or
set client.api_target). Start one locally with ` + "`tapes serve`" + `.

Examples:
  tapes export 0196fdb1-93f4-7c41-a53d-0fbe2c5e1f23 --api-target http://127.0.0.1:8081
  tapes export 0196fdb1 -o session.jsonl
  tapes export <session-id> --detail traces`

// NewExportCmd builds the export subcommand.
func NewExportCmd() *cobra.Command {
	cmder := &exportCommander{flags: exportFlags}

	cmd := &cobra.Command{
		Use:   "export <session-id>",
		Short: exportShortDesc,
		Long:  exportLongDesc,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return nil //nolint:nilerr // non-fatal, fall back to default
			}
			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{config.FlagAPITarget})
			cmder.apiTarget = v.GetString("client.api_target")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cmder.sessionID = args[0]
			return cmder.run(cmd)
		},
	}

	cmd.Flags().StringVar(&cmder.detail, "detail", "spans", "Export granularity: spans (traces with full spans) or traces (turn headers only)")
	cmd.Flags().StringVarP(&cmder.output, "output", "o", "", "Write to this path instead of stdout")
	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *exportCommander) run(cmd *cobra.Command) error {
	if c.detail != "spans" && c.detail != "traces" {
		return fmt.Errorf("invalid --detail %q; valid values: spans, traces", c.detail)
	}
	if strings.TrimSpace(c.apiTarget) == "" {
		return errors.New("no API target configured: pass --api-target (export calls the running API; start one with `tapes serve`)")
	}

	client := skill.NewAPIClient(c.apiTarget)
	sessionID, err := resolveSessionID(cmd.Context(), client, c.sessionID)
	if err != nil {
		return err
	}

	body, err := client.ExportSession(cmd.Context(), sessionID, c.detail)
	if err != nil {
		return err
	}
	return c.write(body)
}

func (c *exportCommander) write(body []byte) error {
	if strings.TrimSpace(c.output) == "" {
		_, err := os.Stdout.Write(body)
		return err
	}
	if err := os.WriteFile(c.output, body, 0o644); err != nil { //nolint:gosec // export artifact, not a secret
		return fmt.Errorf("writing %s: %w", c.output, err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", c.output)
	return nil
}

// resolveSessionID accepts a full session UUID or a unique id prefix,
// resolving a prefix against the session list. The export endpoint needs a
// full UUID; a prefix is a convenience the CLI resolves API-side.
func resolveSessionID(ctx context.Context, client *skill.APIClient, raw string) (string, error) {
	id := strings.TrimSpace(raw)
	if _, err := uuid.Parse(id); err == nil {
		return id, nil
	}

	sessions, err := client.AllSessions(ctx)
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

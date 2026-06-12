// Package synccmder provides the `tapes sync` CLI command.
package synccmder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/inprocessapi"
	"github.com/papercomputeco/tapes/pkg/backfill"
	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/telemetry"
)

type syncCommander struct {
	postgresDSN string
	apiTarget   string
	claudeDir   string
	dryRun      bool
	verbose     bool
	sessions    bool
	orgID       string
	authSubject string
}

// NewSyncCmd creates the sync cobra command.
func NewSyncCmd() *cobra.Command {
	cmder := &syncCommander{}

	cmd := &cobra.Command{
		Use:    "sync",
		Short:  "Sync token usage from Claude Code transcripts",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cmder.run(cmd.Context()); err != nil {
				return err
			}
			telemetry.FromContext(cmd.Context()).CaptureSyncPull()
			return nil
		},
	}

	cmd.Flags().StringVarP(&cmder.apiTarget, "api-target", "a", "", "URL of a running tapes API server")
	cmd.Flags().StringVar(&cmder.postgresDSN, "postgres", "", "PostgreSQL connection string for a local in-process API")
	cmd.Flags().StringVar(&cmder.claudeDir, "claude-dir", "", "Override Claude Code projects directory")
	cmd.Flags().BoolVar(&cmder.dryRun, "dry-run", false, "Preview matches without writing")
	cmd.Flags().BoolVarP(&cmder.verbose, "verbose", "v", false, "Show per-node match details")
	cmd.Flags().BoolVar(&cmder.sessions, "sessions", false, "Also backfill Claude session rows and link legacy nodes")
	cmd.Flags().StringVar(&cmder.orgID, "org-id", "", "Organization UUID to use for backfilled sessions")
	cmd.Flags().StringVar(&cmder.authSubject, "auth-subject", localUserSubject(), "Auth subject to attribute backfilled sessions to (defaults to the local username)")

	return cmd
}

func (c *syncCommander) run(ctx context.Context) error {
	apiTarget, closeFn, location, err := c.resolveAPITarget(ctx)
	if err != nil {
		return err
	}
	defer closeFn()

	claudeDir := c.resolveClaudeDir()

	if c.dryRun {
		fmt.Printf("  %s Dry run mode — no changes will be written\n\n", cliui.DimStyle.Render("●"))
	}

	if c.verbose {
		fmt.Printf("  %s %s\n", cliui.KeyStyle.Render("API:"), cliui.DimStyle.Render(location))
		fmt.Printf("  %s %s\n\n", cliui.KeyStyle.Render("Transcripts:"), cliui.DimStyle.Render(claudeDir))
	}

	var result *backfill.Result
	if err := cliui.Step(os.Stdout, "Syncing token usage", func() error {
		var runErr error
		result, runErr = backfill.RunViaAPI(ctx, apiTarget, claudeDir, backfill.Options{
			DryRun:      c.dryRun,
			Verbose:     c.verbose,
			Sessions:    c.sessions,
			OrgID:       c.orgID,
			AuthSubject: c.authSubject,
		})
		return runErr
	}); err != nil {
		return err
	}

	fmt.Printf("\n  %s %s\n\n", cliui.SuccessMark, result.Summary())
	return nil
}

func (c *syncCommander) resolveAPITarget(ctx context.Context) (string, func(), string, error) {
	if strings.TrimSpace(c.apiTarget) != "" {
		return c.apiTarget, func() {}, c.apiTarget, nil
	}
	if strings.TrimSpace(c.postgresDSN) == "" {
		return "", nil, "", errors.New("no API target configured: pass --api-target or --postgres")
	}

	target, stop, err := inprocessapi.Start(ctx, c.postgresDSN, sessions.DefaultPricing())
	if err != nil {
		return "", nil, "", err
	}

	return target, stop, "local postgres", nil
}

func (c *syncCommander) resolveClaudeDir() string {
	if strings.TrimSpace(c.claudeDir) != "" {
		return c.claudeDir
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".claude", "projects")
	}

	return filepath.Join(home, ".claude", "projects")
}

// localUserSubject is the default attribution subject for locally-run
// backfills: the OS username of whoever is running tapes. Local
// captures have no gateway-validated JWT to derive a subject from, so
// the closest honest identity is the local user. This default lives at
// the CLI flag — never inside pkg/backfill — because the same package
// also runs server-side behind the admin endpoint, where the process
// owner's username would mis-attribute every row.
func localUserSubject() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return os.Getenv("USER")
}

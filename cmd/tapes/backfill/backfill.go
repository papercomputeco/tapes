// Package backfillcmder provides the `tapes backfill` CLI command.
package backfillcmder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/cmd/tapes/sqlitepath"
	"github.com/papercomputeco/tapes/pkg/backfill"
)

const backfillLongDesc string = `Backfill token usage from Claude Code transcripts.

Scans Claude Code's local JSONL transcripts for usage data and updates
existing nodes in the tapes database that are missing token counts.

Examples:
  tapes backfill
  tapes backfill --dry-run
  tapes backfill --sqlite ./tapes.db --verbose
  tapes backfill --claude-dir ~/.claude/projects`

const backfillShortDesc string = "Backfill token usage from Claude Code transcripts"

type backfillCommander struct {
	sqlitePath string
	claudeDir  string
	dryRun     bool
	verbose    bool
}

// NewBackfillCmd creates the backfill cobra command.
func NewBackfillCmd() *cobra.Command {
	cmder := &backfillCommander{}

	cmd := &cobra.Command{
		Use:   "backfill",
		Short: backfillShortDesc,
		Long:  backfillLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd.Context(), cmd)
		},
	}

	cmd.Flags().StringVarP(&cmder.sqlitePath, "sqlite", "s", "", "Path to SQLite database")
	cmd.Flags().StringVar(&cmder.claudeDir, "claude-dir", "", "Override Claude Code projects directory")
	cmd.Flags().BoolVar(&cmder.dryRun, "dry-run", false, "Preview matches without writing")
	cmd.Flags().BoolVarP(&cmder.verbose, "verbose", "v", false, "Show per-node match details")

	return cmd
}

func (c *backfillCommander) run(ctx context.Context, cmd *cobra.Command) error {
	dbPath := c.resolveSQLitePath()
	claudeDir := c.resolveClaudeDir()

	if c.dryRun {
		fmt.Fprintln(cmd.OutOrStdout(), "Dry run mode — no changes will be written")
	}

	if c.verbose {
		fmt.Fprintf(cmd.OutOrStdout(), "Database: %s\n", dbPath)
		fmt.Fprintf(cmd.OutOrStdout(), "Transcripts: %s\n", claudeDir)
	}

	opts := backfill.Options{
		DryRun:  c.dryRun,
		Verbose: c.verbose,
	}

	b, cleanup, err := backfill.NewBackfiller(ctx, dbPath, opts)
	if err != nil {
		return err
	}
	defer func() { _ = cleanup() }()

	result, err := b.Run(ctx, claudeDir)
	if err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), result.Summary())
	return nil
}

func (c *backfillCommander) resolveSQLitePath() string {
	if strings.TrimSpace(c.sqlitePath) != "" {
		return c.sqlitePath
	}

	path, err := sqlitepath.ResolveSQLitePath("")
	if err == nil {
		return path
	}

	return "tapes.db"
}

func (c *backfillCommander) resolveClaudeDir() string {
	if strings.TrimSpace(c.claudeDir) != "" {
		return c.claudeDir
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".claude", "projects")
	}

	return filepath.Join(home, ".claude", "projects")
}

package skillcmder

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/skill"
)

type syncCommander struct {
	global bool
	local  bool
	dryRun bool
}

func newSyncCmd() *cobra.Command {
	cmder := &syncCommander{}

	cmd := &cobra.Command{
		Use:   "sync <name>",
		Short: "Copy a skill to Claude Code's skills directory",
		Long: `Sync a skill from ~/.tapes/skills/ to a Claude Code skills directory.

By default, syncs to ~/.claude/skills/ (global). Use --local to sync to
.claude/skills/ in the current project.

Examples:
  tapes skill sync debug-react-hooks
  tapes skill sync debug-react-hooks --local
  tapes skill sync debug-react-hooks --dry-run`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmder.run(cmd, args[0])
		},
	}

	cmd.Flags().BoolVar(&cmder.global, "global", true, "Sync to ~/.claude/skills/ (default)")
	cmd.Flags().BoolVar(&cmder.local, "local", false, "Sync to .claude/skills/ in current project")
	cmd.Flags().BoolVar(&cmder.dryRun, "dry-run", false, "Show what would be synced")

	return cmd
}

func (c *syncCommander) run(cmd *cobra.Command, name string) error {
	sourceDir, err := skill.SkillsDir()
	if err != nil {
		return err
	}

	var targetDir string
	var label string

	if c.local {
		targetDir = skill.LocalClaudeSkillsDir()
		label = "project"
	} else {
		targetDir, err = skill.GlobalClaudeSkillsDir()
		if err != nil {
			return err
		}
		label = "global"
	}

	if c.dryRun {
		fmt.Fprintf(cmd.OutOrStdout(), "Would sync skill %q to %s (%s)\n", name, targetDir, label)
		return nil
	}

	path, err := skill.Sync(name, sourceDir, targetDir)
	if err != nil {
		return fmt.Errorf("sync skill: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Synced skill %q to %s (%s)\n", name, path, label)
	return nil
}

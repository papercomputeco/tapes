package skillcmder

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/skill"
)

type syncCommander struct {
	claude bool
	local  bool
	dryRun bool
}

func newSyncCmd() *cobra.Command {
	cmder := &syncCommander{}

	cmd := &cobra.Command{
		Use:   "sync <name>",
		Short: "Copy a skill to .agents/skills/ or Claude Code's skills directory",
		Long: `Sync a skill from ~/.tapes/skills/ to an agent skills directory.

By default, syncs to ~/.agents/skills/ (global). Use --local to sync to
.agents/skills/ in the current project. Use --claude to target Claude Code's
.claude/skills/ directory instead.

Examples:
  tapes skill sync debug-react-hooks
  tapes skill sync debug-react-hooks --local
  tapes skill sync debug-react-hooks --claude
  tapes skill sync debug-react-hooks --claude --local
  tapes skill sync debug-react-hooks --dry-run`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmder.run(cmd, args[0])
		},
	}

	cmd.Flags().BoolVar(&cmder.claude, "claude", false, "Sync to .claude/skills/ instead of .agents/skills/")
	cmd.Flags().BoolVar(&cmder.local, "local", false, "Sync to project-local directory instead of global")
	cmd.Flags().BoolVar(&cmder.dryRun, "dry-run", false, "Show what would be synced")

	return cmd
}

func (c *syncCommander) run(cmd *cobra.Command, name string) error {
	w := cmd.OutOrStdout()

	var sourceDir string
	var targetDir string
	var label string

	fmt.Fprintf(w, "\nSyncing skill %q\n\n", name)

	// Step 1: Resolve source
	if err := step(w, "Resolving source skill", func() error {
		var err error
		sourceDir, err = skill.SkillsDir()
		return err
	}); err != nil {
		return err
	}

	// Step 2: Resolve target
	if err := step(w, "Resolving target directory", func() error {
		var err error
		targetDir, label, err = c.resolveTarget()
		return err
	}); err != nil {
		return err
	}

	if c.dryRun {
		fmt.Fprintf(w, "\n  Would sync to %s (%s)\n\n", targetDir, label)
		return nil
	}

	// Step 3: Copy skill
	var path string
	if err := step(w, "Copying skill to Claude Code", func() error {
		var err error
		path, err = skill.Sync(name, sourceDir, targetDir)
		return err
	}); err != nil {
		return fmt.Errorf("sync skill: %w", err)
	}

	fmt.Fprintf(w, "\n  Synced to %s (%s)\n\n", path, label)
	return nil
}

func (c *syncCommander) resolveTarget() (string, string, error) {
	if c.claude {
		if c.local {
			return skill.LocalClaudeSkillsDir(), "project, claude", nil
		}
		dir, err := skill.GlobalClaudeSkillsDir()
		return dir, "global, claude", err
	}

	if c.local {
		return skill.LocalAgentsSkillsDir(), "project", nil
	}
	dir, err := skill.GlobalAgentsSkillsDir()
	return dir, "global", err
}

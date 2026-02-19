// Package skillcmder provides the `tapes skill` CLI commands for generating,
// listing, and syncing agent skills from session data.
package skillcmder

import "github.com/spf13/cobra"

// NewSkillCmd creates the parent skill command.
func NewSkillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skill",
		Short: "Generate, list, and sync agent skills from sessions",
		Long: `Extract reusable patterns from tapes sessions and output them as
agent skill files. By default, skills sync to .agents/skills/ for use with any
coding agent. Use --claude to sync to Claude Code's .claude/skills/ directory.

Examples:
  tapes skill generate abc123 --name debug-react-hooks
  tapes skill generate --name my-skill   (uses current checkout)
  tapes skill list
  tapes skill sync debug-react-hooks
  tapes skill sync debug-react-hooks --claude`,
	}

	cmd.AddCommand(newGenerateCmd())
	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSyncCmd())

	return cmd
}

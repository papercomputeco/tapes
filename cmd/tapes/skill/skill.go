// Package skillcmder provides the `tapes skill` CLI commands for generating,
// listing, and syncing Claude Code skills from session data.
package skillcmder

import "github.com/spf13/cobra"

// NewSkillCmd creates the parent skill command.
func NewSkillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skill",
		Short: "Generate, list, and sync Claude Code skills from sessions",
		Long: `Extract reusable patterns from tapes sessions and output them as
Claude Code SKILL.md files.

Examples:
  tapes skill generate abc123 --name debug-react-hooks
  tapes skill generate --name my-skill   (uses current checkout)
  tapes skill list
  tapes skill sync debug-react-hooks --global`,
	}

	cmd.AddCommand(newGenerateCmd())
	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSyncCmd())

	return cmd
}

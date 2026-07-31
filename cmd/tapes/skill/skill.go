// Package skillcmder provides the `tapes skill` CLI commands for generating
// and listing agent skills from session data.
package skillcmder

import "github.com/spf13/cobra"

// NewSkillCmd creates the parent skill command.
func NewSkillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skill",
		Short: "Generate and list agent skills from sessions",
		Long: `Extract reusable patterns from tapes sessions and output them as
agent skill files under ~/.tapes/skills/.

Authoring a skill reads session data, so it lives here. Installing one into an
agent's skills directory is a client concern: use 'tapesctl skill sync'.

Examples:
  tapes skill generate abc123 --name debug-react-hooks
  tapes skill generate --name my-skill   (from the current session)
  tapes skill list`,
	}

	cmd.AddCommand(newGenerateCmd())
	cmd.AddCommand(newListCmd())

	return cmd
}

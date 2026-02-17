package skillcmder

import (
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/skill"
)

type listCommander struct {
	skillType string
}

func newListCmd() *cobra.Command {
	cmder := &listCommander{}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List skills in ~/.tapes/skills/",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmder.run(cmd)
		},
	}

	cmd.Flags().StringVar(&cmder.skillType, "type", "", "Filter by skill type")

	return cmd
}

func (c *listCommander) run(cmd *cobra.Command) error {
	skillsDir, err := skill.SkillsDir()
	if err != nil {
		return err
	}

	skills, err := skill.List(skillsDir)
	if err != nil {
		return fmt.Errorf("list skills: %w", err)
	}

	if len(skills) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No skills found. Generate one with: tapes skill generate --from <session-id> --name <name>")
		return nil
	}

	// Filter by type if specified
	if c.skillType != "" {
		var filtered []*skill.Skill
		for _, sk := range skills {
			if sk.Type == c.skillType {
				filtered = append(filtered, sk)
			}
		}
		skills = filtered
	}

	if len(skills) == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "No skills found with type %q\n", c.skillType)
		return nil
	}

	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tTYPE\tVERSION\tDESCRIPTION")
	for _, sk := range skills {
		desc := sk.Description
		if len(desc) > 60 {
			desc = desc[:57] + "..."
		}
		desc = strings.ReplaceAll(desc, "\n", " ")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", sk.Name, sk.Type, sk.Version, desc)
	}
	return w.Flush()
}
